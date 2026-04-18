import os
import sys
import json
import sqlite3
import argparse

# Enable absolute imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import database, settings

def get_json_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data
            except json.JSONDecodeError:
                pass
    return None


def run_json_seed_migration():
    print("Initializing Database...")
    database.init_db()

    # 1. Accounts
    print(f"Migrating {settings.ACCOUNTS_FILE}...")
    accounts = get_json_data(settings.ACCOUNTS_FILE)
    if accounts and isinstance(accounts, list):
        database.save_accounts(accounts, include_all=True)

    # 2. Models
    print(f"Migrating {settings.MODELS_FILE}...")
    models = get_json_data(settings.MODELS_FILE)
    if models and isinstance(models, list):
        database.save_models(models)

    # 3. Messages
    print(f"Migrating {settings.MESSAGES_FILE}...")
    msgs = get_json_data(settings.MESSAGES_FILE)
    if msgs and isinstance(msgs, list):
        database.save_messages(msgs)

    # 4. Settings
    print(f"Migrating {settings.SETTINGS_FILE}...")
    sets = get_json_data(settings.SETTINGS_FILE)
    if sets and isinstance(sets, dict):
        database.save_settings(sets)

    # 5. DM Logs
    print(f"Migrating {settings.DM_LOG_FILE}...")
    dm_log = get_json_data(settings.DM_LOG_FILE)
    if dm_log and isinstance(dm_log, dict):
        conn = database._get_connection()
        try:
            for username, log_data in dm_log.items():
                timestamp = log_data.get("timestamp", "") if isinstance(log_data, dict) else str(log_data)
                conn.execute(
                    "INSERT INTO dm_log (username, timestamp) VALUES (?, ?) ON CONFLICT(username) DO UPDATE SET timestamp=excluded.timestamp",
                    (username, timestamp)
                )
            conn.commit()
        finally:
            conn.close()

    # 6. Cookies
    print(f"Migrating Cookies in {settings.COOKIES_DIR}...")
    if os.path.exists(settings.COOKIES_DIR):
        for fname in os.listdir(settings.COOKIES_DIR):
            if fname.endswith(".json"):
                username = fname[:-5]
                cookie_data = get_json_data(os.path.join(settings.COOKIES_DIR, fname))
                if cookie_data and isinstance(cookie_data, list):
                    database.save_cookies(username, cookie_data)

    print("Migration Complete.")
    print("Files can now be safely removed.")


def _sqlite_table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def _row_get(row, key, default=None):
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        return default


def run_sqlite_to_postgres_migration(sqlite_path: str = None):
    if not database.using_postgres():
        raise RuntimeError(
            "SQLite -> PostgreSQL migration requires DATABASE_URL to point to PostgreSQL."
        )

    source_path = str(sqlite_path or database.DATABASE_PATH).strip()
    if not source_path:
        raise ValueError("Source SQLite path is empty")
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source SQLite file not found: {source_path}")

    print("Initializing target PostgreSQL schema...")
    database.init_db()

    source_conn = sqlite3.connect(source_path)
    source_conn.row_factory = sqlite3.Row
    target_conn = database._get_connection()

    copied_counts = {
        "settings": 0,
        "users": 0,
        "accounts": 0,
        "models": 0,
        "messages": 0,
        "cookies": 0,
        "dm_log": 0,
        "dm_event_log": 0,
        "activity_log": 0,
    }

    try:
        # Truncate target tables so migration is deterministic.
        for table_name in (
            "activity_log",
            "dm_event_log",
            "dm_log",
            "cookies",
            "accounts",
            "models",
            "messages",
            "users",
            "settings",
        ):
            target_conn.execute(f"DELETE FROM {table_name}")

        if _sqlite_table_exists(source_conn, "settings"):
            rows = source_conn.execute("SELECT key, value_json FROM settings").fetchall()
            for row in rows:
                key = str(row["key"] or "").strip()
                value_json = str(row["value_json"] or "null")
                if not key:
                    continue
                target_conn.execute(
                    "INSERT INTO settings (key, value_json) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                    (key, value_json),
                )
                copied_counts["settings"] += 1

        if _sqlite_table_exists(source_conn, "users"):
            rows = source_conn.execute(
                "SELECT username, password_hash, role, is_active, created_at FROM users"
            ).fetchall()
            for row in rows:
                username = str(_row_get(row, "username", "") or "").strip().lower()
                if not username:
                    continue
                target_conn.execute(
                    """
                    INSERT INTO users (username, password_hash, role, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                        password_hash = excluded.password_hash,
                        role = excluded.role,
                        is_active = excluded.is_active,
                        created_at = excluded.created_at
                    """,
                    (
                        username,
                        str(_row_get(row, "password_hash", "") or ""),
                        str(_row_get(row, "role", "employee") or "employee"),
                        _safe_int(_row_get(row, "is_active", 1), default=1),
                        str(_row_get(row, "created_at", "") or ""),
                    ),
                )
                copied_counts["users"] += 1

        if _sqlite_table_exists(source_conn, "accounts"):
            rows = source_conn.execute("SELECT * FROM accounts").fetchall()
            for row in rows:
                username = str(_row_get(row, "username", "") or "").strip()
                if not username:
                    continue

                target_conn.execute(
                    """
                    INSERT INTO accounts (
                        username,
                        password,
                        owner_username,
                        model_label,
                        custom_messages_json,
                        proxy,
                        profile_note,
                        automation_enabled,
                        is_suspended
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                        password = excluded.password,
                        owner_username = excluded.owner_username,
                        model_label = excluded.model_label,
                        custom_messages_json = excluded.custom_messages_json,
                        proxy = excluded.proxy,
                        profile_note = excluded.profile_note,
                        automation_enabled = excluded.automation_enabled,
                        is_suspended = excluded.is_suspended
                    """,
                    (
                        username,
                        str(_row_get(row, "password", "") or "") or None,
                        str(_row_get(row, "owner_username", "master") or "master").strip().lower() or "master",
                        str(_row_get(row, "model_label", "") or "").strip(),
                        str(_row_get(row, "custom_messages_json", "[]") or "[]"),
                        str(_row_get(row, "proxy", "") or "").strip(),
                        str(_row_get(row, "profile_note", "") or "").strip(),
                        _safe_int(_row_get(row, "automation_enabled", 1), default=1),
                        _safe_int(_row_get(row, "is_suspended", 0), default=0),
                    ),
                )
                copied_counts["accounts"] += 1

        if _sqlite_table_exists(source_conn, "models"):
            rows = source_conn.execute("SELECT username FROM models").fetchall()
            for row in rows:
                username = str(row["username"] or "").strip().lstrip("@")
                if not username:
                    continue
                target_conn.execute(
                    "INSERT INTO models (username) VALUES (?) ON CONFLICT(username) DO NOTHING",
                    (username,),
                )
                copied_counts["models"] += 1

        if _sqlite_table_exists(source_conn, "messages"):
            rows = source_conn.execute("SELECT text FROM messages").fetchall()
            for row in rows:
                text = str(row["text"] or "").strip()
                if not text:
                    continue
                target_conn.execute(
                    "INSERT INTO messages (text) VALUES (?) ON CONFLICT(text) DO NOTHING",
                    (text,),
                )
                copied_counts["messages"] += 1

        if _sqlite_table_exists(source_conn, "cookies"):
            rows = source_conn.execute("SELECT username, cookie_json FROM cookies").fetchall()
            for row in rows:
                username = str(row["username"] or "").strip()
                if not username:
                    continue
                target_conn.execute(
                    "INSERT INTO cookies (username, cookie_json) VALUES (?, ?) "
                    "ON CONFLICT(username) DO UPDATE SET cookie_json=excluded.cookie_json",
                    (username, str(row["cookie_json"] or "[]")),
                )
                copied_counts["cookies"] += 1

        if _sqlite_table_exists(source_conn, "dm_log"):
            rows = source_conn.execute("SELECT username, timestamp FROM dm_log").fetchall()
            for row in rows:
                username = str(row["username"] or "").strip()
                if not username:
                    continue
                target_conn.execute(
                    "INSERT INTO dm_log (username, timestamp) VALUES (?, ?) "
                    "ON CONFLICT(username) DO UPDATE SET timestamp=excluded.timestamp",
                    (username, str(row["timestamp"] or "")),
                )
                copied_counts["dm_log"] += 1

        if _sqlite_table_exists(source_conn, "dm_event_log"):
            rows = source_conn.execute(
                "SELECT sender_account, target_username, model_username, status, timestamp FROM dm_event_log"
            ).fetchall()
            for row in rows:
                target_conn.execute(
                    """
                    INSERT INTO dm_event_log (
                        sender_account,
                        target_username,
                        model_username,
                        status,
                        timestamp
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["sender_account"] or "").strip(),
                        str(row["target_username"] or "").strip(),
                        str(row["model_username"] or "").strip(),
                        str(row["status"] or "sent").strip(),
                        str(row["timestamp"] or "").strip(),
                    ),
                )
                copied_counts["dm_event_log"] += 1

        if _sqlite_table_exists(source_conn, "activity_log"):
            rows = source_conn.execute(
                """
                SELECT actor_username, actor_role, action, target_type, target_value, details_json, created_at
                FROM activity_log
                """
            ).fetchall()
            for row in rows:
                target_conn.execute(
                    """
                    INSERT INTO activity_log (
                        actor_username,
                        actor_role,
                        action,
                        target_type,
                        target_value,
                        details_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["actor_username"] or "").strip(),
                        str(row["actor_role"] or "").strip(),
                        str(row["action"] or "").strip(),
                        str(row["target_type"] or "").strip(),
                        str(row["target_value"] or "").strip(),
                        str(row["details_json"] or "{}"),
                        str(row["created_at"] or "").strip(),
                    ),
                )
                copied_counts["activity_log"] += 1

        target_conn.commit()

        # Re-apply DB bootstrap to backfill any newly introduced defaults
        # and guarantee a master user exists after copy.
        database.init_db()

        print("SQLite -> PostgreSQL migration complete.")
        print(f"Source SQLite: {source_path}")
        for key in (
            "settings",
            "users",
            "accounts",
            "models",
            "messages",
            "cookies",
            "dm_log",
            "dm_event_log",
            "activity_log",
        ):
            print(f"  {key}: {copied_counts[key]}")
    except Exception:
        try:
            target_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        source_conn.close()
        target_conn.close()


def configure_postgres_database_url(database_url: str):
    """Configure PostgreSQL target URL at runtime for migration CLI usage."""
    clean_url = str(database_url or "").strip()
    if not clean_url:
        return

    if not clean_url.startswith(("postgres://", "postgresql://")):
        raise ValueError("--database-url must start with postgres:// or postgresql://")

    os.environ["DATABASE_URL"] = clean_url
    database.DATABASE_URL = clean_url
    database.USE_POSTGRES = True


def parse_args():
    parser = argparse.ArgumentParser(description="Data migration utilities")
    parser.add_argument(
        "--sqlite-to-postgres",
        action="store_true",
        help="Migrate full data from SQLite file into PostgreSQL (requires DATABASE_URL env var or --database-url).",
    )
    parser.add_argument(
        "--sqlite-path",
        default=database.DATABASE_PATH,
        help="Path to source SQLite file for --sqlite-to-postgres (default: config/database.py DATABASE_PATH).",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="PostgreSQL DATABASE_URL. If provided, overrides current environment variable.",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.sqlite_to_postgres:
        if args.database_url:
            configure_postgres_database_url(args.database_url)
        run_sqlite_to_postgres_migration(sqlite_path=args.sqlite_path)
    else:
        run_json_seed_migration()

import sqlite3
import json
import os
from datetime import datetime, timedelta
from uuid import uuid4
from werkzeug.security import generate_password_hash, check_password_hash

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATABASE_PATH = os.path.join(DATA_DIR, "app_data.db")
DATABASE_URL = str(os.environ.get("DATABASE_URL", "") or "").strip()
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))


class _PgQueryResult:
    """Lightweight result wrapper that mimics sqlite cursor fetch APIs."""

    def __init__(self, rowcount: int, rows=None):
        self.rowcount = int(rowcount or 0)
        self._rows = list(rows or [])
        self._idx = 0

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def fetchall(self):
        if self._idx >= len(self._rows):
            return []
        rows = self._rows[self._idx:]
        self._idx = len(self._rows)
        return rows


class _PgConnectionAdapter:
    """Adapter so callers can keep using conn.execute(...).fetch* style."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None):
        translated_sql = str(sql or "").replace("?", "%s")
        bind_params = params if params is not None else ()

        cursor = self._conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(translated_sql, bind_params)

        rowcount = cursor.rowcount
        if cursor.description:
            rows = cursor.fetchall()
        else:
            rows = []

        cursor.close()
        return _PgQueryResult(rowcount=rowcount, rows=rows)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


INTEGRITY_ERRORS = (sqlite3.IntegrityError,)
if psycopg2 is not None:
    INTEGRITY_ERRORS = INTEGRITY_ERRORS + (psycopg2.IntegrityError,)

# Canonical setting keys live in the database. These values are only used to
# seed missing rows during initialization.
DEFAULT_DB_SETTINGS = {
    "TELEGRAM_BOT_TOKEN": "8671289565:AAFxbYRSVvPkFRUaymh2T7BG6hyE-oIXXnE",
    "TELEGRAM_CHAT_IDS": ["128663994"],
    "LOCK_ALERT_BOT_TOKEN": "",
    "LOCK_ALERT_CHAT_IDS": [],
    "MODEL_MESSAGE_MAP": {},
    "MODEL_MESSAGE_META": {},
    "DM_MIN_PER_MODEL": 5,
    "DM_MAX_PER_MODEL": 10,
    "DM_DELAY_MIN": 3,
    "DM_DELAY_MAX": 7,
    "DM_BATCH_PAUSE_ENABLED": False,
    "DM_RANDOM_EMOJI_SUFFIX_ENABLED": False,
    "DM_RANDOM_EMOJI_SUFFIX_POOL": ["🙂", "😊", "😉", "✨", "🌸", "🙏"],
    "ACTION_DELAY_MIN": 2,
    "ACTION_DELAY_MAX": 5,
    "TYPING_DELAY_MIN": 0.05,
    "TYPING_DELAY_MAX": 0.15,
    "ACCOUNT_SWITCH_DELAY_MIN": 10,
    "ACCOUNT_SWITCH_DELAY_MAX": 20,
    "MODEL_SWITCH_DELAY_MIN": 15,
    "MODEL_SWITCH_DELAY_MAX": 20,
    "COOLDOWN_MIN": 25,
    "COOLDOWN_MAX": 40,
    "POST_AGE_PRIORITY_HOURS": 24,
    "MAX_POSTS_TO_CHECK": 6,
    "MAX_LIKERS_PER_POST": 30,
    "TARGET_ENGAGEMENT_MAX_USERS_PER_MODEL": 20,
    "TARGET_ENGAGEMENT_MAX_POST_AGE_HOURS": 4,
    "COMMENT_LIKING_ENABLED": False,
    "COMMENT_LIKING_MAX_POSTS_PER_PASS": 50,
    "COMMENT_LIKING_MIN_LIKES_PER_PASS": 50,
    "COMMENT_LIKING_MAX_LIKES_PER_PASS": 50,
    "COMMENT_LIKING_MIN_COMMENTS_PER_PASS": 50,
    "COMMENT_LIKING_MAX_COMMENTS_PER_PASS": 50,
    "COMMENT_LIKING_COMMENT_CHANCE_PCT": 100,
    "COMMENT_LIKING_COMMENT_POOL": [
        "Nice post!",
        "Love this!",
        "Great shot!",
        "Amazing vibe!",
        "So good!",
        "This looks awesome!",
    ],
    "MAX_FOLLOWERS_TO_SCRAPE": 50,
    "CHALLENGE_WAIT_TIMEOUT": 300,
    "CHALLENGE_POLL_INTERVAL": 5,
    "REDIS_COORDINATION_ENABLED": False,
    "REDIS_URL": "",
    "REDIS_LOCK_NAMESPACE": "igbot:v1",
    "REDIS_ACCOUNT_LOCK_TTL_SEC": 180,
    "REDIS_ACCOUNT_HEARTBEAT_SEC": 30,
    "REDIS_INSTANCE_HEARTBEAT_TTL_SEC": 90,
    "REDIS_TARGET_CLAIM_TTL_SEC": 86400,
    "GLOBAL_TARGET_DEDUP_ENABLED": False,
    "REDIS_FAIL_CLOSED": True,
    "REDIS_INSTANCE_ID": "",
    "WEB_UI_USERNAME": "beyinstabot",
    "WEB_UI_PASSWORD": "#beymedia!",
}

def _get_connection():
    if USE_POSTGRES:
        if psycopg2 is None:
            raise RuntimeError(
                "DATABASE_URL is set for PostgreSQL but psycopg2 is not installed. "
                "Install dependencies with: pip install -r requirements.txt"
            )
        raw_conn = psycopg2.connect(DATABASE_URL)
        raw_conn.autocommit = False
        return _PgConnectionAdapter(raw_conn)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def using_postgres() -> bool:
    return bool(USE_POSTGRES)


def _table_columns(conn, table_name: str) -> set:
    if USE_POSTGRES:
        rows = conn.execute(
            """
            SELECT column_name AS name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = ?
            """,
            (str(table_name or "").strip().lower(),),
        ).fetchall()
        return {str(r["name"] or "").strip() for r in rows if str(r.get("name", "")).strip()}

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {r["name"] for r in rows}


def _get_setting_from_conn(conn, key: str, default=None):
    row = conn.execute("SELECT value_json FROM settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def _ensure_default_settings(conn):
    for key, value in DEFAULT_DB_SETTINGS.items():
        exists = conn.execute(
            "SELECT 1 FROM settings WHERE key = ?",
            (key,),
        ).fetchone()
        if exists:
            continue

        conn.execute(
            "INSERT INTO settings (key, value_json) VALUES (?, ?)",
            (key, json.dumps(value)),
        )


def _ensure_account_owner_column(conn):
    if "owner_username" in _table_columns(conn, "accounts"):
        return

    conn.execute("ALTER TABLE accounts ADD COLUMN owner_username TEXT NOT NULL DEFAULT 'master'")
    conn.execute("UPDATE accounts SET owner_username = 'master' WHERE owner_username IS NULL OR owner_username = ''")


def _ensure_account_model_label_column(conn):
    if "model_label" in _table_columns(conn, "accounts"):
        return

    conn.execute("ALTER TABLE accounts ADD COLUMN model_label TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE accounts SET model_label = '' WHERE model_label IS NULL")


def _ensure_account_custom_messages_column(conn):
    columns = _table_columns(conn, "accounts")
    if "custom_messages_json" not in columns:
        conn.execute("ALTER TABLE accounts ADD COLUMN custom_messages_json TEXT NOT NULL DEFAULT '[]'")

    conn.execute(
        "UPDATE accounts SET custom_messages_json = '[]' "
        "WHERE custom_messages_json IS NULL OR TRIM(custom_messages_json) = ''"
    )


def _ensure_account_proxy_column(conn):
    if "proxy" in _table_columns(conn, "accounts"):
        return
    conn.execute("ALTER TABLE accounts ADD COLUMN proxy TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE accounts SET proxy = '' WHERE proxy IS NULL")


def _ensure_account_profile_note_column(conn):
    if "profile_note" in _table_columns(conn, "accounts"):
        return
    conn.execute("ALTER TABLE accounts ADD COLUMN profile_note TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE accounts SET profile_note = '' WHERE profile_note IS NULL")


def _ensure_account_automation_enabled_column(conn):
    if "automation_enabled" in _table_columns(conn, "accounts"):
        return
    conn.execute("ALTER TABLE accounts ADD COLUMN automation_enabled INTEGER NOT NULL DEFAULT 1")
    conn.execute("UPDATE accounts SET automation_enabled = 1 WHERE automation_enabled IS NULL")


def _ensure_account_suspended_column(conn):
    if "is_suspended" in _table_columns(conn, "accounts"):
        return
    conn.execute("ALTER TABLE accounts ADD COLUMN is_suspended INTEGER NOT NULL DEFAULT 0")
    conn.execute("UPDATE accounts SET is_suspended = 0 WHERE is_suspended IS NULL")


def _normalize_account_model_label(value) -> str:
    clean = str(value or "").strip().lstrip("@")
    key = clean.lower()
    if key in ("", "generic", "any", "all", "*", "none"):
        return ""
    return clean


def _normalize_account_custom_messages(raw_messages) -> list:
    if not isinstance(raw_messages, list):
        return []

    clean_messages = []
    for msg in raw_messages:
        if not isinstance(msg, str):
            continue
        trimmed = msg.strip()
        if trimmed:
            clean_messages.append(trimmed)
    return clean_messages


def _normalize_account_automation_enabled(raw_value, default: bool = True) -> int:
    if isinstance(raw_value, bool):
        return 1 if raw_value else 0

    if raw_value is None:
        return 1 if default else 0

    if isinstance(raw_value, (int, float)):
        return 0 if int(raw_value) == 0 else 1

    text = str(raw_value).strip().lower()
    if text in ("", "none", "null"):
        return 1 if default else 0
    if text in ("0", "false", "off", "no", "disable", "disabled"):
        return 0
    if text in ("1", "true", "on", "yes", "enable", "enabled"):
        return 1

    return 1 if default else 0


def _normalize_account_suspended(raw_value, default: bool = False) -> int:
    if isinstance(raw_value, bool):
        return 1 if raw_value else 0

    if raw_value is None:
        return 1 if default else 0

    if isinstance(raw_value, (int, float)):
        return 0 if int(raw_value) == 0 else 1

    text = str(raw_value).strip().lower()
    if text in ("", "none", "null"):
        return 1 if default else 0
    if text in ("0", "false", "off", "no", "active", "unsuspended", "unlock", "unlocked"):
        return 0
    if text in ("1", "true", "on", "yes", "suspend", "suspended", "lock", "locked"):
        return 1

    return 1 if default else 0


def _ensure_master_user(conn):
    users_count_row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    users_count = int(users_count_row["c"] if users_count_row else 0)

    if users_count > 0:
        master_row = conn.execute("SELECT id FROM users WHERE role = 'master' LIMIT 1").fetchone()
        if master_row:
            return

    seed_username = str(
        _get_setting_from_conn(conn, "WEB_UI_USERNAME", DEFAULT_DB_SETTINGS["WEB_UI_USERNAME"]) or ""
    ).strip().lower()
    seed_password = str(
        _get_setting_from_conn(conn, "WEB_UI_PASSWORD", DEFAULT_DB_SETTINGS["WEB_UI_PASSWORD"]) or ""
    )
    if not seed_username:
        seed_username = "beyinstabot"
    if not seed_password:
        seed_password = "#beymedia!"

    existing_named = conn.execute("SELECT id FROM users WHERE username = ?", (seed_username,)).fetchone()
    now_str = datetime.now().isoformat()

    if existing_named:
        conn.execute("UPDATE users SET role = 'master', is_active = 1 WHERE id = ?", (existing_named["id"],))
        return

    conn.execute(
        """
        INSERT INTO users (username, password_hash, role, is_active, created_at)
        VALUES (?, ?, 'master', 1, ?)
        """,
        (seed_username, generate_password_hash(seed_password), now_str),
    )

def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = _get_connection()
    try:
        id_primary = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"

        # Accounts Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS accounts (
                id {id_primary},
                username TEXT UNIQUE NOT NULL,
                password TEXT,
                owner_username TEXT NOT NULL DEFAULT 'master',
                model_label TEXT NOT NULL DEFAULT '',
                custom_messages_json TEXT NOT NULL DEFAULT '[]',
                proxy TEXT NOT NULL DEFAULT '',
                profile_note TEXT NOT NULL DEFAULT '',
                automation_enabled INTEGER NOT NULL DEFAULT 1,
                is_suspended INTEGER NOT NULL DEFAULT 0
            )
        """)
        
        # Models Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS models (
                id {id_primary},
                username TEXT UNIQUE NOT NULL
            )
        """)
        
        # Messages Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS messages (
                id {id_primary},
                text TEXT UNIQUE NOT NULL
            )
        """)

        # Comment Pool Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS comments (
                id {id_primary},
                text TEXT UNIQUE NOT NULL
            )
        """)
        
        # Settings Table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            )
        """)

        # Cluster Notification Dedupe Table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_dedup (
                event_key TEXT PRIMARY KEY,
                last_sent_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notification_dedup_last_sent_at ON notification_dedup(last_sent_at DESC)"
        )

        _ensure_default_settings(conn)
        
        # DM Logs Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS dm_log (
                id {id_primary},
                username TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)

        # DM Event Log Table (full per-attempt history for reporting)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS dm_event_log (
                id {id_primary},
                sender_account TEXT NOT NULL,
                target_username TEXT NOT NULL,
                model_username TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'sent',
                timestamp TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dm_event_log_sender ON dm_event_log(sender_account)"
        )
        
        # Engagement Log Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS engagement_log (
                id {id_primary},
                sender_account TEXT NOT NULL,
                target_username TEXT NOT NULL,
                engagement_type TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_engagement_log_timestamp ON engagement_log(timestamp DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dm_event_log_sender_ts ON dm_event_log(sender_account, timestamp DESC)"
        )

        # Dashboard Users Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS users (
                id {id_primary},
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                CHECK(role IN ('master', 'employee'))
            )
        """)
        
        # Cookies Table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cookies (
                username TEXT PRIMARY KEY,
                cookie_json TEXT NOT NULL
            )
        """)

        # Employee Activity Log Table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS activity_log (
                id {id_primary},
                actor_username TEXT NOT NULL,
                actor_role TEXT NOT NULL,
                action TEXT NOT NULL,
                target_type TEXT NOT NULL DEFAULT '',
                target_value TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{{}}',
                created_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activity_log_created_at ON activity_log(created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activity_log_actor ON activity_log(actor_username)"
        )

        # Inbox Messages Table (scraped DM threads)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS inbox_messages (
                id {id_primary},
                ig_account TEXT NOT NULL,
                thread_display_name TEXT NOT NULL,
                thread_username TEXT NOT NULL DEFAULT '',
                message_preview TEXT NOT NULL DEFAULT '',
                timestamp_short TEXT NOT NULL DEFAULT '',
                timestamp_label TEXT NOT NULL DEFAULT '',
                profile_pic_url TEXT NOT NULL DEFAULT '',
                is_unread INTEGER NOT NULL DEFAULT 1,
                scraped_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_messages_account ON inbox_messages(ig_account)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_messages_scraped ON inbox_messages(scraped_at DESC)"
        )

        # Inbox Thread Message Lines Table (per-thread conversation history)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS inbox_thread_lines (
                id {id_primary},
                thread_id INTEGER NOT NULL,
                sender_name TEXT NOT NULL DEFAULT '',
                text_content TEXT NOT NULL DEFAULT '',
                has_attachment INTEGER NOT NULL DEFAULT 0,
                timestamp TEXT NOT NULL DEFAULT '',
                is_self INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_thread_lines_thread_id ON inbox_thread_lines(thread_id, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_thread_lines_timestamp ON inbox_thread_lines(timestamp)"
        )

        # Inbox Reply Queue Table (persistent queued/running reply jobs)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS inbox_reply_queue (
                id {id_primary},
                job_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                progress TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                ig_account TEXT NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                thread_display_name TEXT NOT NULL DEFAULT '',
                text_message TEXT NOT NULL DEFAULT '',
                has_attachment INTEGER NOT NULL DEFAULT 0,
                upload_path TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT '',
                CHECK(status IN ('queued', 'running', 'done', 'error'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_reply_queue_account_status ON inbox_reply_queue(ig_account, status, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_reply_queue_status_updated ON inbox_reply_queue(status, updated_at DESC)"
        )

        # Schema/data migrations
        _ensure_account_owner_column(conn)
        _ensure_account_model_label_column(conn)
        _ensure_account_custom_messages_column(conn)
        _ensure_account_proxy_column(conn)
        _ensure_account_profile_note_column(conn)
        _ensure_account_automation_enabled_column(conn)
        _ensure_account_suspended_column(conn)
        _ensure_master_user(conn)

        # Force-update comment-liking targets to 50 per pass
        _force_update_comment_liking_targets(conn)

        conn.commit()
    finally:
        conn.close()


def _force_update_comment_liking_targets(conn):
    """One-time migration: raise comment-liking targets to 50 per pass."""
    updates = {
        "COMMENT_LIKING_MAX_POSTS_PER_PASS": 50,
        "COMMENT_LIKING_MIN_LIKES_PER_PASS": 50,
        "COMMENT_LIKING_MAX_LIKES_PER_PASS": 50,
        "COMMENT_LIKING_MIN_COMMENTS_PER_PASS": 50,
        "COMMENT_LIKING_MAX_COMMENTS_PER_PASS": 50,
        "COMMENT_LIKING_COMMENT_CHANCE_PCT": 100,
    }
    for key, new_value in updates.items():
        conn.execute(
            "UPDATE settings SET value_json = ? WHERE key = ?",
            (json.dumps(new_value), key),
        )


# ── Auth/User CRUD ──
def authenticate_user(username: str, password: str):
    clean_username = str(username or "").strip().lower()
    if not clean_username or not password:
        return None

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT username, password_hash, role, is_active FROM users WHERE username = ?",
            (clean_username,),
        ).fetchone()
        if not row or int(row["is_active"] or 0) != 1:
            return None

        stored_hash = row["password_hash"] or ""
        valid = False

        # Fallback allows one-time migration from legacy plain-text values if any exist.
        if stored_hash.startswith("scrypt:") or stored_hash.startswith("pbkdf2:"):
            valid = check_password_hash(stored_hash, password)
        else:
            valid = stored_hash == password
            if valid:
                conn.execute(
                    "UPDATE users SET password_hash = ? WHERE username = ?",
                    (generate_password_hash(password), clean_username),
                )
                conn.commit()

        if not valid:
            return None

        return {"username": row["username"], "role": row["role"]}
    finally:
        conn.close()


def get_users():
    conn = _get_connection()
    try:
        rows = conn.execute(
            """
            SELECT username, role, is_active, created_at
            FROM users
            ORDER BY CASE role WHEN 'master' THEN 0 ELSE 1 END, username
            """
        ).fetchall()
        return [
            {
                "username": r["username"],
                "role": r["role"],
                "is_active": bool(r["is_active"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def create_user(username: str, password: str, role: str = "employee"):
    clean_username = str(username or "").strip().lower()
    clean_password = str(password or "")
    clean_role = str(role or "employee").strip().lower()

    if clean_role not in ("master", "employee"):
        raise ValueError("Invalid role")
    if not clean_username:
        raise ValueError("Username is required")
    if len(clean_password) < 4:
        raise ValueError("Password must be at least 4 characters")

    conn = _get_connection()
    try:
        now_str = datetime.now().isoformat()
        conn.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (clean_username, generate_password_hash(clean_password), clean_role, now_str),
        )
        conn.commit()
        return {"username": clean_username, "role": clean_role, "is_active": True, "created_at": now_str}
    except INTEGRITY_ERRORS:
        raise ValueError("User already exists")
    finally:
        conn.close()


def update_user_password(username: str, new_password: str):
    clean_username = str(username or "").strip().lower()
    clean_password = str(new_password or "")
    if not clean_username:
        raise ValueError("Username is required")
    if len(clean_password) < 4:
        raise ValueError("Password must be at least 4 characters")

    conn = _get_connection()
    try:
        updated = conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (generate_password_hash(clean_password), clean_username),
        ).rowcount
        conn.commit()
        return updated > 0
    finally:
        conn.close()


def update_user_credentials(username: str, new_username: str = None, new_password: str = None):
    """Update a dashboard user's username and/or password.

    Returns dict with old/new username and role when successful, None if user is missing.
    """
    clean_current = str(username or "").strip().lower()
    if not clean_current:
        raise ValueError("Username is required")

    candidate_username = str(new_username if new_username is not None else clean_current).strip().lower()
    if not candidate_username:
        raise ValueError("Username is required")

    password_to_set = None
    if new_password is not None and str(new_password) != "":
        password_to_set = str(new_password)
        if len(password_to_set) < 4:
            raise ValueError("Password must be at least 4 characters")

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT username, role FROM users WHERE username = ?",
            (clean_current,),
        ).fetchone()
        if not row:
            return None

        if candidate_username != clean_current:
            exists = conn.execute(
                "SELECT 1 FROM users WHERE username = ?",
                (candidate_username,),
            ).fetchone()
            if exists:
                raise ValueError("User already exists")

        if password_to_set is not None:
            conn.execute(
                "UPDATE users SET username = ?, password_hash = ? WHERE username = ?",
                (candidate_username, generate_password_hash(password_to_set), clean_current),
            )
        else:
            conn.execute(
                "UPDATE users SET username = ? WHERE username = ?",
                (candidate_username, clean_current),
            )

        if candidate_username != clean_current:
            conn.execute(
                "UPDATE accounts SET owner_username = ? WHERE owner_username = ?",
                (candidate_username, clean_current),
            )

        conn.commit()
        return {
            "old_username": clean_current,
            "username": candidate_username,
            "role": row["role"],
        }
    finally:
        conn.close()


def delete_user(username: str):
    clean_username = str(username or "").strip().lower()
    if not clean_username:
        raise ValueError("Username is required")

    conn = _get_connection()
    try:
        row = conn.execute("SELECT role FROM users WHERE username = ?", (clean_username,)).fetchone()
        if not row:
            return False
        if row["role"] == "master":
            raise ValueError("Cannot delete master user")

        # Keep account ownership valid by re-assigning to master.
        conn.execute("UPDATE accounts SET owner_username = 'master' WHERE owner_username = ?", (clean_username,))
        conn.execute("DELETE FROM users WHERE username = ?", (clean_username,))
        conn.commit()
        return True
    finally:
        conn.close()


def is_valid_admin_login(username, password):
    """Backward-compatible alias used by older server routes."""
    return authenticate_user(username, password) is not None

# ── Accounts CRUD ──
def get_accounts(owner_username: str = None, include_all: bool = False):
    conn = _get_connection()
    try:
        if include_all:
            rows = conn.execute(
                "SELECT username, password, owner_username, model_label, custom_messages_json, proxy, profile_note, automation_enabled, is_suspended "
                "FROM accounts ORDER BY owner_username, username"
            ).fetchall()
        else:
            clean_owner = str(owner_username or "").strip().lower()
            if not clean_owner:
                return []
            rows = conn.execute(
                "SELECT username, password, owner_username, model_label, custom_messages_json, proxy, profile_note, automation_enabled, is_suspended "
                "FROM accounts WHERE owner_username = ? ORDER BY username",
                (clean_owner,),
            ).fetchall()

        accounts = []
        for r in rows:
            try:
                raw_custom_messages = json.loads(r["custom_messages_json"] or "[]")
            except Exception:
                raw_custom_messages = []

            accounts.append(
                {
                    "username": r["username"],
                    "password": r["password"] or "",
                    "owner_username": r["owner_username"] or "master",
                    "model_label": str(r["model_label"] or "").strip(),
                    "custom_messages": _normalize_account_custom_messages(raw_custom_messages),
                    "proxy": str(r["proxy"] or "").strip(),
                    "profile_note": str(r["profile_note"] or "").strip(),
                    "automation_enabled": bool(_normalize_account_automation_enabled(r["automation_enabled"], default=True)),
                    "is_suspended": bool(
                        _normalize_account_suspended(
                            r["is_suspended"] if "is_suspended" in r.keys() else 0,
                            default=False,
                        )
                    ),
                }
            )

        return accounts
    finally:
        conn.close()

def save_accounts(accounts_list, owner_username: str = None, include_all: bool = False):
    if not isinstance(accounts_list, list):
        raise ValueError("Accounts payload must be a list")

    conn = _get_connection()
    try:
        if include_all:
            previous_rows = conn.execute("SELECT username FROM accounts").fetchall()
            clean_owner = ""
        else:
            clean_owner = str(owner_username or "").strip().lower()
            if not clean_owner:
                raise ValueError("Owner username is required")
            previous_rows = conn.execute(
                "SELECT username FROM accounts WHERE owner_username = ?",
                (clean_owner,),
            ).fetchall()

        previous_username_keys = set()
        for row in previous_rows:
            username = str(row["username"] or "").strip().lower()
            if username:
                previous_username_keys.add(username)

        incoming_username_keys = set()

        if include_all:
            conn.execute("DELETE FROM accounts")
            for acc in accounts_list:
                username = str(acc.get("username", "")).strip()
                if not username:
                    continue
                incoming_username_keys.add(username.lower())
                password = str(acc.get("password", "")).strip() or None
                owner = str(acc.get("owner_username") or acc.get("owner") or "master").strip().lower()
                owner = owner or "master"
                model_label = _normalize_account_model_label(acc.get("model_label", ""))
                custom_messages_json = json.dumps(
                    _normalize_account_custom_messages(acc.get("custom_messages", []))
                )
                proxy = str(acc.get("proxy", "") or "").strip()
                profile_note = str(acc.get("profile_note", "") or "").strip()
                automation_enabled = _normalize_account_automation_enabled(acc.get("automation_enabled", True), default=True)
                is_suspended = _normalize_account_suspended(acc.get("is_suspended", False), default=False)
                if is_suspended:
                    automation_enabled = 0
                if not profile_note:
                    raise ValueError(f"Bio + URL is required for account '{username}'")
                conn.execute(
                    "INSERT INTO accounts (username, password, owner_username, model_label, custom_messages_json, proxy, profile_note, automation_enabled, is_suspended) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        username,
                        password,
                        owner,
                        model_label,
                        custom_messages_json,
                        proxy,
                        profile_note,
                        automation_enabled,
                        is_suspended,
                    ),
                )
        else:
            conn.execute("DELETE FROM accounts WHERE owner_username = ?", (clean_owner,))
            for acc in accounts_list:
                username = str(acc.get("username", "")).strip()
                if not username:
                    continue
                incoming_username_keys.add(username.lower())
                password = str(acc.get("password", "")).strip() or None
                model_label = _normalize_account_model_label(acc.get("model_label", ""))
                custom_messages_json = json.dumps(
                    _normalize_account_custom_messages(acc.get("custom_messages", []))
                )
                proxy = str(acc.get("proxy", "") or "").strip()
                profile_note = str(acc.get("profile_note", "") or "").strip()
                automation_enabled = _normalize_account_automation_enabled(acc.get("automation_enabled", True), default=True)
                is_suspended = _normalize_account_suspended(acc.get("is_suspended", False), default=False)
                if is_suspended:
                    automation_enabled = 0
                if not profile_note:
                    raise ValueError(f"Bio + URL is required for account '{username}'")
                conn.execute(
                    "INSERT INTO accounts (username, password, owner_username, model_label, custom_messages_json, proxy, profile_note, automation_enabled, is_suspended) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        username,
                        password,
                        clean_owner,
                        model_label,
                        custom_messages_json,
                        proxy,
                        profile_note,
                        automation_enabled,
                        is_suspended,
                    ),
                )

        removed_username_keys = previous_username_keys - incoming_username_keys
        for username_key in removed_username_keys:
            conn.execute("DELETE FROM cookies WHERE LOWER(username) = ?", (username_key,))

        conn.commit()
    except INTEGRITY_ERRORS:
        raise ValueError("One or more account usernames are already assigned")
    finally:
        conn.close()


def update_account_proxy(account_username: str, proxy_value: str) -> bool:
    """Update proxy for an existing IG account username."""
    clean_username = str(account_username or "").strip()
    if not clean_username:
        raise ValueError("Account username is required")

    clean_proxy = str(proxy_value or "").strip()

    conn = _get_connection()
    try:
        result = conn.execute(
            "UPDATE accounts SET proxy = ? WHERE username = ?",
            (clean_proxy, clean_username),
        )
        conn.commit()
        return (result.rowcount or 0) > 0
    finally:
        conn.close()


def set_account_automation_enabled(account_username: str, enabled: bool) -> bool:
    """Enable or disable automation for a specific IG account username."""
    clean_username = str(account_username or "").strip().lstrip("@")
    if not clean_username:
        raise ValueError("Account username is required")

    automation_enabled = 1 if bool(enabled) else 0

    conn = _get_connection()
    try:
        if automation_enabled:
            result = conn.execute(
                "UPDATE accounts "
                "SET automation_enabled = 1 "
                "WHERE LOWER(username) = LOWER(?) AND COALESCE(is_suspended, 0) = 0",
                (clean_username,),
            )
        else:
            result = conn.execute(
                "UPDATE accounts SET automation_enabled = 0 WHERE LOWER(username) = LOWER(?)",
                (clean_username,),
            )
        conn.commit()
        return (result.rowcount or 0) > 0
    finally:
        conn.close()


def set_account_suspended(account_username: str, suspended: bool = True) -> bool:
    """Mark an account as suspended/active. Suspended accounts are forced automation-disabled."""
    clean_username = str(account_username or "").strip().lstrip("@")
    if not clean_username:
        raise ValueError("Account username is required")

    is_suspended = 1 if bool(suspended) else 0

    conn = _get_connection()
    try:
        if is_suspended:
            result = conn.execute(
                "UPDATE accounts SET is_suspended = 1, automation_enabled = 0 WHERE LOWER(username) = LOWER(?)",
                (clean_username,),
            )
        else:
            result = conn.execute(
                "UPDATE accounts SET is_suspended = 0 WHERE LOWER(username) = LOWER(?)",
                (clean_username,),
            )
        conn.commit()
        return (result.rowcount or 0) > 0
    finally:
        conn.close()


def is_account_suspended(account_username: str, default: bool = False) -> bool:
    """Return current suspended flag for an account username."""
    clean_username = str(account_username or "").strip().lstrip("@")
    if not clean_username:
        return bool(default)

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT is_suspended FROM accounts WHERE LOWER(username) = LOWER(?)",
            (clean_username,),
        ).fetchone()
        if not row:
            return bool(default)
        raw_value = row["is_suspended"] if "is_suspended" in row.keys() else 0
        return bool(_normalize_account_suspended(raw_value, default=default))
    finally:
        conn.close()


def is_account_automation_enabled(account_username: str, default: bool = True) -> bool:
    """Return current automation flag for an account username."""
    clean_username = str(account_username or "").strip().lstrip("@")
    if not clean_username:
        return bool(default)

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT automation_enabled FROM accounts WHERE LOWER(username) = LOWER(?)",
            (clean_username,),
        ).fetchone()
        if not row:
            return bool(default)
        return bool(_normalize_account_automation_enabled(row["automation_enabled"], default=default))
    finally:
        conn.close()


def user_can_access_account(account_username: str, requester_username: str, requester_role: str) -> bool:
    clean_requester = str(requester_username or "").strip().lower()
    clean_role = str(requester_role or "").strip().lower()
    if clean_role == "master":
        return True

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT owner_username FROM accounts WHERE username = ?",
            (str(account_username or "").strip(),),
        ).fetchone()
        if not row:
            return False
        return (row["owner_username"] or "").strip().lower() == clean_requester
    finally:
        conn.close()

# ── Models CRUD ──
def get_models():
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT username FROM models").fetchall()
        return [str(r["username"] or "").strip() for r in rows if str(r["username"] or "").strip()]
    finally:
        conn.close()

def save_models(models_list):
    conn = _get_connection()
    try:
        conn.execute("DELETE FROM models")
        for model in models_list:
            clean_model = str(model or "").strip().lstrip("@")
            if not clean_model:
                continue
            conn.execute(
                "INSERT INTO models (username) VALUES (?) ON CONFLICT(username) DO NOTHING",
                (clean_model,),
            )
        conn.commit()
    finally:
        conn.close()

# ── Messages CRUD ──
def get_messages():
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT text FROM messages").fetchall()
        return [str(r["text"] or "").strip() for r in rows if str(r["text"] or "").strip()]
    finally:
        conn.close()

def save_messages(messages_list):
    conn = _get_connection()
    try:
        conn.execute("DELETE FROM messages")
        for msg in messages_list:
            clean_msg = str(msg or "").strip()
            if not clean_msg:
                continue
            conn.execute(
                "INSERT INTO messages (text) VALUES (?) ON CONFLICT(text) DO NOTHING",
                (clean_msg,),
            )
        conn.commit()
    finally:
        conn.close()

# ── Comments CRUD ──
def get_comments():
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT text FROM comments").fetchall()
        return [str(r["text"] or "").strip() for r in rows if str(r["text"] or "").strip()]
    finally:
        conn.close()

def save_comments(comments_list):
    conn = _get_connection()
    try:
        conn.execute("DELETE FROM comments")
        for comment in comments_list:
            clean_comment = str(comment or "").strip()
            if not clean_comment:
                continue
            conn.execute(
                "INSERT INTO comments (text) VALUES (?) ON CONFLICT(text) DO NOTHING",
                (clean_comment,),
            )
        conn.commit()
    finally:
        conn.close()

# ── Settings CRUD ──
def get_all_settings():
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT key, value_json FROM settings").fetchall()
        merged = {}
        for row in rows:
            try:
                merged[row["key"]] = json.loads(row["value_json"])
            except: pass
        return merged
    finally:
        conn.close()

def get_setting(key, default=None):
    all_sets = get_all_settings()
    return all_sets.get(key, default)


def get_required_setting(key):
    value = get_setting(key, None)
    if value is None:
        raise KeyError(f"Missing required setting in database: {key}")
    return value

def save_settings(settings_dict):
    conn = _get_connection()
    try:
        for k, v in settings_dict.items():
            conn.execute(
                "INSERT INTO settings (key, value_json) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (k, json.dumps(v))
            )
        conn.commit()
    finally:
        conn.close()


def claim_notification_event(event_key: str, cooldown_seconds: int = 60) -> bool:
    """Atomically claim a cluster notification slot.

    Returns True only for the first caller within the cooldown window.
    """
    clean_key = str(event_key or "").strip().lower()
    if not clean_key:
        return False

    try:
        window_seconds = max(0, int(cooldown_seconds or 0))
    except Exception:
        window_seconds = 0

    now_utc = datetime.utcnow().replace(microsecond=0)
    now_iso = now_utc.isoformat() + "Z"
    cutoff_iso = (now_utc - timedelta(seconds=window_seconds)).isoformat() + "Z"

    conn = _get_connection()
    try:
        result = conn.execute(
            """
            INSERT INTO notification_dedup (event_key, last_sent_at)
            VALUES (?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                last_sent_at = excluded.last_sent_at
            WHERE notification_dedup.last_sent_at < ?
            """,
            (clean_key, now_iso, cutoff_iso),
        )
        conn.commit()
        return int(result.rowcount or 0) > 0
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()

# ── DM Logs CRUD ──
def get_dm_logs():
    """Returns a dict mapping username -> timestamp (ISO format)"""
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT username, timestamp FROM dm_log").fetchall()
        return {r["username"]: r["timestamp"] for r in rows}
    finally:
        conn.close()

def log_dm_sent(username):
    conn = _get_connection()
    try:
        now_str = datetime.now().isoformat()
        conn.execute(
            "INSERT INTO dm_log (username, timestamp) VALUES (?, ?) ON CONFLICT(username) DO UPDATE SET timestamp=excluded.timestamp",
            (username, now_str)
        )
        conn.commit()
    finally:
        conn.close()


def log_dm_event(sender_account: str, target_username: str, model_username: str = "", status: str = "sent"):
    """Append one DM attempt event for reporting and auditing."""
    clean_sender = str(sender_account or "").strip()
    clean_target = str(target_username or "").strip()
    clean_model = str(model_username or "").strip().lstrip("@")
    clean_status = str(status or "").strip().lower() or "sent"

    if not clean_sender or not clean_target:
        return

    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO dm_event_log (sender_account, target_username, model_username, status, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                clean_sender,
                clean_target,
                clean_model,
                clean_status,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def log_engagement(sender_account: str, target_username: str, engagement_type: str):
    """Log a like or comment event for dashboard reporting."""
    clean_sender = str(sender_account or "").strip()
    clean_target = str(target_username or "").strip()
    clean_type = str(engagement_type or "").strip().lower()

    if not clean_sender or not clean_target:
        return

    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO engagement_log (sender_account, target_username, engagement_type, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (
                clean_sender,
                clean_target,
                clean_type,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_dm_sent_summary_last_hours(hours: int = 24, include_all_accounts: bool = False):
    """Return sent DM totals for the last N hours, grouped by sender account."""
    safe_hours = max(1, min(int(hours or 24), 720))
    cutoff_dt = datetime.now() - timedelta(hours=safe_hours)
    cutoff_iso = cutoff_dt.isoformat(timespec="seconds")

    conn = _get_connection()
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_event_log WHERE status = 'sent' AND timestamp >= ?",
            (cutoff_iso,),
        ).fetchone()
        total_sent = int(total_row["c"] if total_row and total_row["c"] is not None else 0)

        lifetime_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_event_log WHERE status = 'sent'"
        ).fetchone()
        lifetime_event_sent = int(
            lifetime_row["c"] if lifetime_row and lifetime_row["c"] is not None else 0
        )

        legacy_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_log WHERE timestamp IS NOT NULL AND TRIM(timestamp) != ''"
        ).fetchone()
        legacy_lifetime_sent = int(
            legacy_row["c"] if legacy_row and legacy_row["c"] is not None else 0
        )

        # Keep lifetime totals compatible with historical installs while still using new event logs.
        lifetime_total_sent = max(lifetime_event_sent, legacy_lifetime_sent)

        rows = conn.execute(
            """
            SELECT l.sender_account, COUNT(*) AS sent_count
            FROM dm_event_log l
            JOIN accounts a ON a.username = l.sender_account
            WHERE l.status = 'sent' AND l.timestamp >= ? AND a.is_suspended = 0
            GROUP BY l.sender_account
            ORDER BY sent_count DESC, l.sender_account ASC
            """,
            (cutoff_iso,),
        ).fetchall()

        counts_by_account = {}
        for row in rows:
            sender = str(row["sender_account"] or "").strip()
            if not sender:
                continue
            counts_by_account[sender] = int(row["sent_count"] or 0)

        if include_all_accounts:
            account_rows = conn.execute(
                "SELECT username FROM accounts WHERE is_suspended = 0 ORDER BY username"
            ).fetchall()
            for row in account_rows:
                username = str(row["username"] or "").strip()
                if not username:
                    continue
                counts_by_account.setdefault(username, 0)

        by_account = [
            {"sender_account": name, "count": count}
            for name, count in sorted(
                counts_by_account.items(),
                key=lambda item: (-item[1], item[0].lower()),
            )
        ]

        return {
            "hours": safe_hours,
            "cutoff": cutoff_iso,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "total_sent": total_sent,
            "lifetime_total_sent": lifetime_total_sent,
            "by_account": by_account,
        }
    finally:
        conn.close()


def get_lifetime_dm_sent_total() -> int:
    """Return lifetime sent DMs using event log with legacy fallback compatibility."""
    conn = _get_connection()
    try:
        lifetime_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_event_log WHERE status = 'sent'"
        ).fetchone()
        lifetime_event_sent = int(
            lifetime_row["c"] if lifetime_row and lifetime_row["c"] is not None else 0
        )

        legacy_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_log WHERE timestamp IS NOT NULL AND TRIM(timestamp) != ''"
        ).fetchone()
        legacy_lifetime_sent = int(
            legacy_row["c"] if legacy_row and legacy_row["c"] is not None else 0
        )

        return max(lifetime_event_sent, legacy_lifetime_sent)
    finally:
        conn.close()


def get_dm_dashboard_metrics() -> dict:
    """Return lightweight dashboard DM metrics for today and latest DM timestamp."""
    day_start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_iso = day_start_dt.isoformat(timespec="seconds")

    def _parse_iso(raw_value):
        text = str(raw_value or "").strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            return None
        if dt.tzinfo is not None:
            try:
                return dt.astimezone().replace(tzinfo=None)
            except Exception:
                return dt.replace(tzinfo=None)
        return dt

    def _format_dashboard_timestamp(raw_value):
        parsed = _parse_iso(raw_value)
        if parsed is None:
            return str(raw_value or "").strip().replace("T", " ").replace("Z", "")
        return parsed.strftime("%Y-%m-%d %H:%M:%S")

    conn = _get_connection()
    try:
        today_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_event_log WHERE status = 'sent' AND timestamp >= ?",
            (day_start_iso,),
        ).fetchone()
        dms_sent_today = int(
            today_row["c"] if today_row and today_row["c"] is not None else 0
        )

        lifetime_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_event_log WHERE status = 'sent'"
        ).fetchone()
        lifetime_event_sent = int(
            lifetime_row["c"] if lifetime_row and lifetime_row["c"] is not None else 0
        )

        legacy_row = conn.execute(
            "SELECT COUNT(*) AS c FROM dm_log WHERE timestamp IS NOT NULL AND TRIM(timestamp) != ''"
        ).fetchone()
        legacy_lifetime_sent = int(
            legacy_row["c"] if legacy_row and legacy_row["c"] is not None else 0
        )

        latest_event_row = conn.execute(
            """
            SELECT timestamp
            FROM dm_event_log
            WHERE status = 'sent' AND timestamp IS NOT NULL AND TRIM(timestamp) != ''
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_event_ts = ""
        if latest_event_row:
            latest_event_ts = str(latest_event_row["timestamp"] or "").strip()

        latest_legacy_row = conn.execute(
            """
            SELECT timestamp
            FROM dm_log
            WHERE timestamp IS NOT NULL AND TRIM(timestamp) != ''
            ORDER BY timestamp DESC
            LIMIT 1
            """
        ).fetchone()
        latest_legacy_ts = ""
        if latest_legacy_row:
            latest_legacy_ts = str(latest_legacy_row["timestamp"] or "").strip()

        event_dt = _parse_iso(latest_event_ts)
        legacy_dt = _parse_iso(latest_legacy_ts)

        if event_dt is not None and legacy_dt is not None:
            chosen_ts = latest_event_ts if event_dt >= legacy_dt else latest_legacy_ts
        elif event_dt is not None:
            chosen_ts = latest_event_ts
        elif legacy_dt is not None:
            chosen_ts = latest_legacy_ts
        else:
            chosen_ts = latest_event_ts or latest_legacy_ts

        last_dm_sent_at = _format_dashboard_timestamp(chosen_ts)

        return {
            "day_start": day_start_iso,
            "dms_sent_today": dms_sent_today,
            "lifetime_total_sent": max(lifetime_event_sent, legacy_lifetime_sent),
            "last_dm_sent_at": last_dm_sent_at,
        }
    finally:
        conn.close()

# ── Cookies CRUD ──
def get_cookies(username):
    conn = _get_connection()
    try:
        row = conn.execute("SELECT cookie_json FROM cookies WHERE username = ?", (username,)).fetchone()
        if row:
            try: return json.loads(row["cookie_json"])
            except: return []
        return []
    finally:
        conn.close()

def save_cookies(username, cookies_list):
    conn = _get_connection()
    try:
        if not cookies_list:
            conn.execute("DELETE FROM cookies WHERE username = ?", (username,))
        else:
            conn.execute(
                "INSERT INTO cookies (username, cookie_json) VALUES (?, ?) ON CONFLICT(username) DO UPDATE SET cookie_json=excluded.cookie_json",
                (username, json.dumps(cookies_list))
            )
        conn.commit()
    finally:
        conn.close()


# ── Employee Activity Log ──
def log_activity(
    actor_username: str,
    actor_role: str,
    action: str,
    target_type: str = "",
    target_value: str = "",
    details=None,
):
    """Append an activity event to the audit log."""
    clean_actor = str(actor_username or "").strip().lower()
    clean_role = str(actor_role or "").strip().lower() or "employee"
    clean_action = str(action or "").strip()
    clean_target_type = str(target_type or "").strip()
    clean_target_value = str(target_value or "").strip()

    if not clean_actor or not clean_action:
        return

    payload = details if details is not None else {}
    try:
        details_json = json.dumps(payload)
    except Exception:
        details_json = json.dumps({"raw": str(payload)})

    conn = _get_connection()
    try:
        conn.execute(
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
                clean_actor,
                clean_role,
                clean_action,
                clean_target_type,
                clean_target_value,
                details_json,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_activity_logs(limit: int = 200, employees_only: bool = False):
    """Read latest activity logs, newest first."""
    safe_limit = max(1, min(int(limit or 200), 500))

    conn = _get_connection()
    try:
        if employees_only:
            rows = conn.execute(
                """
                SELECT id, actor_username, actor_role, action, target_type, target_value, details_json, created_at
                FROM activity_log
                WHERE actor_role = 'employee'
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, actor_username, actor_role, action, target_type, target_value, details_json, created_at
                FROM activity_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

        logs = []
        for row in rows:
            details_val = {}
            try:
                details_val = json.loads(row["details_json"])
            except Exception:
                details_val = {"raw": row["details_json"]}

            logs.append(
                {
                    "id": row["id"],
                    "actor_username": row["actor_username"],
                    "actor_role": row["actor_role"],
                    "action": row["action"],
                    "target_type": row["target_type"],
                    "target_value": row["target_value"],
                    "details": details_val,
                    "created_at": row["created_at"],
                }
            )
        return logs
    finally:
        conn.close()


def _parse_activity_datetime(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return None

    # Normalize trailing Z so fromisoformat can parse timezone-aware timestamps too.
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None

    if dt.tzinfo is not None:
        try:
            return dt.astimezone().replace(tzinfo=None)
        except Exception:
            return dt.replace(tzinfo=None)
    return dt


def get_activity_logs_recent_hours(hours: int = 24, limit: int = 500, employees_only: bool = False):
    """Read latest activity logs in the last N hours."""
    safe_limit = max(1, min(int(limit or 500), 1000))
    safe_hours = max(1, min(int(hours or 24), 168))
    scan_limit = min(max(safe_limit * 5, 1000), 5000)
    cutoff = datetime.now() - timedelta(hours=safe_hours)

    conn = _get_connection()
    try:
        if employees_only:
            rows = conn.execute(
                """
                SELECT id, actor_username, actor_role, action, target_type, target_value, details_json, created_at
                FROM activity_log
                WHERE actor_role = 'employee'
                ORDER BY id DESC
                LIMIT ?
                """,
                (scan_limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, actor_username, actor_role, action, target_type, target_value, details_json, created_at
                FROM activity_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (scan_limit,),
            ).fetchall()

        logs = []
        for row in rows:
            created_at_raw = row["created_at"]
            created_dt = _parse_activity_datetime(created_at_raw)
            if created_dt is None or created_dt < cutoff:
                continue

            details_val = {}
            try:
                details_val = json.loads(row["details_json"])
            except Exception:
                details_val = {"raw": row["details_json"]}

            logs.append(
                {
                    "id": row["id"],
                    "actor_username": row["actor_username"],
                    "actor_role": row["actor_role"],
                    "action": row["action"],
                    "target_type": row["target_type"],
                    "target_value": row["target_value"],
                    "details": details_val,
                    "created_at": created_at_raw,
                }
            )

            if len(logs) >= safe_limit:
                break

        return logs
    finally:
        conn.close()


# ── Inbox Messages CRUD ──
def save_inbox_threads(ig_account: str, threads: list):
    """Save scraped inbox threads for an IG account while preserving stable thread IDs."""
    clean_account = str(ig_account or "").strip().lower()
    if not clean_account:
        return

    base_scrape_dt = datetime.now().replace(microsecond=0)
    conn = _get_connection()
    try:
        existing_rows = conn.execute(
            "SELECT id, thread_display_name FROM inbox_messages WHERE ig_account = ? ORDER BY id ASC",
            (clean_account,),
        ).fetchall()

        existing_id_by_key = {}
        duplicate_existing_ids = []
        for row in existing_rows:
            display_key = str(row["thread_display_name"] or "").strip().lower()
            if not display_key:
                continue
            row_id = int(row["id"])
            if display_key in existing_id_by_key:
                duplicate_existing_ids.append(row_id)
            else:
                existing_id_by_key[display_key] = row_id

        incoming_keys = set()

        saved_position = 0
        for thread in (threads or []):
            if not isinstance(thread, dict):
                continue

            display_name = str(thread.get("display_name", "")).strip()
            if not display_name:
                continue

            display_key = display_name.lower()
            if display_key in incoming_keys:
                continue
            incoming_keys.add(display_key)

            # Keep the same top-to-bottom ordering as the scraped inbox list.
            # First (newest) thread gets the newest scraped_at value.
            row_scraped_at = (base_scrape_dt - timedelta(seconds=saved_position)).isoformat(timespec="seconds")
            saved_position += 1

            bind_values = (
                str(thread.get("username", "")).strip(),
                str(thread.get("message_preview", "")).strip(),
                str(thread.get("timestamp", "")).strip(),
                str(thread.get("timestamp_label", "")).strip(),
                str(thread.get("profile_pic_url", "")).strip(),
                1 if thread.get("is_unread", True) else 0,
                row_scraped_at,
            )

            existing_id = existing_id_by_key.get(display_key)
            if existing_id is not None:
                conn.execute(
                    """
                    UPDATE inbox_messages
                    SET thread_username = ?,
                        message_preview = ?,
                        timestamp_short = ?,
                        timestamp_label = ?,
                        profile_pic_url = ?,
                        is_unread = ?,
                        scraped_at = ?
                    WHERE id = ?
                    """,
                    bind_values + (existing_id,),
                )
                continue

            conn.execute(
                """
                INSERT INTO inbox_messages
                    (ig_account, thread_display_name, thread_username, message_preview,
                     timestamp_short, timestamp_label, profile_pic_url, is_unread, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    clean_account,
                    display_name,
                    bind_values[0],
                    bind_values[1],
                    bind_values[2],
                    bind_values[3],
                    bind_values[4],
                    bind_values[5],
                    bind_values[6],
                ),
            )

        # Keep older cached threads even if current scrape does not include them.
        # This prevents inbox chats from disappearing after partial/short scrape passes.
        stale_ids = list(duplicate_existing_ids)

        if stale_ids:
            placeholders = ",".join("?" for _ in stale_ids)
            conn.execute(
                f"DELETE FROM inbox_thread_lines WHERE thread_id IN ({placeholders})",
                tuple(stale_ids),
            )
            conn.execute(
                f"DELETE FROM inbox_messages WHERE id IN ({placeholders})",
                tuple(stale_ids),
            )

        conn.commit()
    finally:
        conn.close()


def get_inbox_threads(ig_account: str = None, unread_only: bool = False) -> list:
    """Get saved inbox threads, optionally filtered by IG account and unread status."""
    conn = _get_connection()
    try:
        if ig_account:
            clean_account = str(ig_account).strip().lower()
            if unread_only:
                rows = conn.execute(
                    "SELECT * FROM inbox_messages WHERE ig_account = ? AND is_unread = 1 ORDER BY scraped_at DESC, id ASC",
                    (clean_account,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM inbox_messages WHERE ig_account = ? ORDER BY scraped_at DESC, id ASC",
                    (clean_account,),
                ).fetchall()
        else:
            if unread_only:
                rows = conn.execute(
                    "SELECT * FROM inbox_messages WHERE is_unread = 1 ORDER BY scraped_at DESC, id ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM inbox_messages ORDER BY scraped_at DESC, id ASC"
                ).fetchall()

        threads = []
        for r in rows:
            threads.append({
                "id": r["id"],
                "ig_account": r["ig_account"],
                "display_name": r["thread_display_name"],
                "username": r["thread_username"],
                "message_preview": r["message_preview"],
                "timestamp": r["timestamp_short"],
                "timestamp_label": r["timestamp_label"],
                "profile_pic_url": r["profile_pic_url"],
                "is_unread": bool(r["is_unread"]),
                "scraped_at": r["scraped_at"],
            })
        return threads
    finally:
        conn.close()


def get_inbox_thread_by_id(thread_id: int):
    """Return one inbox thread row by numeric ID."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return None

    if thread_key <= 0:
        return None

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM inbox_messages WHERE id = ?",
            (thread_key,),
        ).fetchone()
        if not row:
            return None

        return {
            "id": row["id"],
            "ig_account": row["ig_account"],
            "display_name": row["thread_display_name"],
            "username": row["thread_username"],
            "message_preview": row["message_preview"],
            "timestamp": row["timestamp_short"],
            "timestamp_label": row["timestamp_label"],
            "profile_pic_url": row["profile_pic_url"],
            "is_unread": bool(row["is_unread"]),
            "scraped_at": row["scraped_at"],
        }
    finally:
        conn.close()


def delete_inbox_thread(thread_id: int) -> bool:
    """Delete one inbox thread and its cached conversation lines."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return False

    if thread_key <= 0:
        return False

    conn = _get_connection()
    try:
        conn.execute(
            "DELETE FROM inbox_thread_lines WHERE thread_id = ?",
            (thread_key,),
        )
        result = conn.execute(
            "DELETE FROM inbox_messages WHERE id = ?",
            (thread_key,),
        )
        conn.commit()
        return int(result.rowcount or 0) > 0
    finally:
        conn.close()


def set_inbox_thread_unread(thread_id: int, is_unread: bool) -> bool:
    """Update unread flag for a single inbox thread."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return False

    if thread_key <= 0:
        return False

    conn = _get_connection()
    try:
        result = conn.execute(
            "UPDATE inbox_messages SET is_unread = ? WHERE id = ?",
            (1 if bool(is_unread) else 0, thread_key),
        )
        conn.commit()
        return int(result.rowcount or 0) > 0
    finally:
        conn.close()


def update_inbox_thread_preview(
    thread_id: int,
    message_preview: str,
    timestamp_short: str = "",
    timestamp_label: str = "",
) -> bool:
    """Update preview text and optional timestamp values for one inbox thread."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return False

    if thread_key <= 0:
        return False

    conn = _get_connection()
    try:
        now_str = datetime.now().isoformat(timespec="seconds")
        result = conn.execute(
            """
            UPDATE inbox_messages
            SET message_preview = ?,
                timestamp_short = ?,
                timestamp_label = ?,
                scraped_at = ?
            WHERE id = ?
            """,
            (
                str(message_preview or "").strip(),
                str(timestamp_short or "").strip(),
                str(timestamp_label or "").strip(),
                now_str,
                thread_key,
            ),
        )
        conn.commit()
        return int(result.rowcount or 0) > 0
    finally:
        conn.close()


def save_thread_messages(thread_id: int, messages_list: list):
    """Save cached message lines for one inbox thread without losing existing history."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return

    if thread_key <= 0:
        return

    normalized_messages = []
    for msg in (messages_list or []):
        if not isinstance(msg, dict):
            continue

        text_content = str(msg.get("text") or msg.get("text_content") or "").strip()
        has_attachment = bool(msg.get("has_attachment", False))
        if not text_content and not has_attachment:
            continue

        normalized_messages.append(
            {
                "sender_name": str(msg.get("sender") or msg.get("sender_name") or "").strip(),
                "text_content": text_content,
                "has_attachment": has_attachment,
                "timestamp": str(msg.get("timestamp") or "").strip(),
                "is_self": bool(msg.get("is_self", False)),
            }
        )

    if not normalized_messages:
        return

    conn = _get_connection()
    try:
        existing_rows = conn.execute(
            """
            SELECT sender_name, text_content, has_attachment, timestamp, is_self
            FROM inbox_thread_lines
            WHERE thread_id = ?
            ORDER BY id ASC
            """,
            (thread_key,),
        ).fetchall()

        # If incoming payload is at least as complete as current cache,
        # replace cache to keep ordering identical to source conversation.
        if len(normalized_messages) >= len(existing_rows):
            conn.execute("DELETE FROM inbox_thread_lines WHERE thread_id = ?", (thread_key,))
            for msg in normalized_messages:
                conn.execute(
                    """
                    INSERT INTO inbox_thread_lines
                        (thread_id, sender_name, text_content, has_attachment, timestamp, is_self)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        thread_key,
                        msg["sender_name"],
                        msg["text_content"],
                        1 if msg["has_attachment"] else 0,
                        msg["timestamp"],
                        1 if msg["is_self"] else 0,
                    ),
                )
            conn.commit()
            return

        # Partial scrape: append only unseen lines so older history is retained.
        existing_keys = set()
        for row in existing_rows:
            existing_keys.add(
                (
                    str(row["sender_name"] or "").strip(),
                    str(row["text_content"] or "").strip(),
                    bool(row["has_attachment"]),
                    str(row["timestamp"] or "").strip(),
                    bool(row["is_self"]),
                )
            )

        for msg in normalized_messages:
            msg_key = (
                msg["sender_name"],
                msg["text_content"],
                bool(msg["has_attachment"]),
                msg["timestamp"],
                bool(msg["is_self"]),
            )
            if msg_key in existing_keys:
                continue

            conn.execute(
                """
                INSERT INTO inbox_thread_lines
                    (thread_id, sender_name, text_content, has_attachment, timestamp, is_self)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_key,
                    msg["sender_name"],
                    msg["text_content"],
                    1 if msg["has_attachment"] else 0,
                    msg["timestamp"],
                    1 if msg["is_self"] else 0,
                ),
            )
            existing_keys.add(msg_key)

        conn.commit()
    finally:
        conn.close()


def append_thread_message(
    thread_id: int,
    sender_name: str,
    text_content: str,
    has_attachment: bool = False,
    timestamp: str = "",
    is_self: bool = False,
) -> bool:
    """Append a single message line to one thread cache."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return False

    if thread_key <= 0:
        return False

    clean_text = str(text_content or "").strip()
    attachment_flag = bool(has_attachment)
    if not clean_text and not attachment_flag:
        return False

    conn = _get_connection()
    try:
        now_str = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT INTO inbox_thread_lines
                (thread_id, sender_name, text_content, has_attachment, timestamp, is_self)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                thread_key,
                str(sender_name or "").strip(),
                clean_text,
                1 if attachment_flag else 0,
                str(timestamp or "").strip(),
                1 if bool(is_self) else 0,
            ),
        )
        conn.execute(
            "UPDATE inbox_messages SET scraped_at = ? WHERE id = ?",
            (now_str, thread_key),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def get_thread_messages(thread_id: int) -> list:
    """Return cached thread message lines ordered from oldest to newest."""
    try:
        thread_key = int(thread_id)
    except Exception:
        return []

    if thread_key <= 0:
        return []

    conn = _get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, thread_id, sender_name, text_content, has_attachment, timestamp, is_self
            FROM inbox_thread_lines
            WHERE thread_id = ?
            ORDER BY id ASC
            """,
            (thread_key,),
        ).fetchall()

        messages = []
        for row in rows:
            messages.append(
                {
                    "id": row["id"],
                    "thread_id": row["thread_id"],
                    "sender_name": row["sender_name"],
                    "text_content": row["text_content"],
                    "has_attachment": bool(row["has_attachment"]),
                    "timestamp": row["timestamp"],
                    "is_self": bool(row["is_self"]),
                }
            )

        return messages
    finally:
        conn.close()


def get_inbox_unread_count(ig_account: str = None) -> int:
    """Count unread inbox messages, optionally filtered by account."""
    conn = _get_connection()
    try:
        if ig_account:
            clean_account = str(ig_account).strip().lower()
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM inbox_messages WHERE ig_account = ? AND is_unread = 1",
                (clean_account,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM inbox_messages WHERE is_unread = 1"
            ).fetchone()
        return int(row["c"] if row else 0)
    finally:
        conn.close()


def clear_inbox_threads(ig_account: str = None):
    """Clear inbox thread data, optionally for a specific account."""
    conn = _get_connection()
    try:
        if ig_account:
            clean_account = str(ig_account).strip().lower()
            rows = conn.execute(
                "SELECT id FROM inbox_messages WHERE ig_account = ?",
                (clean_account,),
            ).fetchall()
            thread_ids = [int(r["id"]) for r in rows]

            if thread_ids:
                placeholders = ",".join("?" for _ in thread_ids)
                conn.execute(
                    f"DELETE FROM inbox_thread_lines WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                )

            conn.execute("DELETE FROM inbox_messages WHERE ig_account = ?", (clean_account,))
        else:
            conn.execute("DELETE FROM inbox_thread_lines")
            conn.execute("DELETE FROM inbox_messages")
        conn.commit()
    finally:
        conn.close()


def _normalize_inbox_reply_job_status(raw_status: str, default: str = "queued") -> str:
    text = str(raw_status or "").strip().lower()
    if text in ("queued", "running", "done", "error"):
        return text

    fallback = str(default or "queued").strip().lower()
    if fallback in ("queued", "running", "done", "error"):
        return fallback
    return "queued"


def _row_get(row, key: str, default=None):
    if row is None:
        return default

    try:
        return row[key]
    except Exception:
        pass

    try:
        return row.get(key, default)
    except Exception:
        return default


def _row_to_inbox_reply_job_dict(row) -> dict:
    if not row:
        return None

    return {
        "job_id": str(_row_get(row, "job_id", "") or "").strip(),
        "status": _normalize_inbox_reply_job_status(_row_get(row, "status", "queued"), default="queued"),
        "progress": str(_row_get(row, "progress", "") or "").strip(),
        "error": str(_row_get(row, "error", "") or "").strip(),
        "ig_account": str(_row_get(row, "ig_account", "") or "").strip().lower(),
        "thread_id": int(_row_get(row, "thread_id", 0) or 0),
        "thread_display_name": str(_row_get(row, "thread_display_name", "") or "").strip(),
        "text_message": str(_row_get(row, "text_message", "") or "").strip(),
        "has_attachment": bool(_row_get(row, "has_attachment", 0)),
        "upload_path": str(_row_get(row, "upload_path", "") or "").strip(),
        "created_at": str(_row_get(row, "created_at", "") or "").strip(),
        "updated_at": str(_row_get(row, "updated_at", "") or "").strip(),
        "completed_at": str(_row_get(row, "completed_at", "") or "").strip(),
    }


def trim_inbox_reply_jobs(max_jobs: int = 300) -> int:
    """Trim persisted inbox reply jobs, preferring to remove done/error rows first."""
    try:
        safe_max = max(10, int(max_jobs or 300))
    except Exception:
        safe_max = 300

    conn = _get_connection()
    try:
        rows = conn.execute(
            "SELECT id, job_id, status FROM inbox_reply_queue ORDER BY updated_at ASC, id ASC"
        ).fetchall()

        total = len(rows)
        if total <= safe_max:
            return 0

        excess = total - safe_max
        delete_ids = []
        delete_seen = set()

        for row in rows:
            if len(delete_ids) >= excess:
                break
            status = _normalize_inbox_reply_job_status(_row_get(row, "status", "queued"), default="queued")
            if status in ("done", "error"):
                job_id = str(_row_get(row, "job_id", "") or "").strip()
                if not job_id or job_id in delete_seen:
                    continue
                delete_ids.append(job_id)
                delete_seen.add(job_id)

        if len(delete_ids) < excess:
            for row in rows:
                if len(delete_ids) >= excess:
                    break
                job_id = str(_row_get(row, "job_id", "") or "").strip()
                if not job_id or job_id in delete_seen:
                    continue
                delete_ids.append(job_id)
                delete_seen.add(job_id)

        if not delete_ids:
            return 0

        placeholders = ",".join("?" for _ in delete_ids)
        conn.execute(
            f"DELETE FROM inbox_reply_queue WHERE job_id IN ({placeholders})",
            tuple(delete_ids),
        )
        conn.commit()
        return len(delete_ids)
    finally:
        conn.close()


def create_inbox_reply_job(
    *,
    ig_account: str,
    thread_id: int,
    thread_display_name: str,
    text_message: str,
    has_attachment: bool,
    upload_path: str,
    job_id: str = "",
    max_jobs: int = 300,
) -> str:
    clean_account = str(ig_account or "").strip().lower()
    now_str = datetime.now().isoformat(timespec="seconds")
    try:
        clean_thread_id = int(thread_id or 0)
    except Exception:
        clean_thread_id = 0

    candidate_job_id = str(job_id or "").strip() or uuid4().hex
    insert_values = (
        candidate_job_id,
        "queued",
        "Queued for bot inbox cycle",
        "",
        clean_account,
        clean_thread_id,
        str(thread_display_name or "").strip(),
        str(text_message or "").strip(),
        1 if bool(has_attachment) else 0,
        str(upload_path or "").strip(),
        now_str,
        now_str,
        "",
    )

    conn = _get_connection()
    try:
        for _ in range(6):
            try:
                conn.execute(
                    """
                    INSERT INTO inbox_reply_queue (
                        job_id, status, progress, error, ig_account,
                        thread_id, thread_display_name, text_message,
                        has_attachment, upload_path, created_at, updated_at, completed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_values,
                )
                conn.commit()
                trim_inbox_reply_jobs(max_jobs=max_jobs)
                return candidate_job_id
            except INTEGRITY_ERRORS:
                try:
                    conn.rollback()
                except Exception:
                    pass
                candidate_job_id = uuid4().hex
                insert_values = (
                    candidate_job_id,
                    insert_values[1],
                    insert_values[2],
                    insert_values[3],
                    insert_values[4],
                    insert_values[5],
                    insert_values[6],
                    insert_values[7],
                    insert_values[8],
                    insert_values[9],
                    insert_values[10],
                    insert_values[11],
                    insert_values[12],
                )

        raise RuntimeError("Could not allocate a unique inbox reply job id")
    finally:
        conn.close()


def update_inbox_reply_job(job_id: str, **updates) -> bool:
    clean_job_id = str(job_id or "").strip()
    if not clean_job_id:
        return False

    status_in = updates.get("status", None)
    now_str = datetime.now().isoformat(timespec="seconds")

    set_fields = []
    params = []

    if "status" in updates:
        set_fields.append("status = ?")
        params.append(_normalize_inbox_reply_job_status(status_in, default="queued"))

    if "progress" in updates:
        set_fields.append("progress = ?")
        params.append(str(updates.get("progress", "") or ""))

    if "error" in updates:
        set_fields.append("error = ?")
        params.append(str(updates.get("error", "") or ""))

    if "thread_display_name" in updates:
        set_fields.append("thread_display_name = ?")
        params.append(str(updates.get("thread_display_name", "") or ""))

    if "text_message" in updates:
        set_fields.append("text_message = ?")
        params.append(str(updates.get("text_message", "") or ""))

    if "upload_path" in updates:
        set_fields.append("upload_path = ?")
        params.append(str(updates.get("upload_path", "") or ""))

    if "has_attachment" in updates:
        set_fields.append("has_attachment = ?")
        params.append(1 if bool(updates.get("has_attachment", False)) else 0)

    completed_override = None
    if "completed_at" in updates:
        completed_override = str(updates.get("completed_at", "") or "")
        set_fields.append("completed_at = ?")
        params.append(completed_override)
    else:
        normalized_status = _normalize_inbox_reply_job_status(status_in, default="") if status_in is not None else ""
        if normalized_status in ("done", "error"):
            set_fields.append("completed_at = ?")
            params.append(now_str)

    set_fields.append("updated_at = ?")
    params.append(now_str)

    if not set_fields:
        return False

    params.append(clean_job_id)

    conn = _get_connection()
    try:
        result = conn.execute(
            f"UPDATE inbox_reply_queue SET {', '.join(set_fields)} WHERE job_id = ?",
            tuple(params),
        )
        conn.commit()
        return int(result.rowcount or 0) > 0
    finally:
        conn.close()


def get_inbox_reply_job(job_id: str):
    clean_job_id = str(job_id or "").strip()
    if not clean_job_id:
        return None

    conn = _get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM inbox_reply_queue WHERE job_id = ?",
            (clean_job_id,),
        ).fetchone()
        return _row_to_inbox_reply_job_dict(row)
    finally:
        conn.close()


def has_queued_inbox_reply_for_account(ig_account: str) -> bool:
    clean_account = str(ig_account or "").strip().lower()
    if not clean_account:
        return False

    conn = _get_connection()
    try:
        row = conn.execute(
            """
            SELECT 1 AS hit
            FROM inbox_reply_queue
            WHERE ig_account = ? AND status = 'queued'
            LIMIT 1
            """,
            (clean_account,),
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def claim_next_inbox_reply_for_account(ig_account: str):
    clean_account = str(ig_account or "").strip().lower()
    if not clean_account:
        return None

    conn = _get_connection()
    try:
        for _ in range(8):
            row = conn.execute(
                """
                SELECT job_id
                FROM inbox_reply_queue
                WHERE ig_account = ? AND status = 'queued'
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_account,),
            ).fetchone()
            if not row:
                return None

            candidate_job_id = str(_row_get(row, "job_id", "") or "").strip()
            if not candidate_job_id:
                return None

            now_str = datetime.now().isoformat(timespec="seconds")
            result = conn.execute(
                """
                UPDATE inbox_reply_queue
                SET status = 'running',
                    progress = 'Picked by bot runtime',
                    error = '',
                    updated_at = ?
                WHERE job_id = ? AND status = 'queued'
                """,
                (now_str, candidate_job_id),
            )

            if int(result.rowcount or 0) <= 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

            claimed_row = conn.execute(
                "SELECT * FROM inbox_reply_queue WHERE job_id = ?",
                (candidate_job_id,),
            ).fetchone()
            conn.commit()
            return _row_to_inbox_reply_job_dict(claimed_row)

        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        conn.close()

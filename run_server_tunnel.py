#!/usr/bin/env python3
"""Cross-platform launcher for Flask server + Cloudflare tunnel.

This script is the single startup path for Windows and macOS/Linux wrappers.
It loads environment variables from a local .env file, runs preflight checks,
starts the Flask server, and keeps the Cloudflare tunnel alive.
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

REQUIRED_ENV_VARS = (
    "DATABASE_URL",
    "REDIS_URL",
    "FLASK_SECRET_KEY",
    "CLOUDFLARE_TUNNEL_TOKEN",
)

OPTIONAL_DEFAULTS = {
    "BOT_AUTO_START": "0",
    "CHROMEDRIVER_PATH": "",
    "LOCK_ALERT_BOT_TOKEN": "",
    "LOCK_ALERT_CHAT_IDS": "",
}


def _load_env_file(env_path: Path, override: bool = False) -> bool:
    if not env_path.exists():
        return False

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.lower().startswith("export "):
            line = line[7:].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value

    return True


def _require_env_values() -> None:
    missing = [key for key in REQUIRED_ENV_VARS if not str(os.environ.get(key, "")).strip()]
    if missing:
        missing_csv = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {missing_csv}. "
            "Set them in .env (preferred) or export them before running."
        )


def _run_preflight_checks() -> None:
    if not shutil.which("cloudflared"):
        raise RuntimeError(
            "cloudflared not found in PATH. Install cloudflared first."
        )

    chromedriver_path = str(os.environ.get("CHROMEDRIVER_PATH", "") or "").strip()
    if chromedriver_path and not Path(chromedriver_path).is_file():
        raise RuntimeError(f"CHROMEDRIVER_PATH does not exist: {chromedriver_path}")

    try:
        import redis
    except Exception as exc:
        raise RuntimeError(
            "Python package 'redis' is not installed. Install with: pip install redis"
        ) from exc

    redis_url = str(os.environ.get("REDIS_URL", "")).strip()
    print("\n[INFO] Checking Redis connectivity...")
    try:
        client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        client.ping()
        print("REDIS_PING_OK")
    except Exception as exc:
        raise RuntimeError(
            "Redis ping failed using REDIS_URL. Verify Redis host/password/firewall (port 6379)."
        ) from exc


def _sync_redis_settings_into_database() -> None:
    print("\n[INFO] Syncing Redis settings in database...")

    from config import database

    database.init_db()
    lock_token = str(os.environ.get("LOCK_ALERT_BOT_TOKEN", "") or "").strip()
    lock_chat_ids = [
        item.strip()
        for item in str(os.environ.get("LOCK_ALERT_CHAT_IDS", "") or "").split(",")
        if item.strip()
    ]

    payload = {
        "REDIS_COORDINATION_ENABLED": True,
        "REDIS_URL": str(os.environ.get("REDIS_URL", "") or "").strip(),
        "REDIS_FAIL_CLOSED": True,
        "LOCK_ALERT_BOT_TOKEN": lock_token,
    }
    if lock_chat_ids:
        payload["LOCK_ALERT_CHAT_IDS"] = lock_chat_ids

    database.save_settings(payload)
    print("REDIS_SETTINGS_SYNCED")


def _resolve_chromedriver_path_if_missing() -> None:
    existing = str(os.environ.get("CHROMEDRIVER_PATH", "") or "").strip()
    if existing:
        return

    print("\n[INFO] Resolving ChromeDriver path...")
    try:
        from core.browser import (
            _auto_install_chromedriver,
            _detect_chrome_version,
            _resolve_chromedriver_path,
        )

        chrome_version = _detect_chrome_version()
        resolved = _resolve_chromedriver_path() or _auto_install_chromedriver(chrome_version)
    except Exception:
        resolved = ""

    if resolved:
        os.environ["CHROMEDRIVER_PATH"] = str(resolved)
        print(f"[INFO] Auto-resolved CHROMEDRIVER_PATH: {resolved}")
    else:
        print("[WARN] ChromeDriver path not resolved in preflight.")
        print("[WARN] Bot will still try runtime bootstrap during browser startup.")


def _start_server_process() -> subprocess.Popen:
    print("\n[INFO] Starting Flask server...")
    env_copy = os.environ.copy()

    process = subprocess.Popen(
        [sys.executable, "server.py"],
        cwd=str(PROJECT_ROOT),
        env=env_copy,
    )

    time.sleep(3)
    if process.poll() is not None:
        raise RuntimeError(f"Flask server exited early with code {process.returncode}")

    return process


def _run_tunnel_loop(retry_delay_sec: int = 5) -> int:
    print("\n[INFO] Starting Cloudflare tunnel connector...")
    print("[INFO] Press Ctrl+C to stop tunnel on this VPS.\n")

    token = str(os.environ.get("CLOUDFLARE_TUNNEL_TOKEN", "") or "").strip()
    while True:
        completed = subprocess.run(
            ["cloudflared", "tunnel", "run", "--token", token],
            cwd=str(PROJECT_ROOT),
            env=os.environ.copy(),
        )
        print(
            f"[WARN] cloudflared exited with code {completed.returncode}. "
            f"Restarting in {retry_delay_sec} seconds..."
        )
        time.sleep(max(1, int(retry_delay_sec)))


def _stop_process(proc: subprocess.Popen) -> None:
    if not proc:
        return

    if proc.poll() is not None:
        return

    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-platform DM bot server+tunnel launcher")
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_PATH),
        help="Path to .env file (default: %(default)s)",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=5,
        help="Seconds to wait before restarting cloudflared",
    )
    parser.add_argument(
        "--no-tunnel",
        action="store_true",
        help="Start only Flask server (skip cloudflared tunnel loop)",
    )
    args = parser.parse_args()

    env_path = Path(args.env_file).expanduser().resolve()
    loaded = _load_env_file(env_path, override=False)

    for key, default_value in OPTIONAL_DEFAULTS.items():
        os.environ.setdefault(key, default_value)

    if loaded:
        print(f"[INFO] Loaded environment from: {env_path}")
    else:
        print(f"[WARN] .env file not found at: {env_path}")
        print("[WARN] Falling back to currently exported shell environment values.")

    try:
        _require_env_values()
        _run_preflight_checks()
        _sync_redis_settings_into_database()
        _resolve_chromedriver_path_if_missing()
    except Exception as exc:
        print(f"\n[ERROR] Preflight failed: {exc}")
        return 1

    server_proc = None

    def _handle_signal(signum, _frame):
        print(f"\n[INFO] Received signal {signum}. Shutting down...")
        _stop_process(server_proc)
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            pass

    try:
        server_proc = _start_server_process()
        if args.no_tunnel:
            print("[INFO] --no-tunnel enabled. Flask server is running.")
            server_proc.wait()
            return int(server_proc.returncode or 0)

        _run_tunnel_loop(retry_delay_sec=max(1, int(args.retry_delay)))
        return 0
    except KeyboardInterrupt:
        print("\n[INFO] Launcher interrupted by user.")
        return 0
    except Exception as exc:
        print(f"\n[ERROR] Runtime failure: {exc}")
        return 1
    finally:
        _stop_process(server_proc)


if __name__ == "__main__":
    sys.exit(main())

"""
Flask server — runs the DM bot continuously until manually stopped.
Provides a live dashboard to monitor status.
"""
import sys
import os
import json
import threading
import logging
import re
from functools import wraps
from datetime import datetime, timedelta
from uuid import uuid4

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template, request, redirect, session, url_for
from werkzeug.utils import secure_filename
from bot import run_bot, setup_logging, force_stop_active_sessions
from config import database
from config.database import get_setting
from core.inbox_reply_queue import (
  create_reply_job as create_inbox_reply_job_queue,
  get_reply_job as get_inbox_reply_job_queue,
  update_reply_job as update_inbox_reply_job_queue,
)
from telegram.bot import telegram_bot

# ── Config ──
BOT_LOOP_ENABLED = True
MAX_PROXIES_PER_ACCOUNT = 5
UPLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "uploads")
ALLOWED_REPLY_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
MAX_INBOX_REPLY_JOBS = 120
INBOX_PREFETCH_THREAD_LIMIT = 0
INBOX_PREFETCH_MESSAGES_LIMIT = 0
INBOX_SCRAPER_ENABLED_SETTING_KEY = "INBOX_SCRAPER_ENABLED"
INBOX_REPLIER_ENABLED_SETTING_KEY = "INBOX_REPLIER_ENABLED"
COMMENT_LIKING_ENABLED_SETTING_KEY = "COMMENT_LIKING_ENABLED"

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "beyinstabot-local-secret")
app.jinja_env.auto_reload = True
logger = logging.getLogger("model_dm_bot")
database.init_db()

# ── Shared State ──
bot_state = {
  "status": "idle",           # idle | running | stopping | stopped
    "current_session": 0,
    "total_sessions": 0,
    "last_run_start": None,
    "last_run_end": None,
    "next_run": None,
    "total_dms_all_time": 0,
    "dms_sent_today": 0,
    "last_dm_sent_at": "",
    "started_by": "",
    "started_by_role": "employee",
    "errors": [],
    "log_lines": [],
}
bot_thread = None
bot_thread_lock = threading.Lock()
stop_event = threading.Event()
engagement_state = {
  "status": "idle",          # idle | running | stopping | stopped
  "current_session": 0,
  "total_sessions": 0,
  "last_run_start": None,
  "last_run_end": None,
  "next_run": None,
  "started_by": "",
  "started_by_role": "employee",
  "errors": [],
}
engagement_thread = None
engagement_thread_lock = threading.Lock()
engagement_stop_event = threading.Event()
cluster_control_thread = None
cluster_control_thread_lock = threading.Lock()
cluster_control_stop_event = threading.Event()
cluster_control_last_nonce = ""
heavy_cl_state = {
  "status": "idle",
  "current_session": 0,
  "total_sessions": 0,
  "last_run_start": None,
  "last_run_end": None,
  "next_run": None,
  "started_by": "",
  "started_by_role": "employee",
  "errors": [],
}
heavy_cl_thread = None
heavy_cl_thread_lock = threading.Lock()
heavy_cl_stop_event = threading.Event()

engagement_state = {
  "status": "idle",
  "current_session": 0,
  "total_sessions": 0,
  "last_run_start": None,
  "last_run_end": None,
  "next_run": None,
  "started_by": "",
  "started_by_role": "employee",
  "errors": [],
}
engagement_thread = None
engagement_thread_lock = threading.Lock()
engagement_stop_event = threading.Event()
heavy_cl_stop_event = threading.Event()

CLUSTER_CONTROL_SETTING_KEY = "BOT_CLUSTER_CONTROL"


def _env_is_true(name: str, default: bool = False) -> bool:
  raw = os.environ.get(name)
  if raw is None:
    return bool(default)

  text = str(raw).strip().lower()
  if text in ("1", "true", "yes", "on", "enable", "enabled"):
    return True
  if text in ("0", "false", "no", "off", "disable", "disabled"):
    return False

  return bool(default)


def _normalize_role(raw_role: str, default: str = "employee") -> str:
  role = str(raw_role or "").strip().lower()
  if role in ("master", "employee"):
    return role

  fallback = str(default or "employee").strip().lower()
  if fallback in ("master", "employee"):
    return fallback
  return "employee"


def _normalize_cluster_state(raw_state: str) -> str:
  state = str(raw_state or "").strip().lower()
  if state in ("start", "run", "running", "resume", "on", "1", "true"):
    return "running"
  if state in ("stop", "stopped", "idle", "off", "0", "false", "pause"):
    return "stopped"
  return ""


def _build_cluster_control_payload(desired_state: str, issued_by: str = "", issued_by_role: str = "employee") -> dict:
  normalized_state = _normalize_cluster_state(desired_state)
  if not normalized_state:
    raise ValueError(f"Invalid cluster control state: {desired_state}")

  return {
    "desired_state": normalized_state,
    "issued_by": str(issued_by or "").strip().lower(),
    "issued_by_role": _normalize_role(issued_by_role, default="employee"),
    "issued_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    "nonce": uuid4().hex,
  }


def _get_cluster_control_payload():
  raw_payload = get_setting(CLUSTER_CONTROL_SETTING_KEY, None)
  if not isinstance(raw_payload, dict):
    return None

  desired_state = _normalize_cluster_state(raw_payload.get("desired_state", ""))
  if not desired_state:
    return None

  issued_by = str(raw_payload.get("issued_by", "") or "").strip().lower()
  issued_by_role = _normalize_role(raw_payload.get("issued_by_role", "employee"), default="employee")
  issued_at = str(raw_payload.get("issued_at", "") or "").strip()
  nonce = str(raw_payload.get("nonce", "") or "").strip() or f"legacy-{desired_state}"

  return {
    "desired_state": desired_state,
    "issued_by": issued_by,
    "issued_by_role": issued_by_role,
    "issued_at": issued_at,
    "nonce": nonce,
  }


def _publish_cluster_control(desired_state: str, issued_by: str = "", issued_by_role: str = "employee") -> dict:
  payload = _build_cluster_control_payload(
    desired_state=desired_state,
    issued_by=issued_by,
    issued_by_role=issued_by_role,
  )
  database.save_settings({CLUSTER_CONTROL_SETTING_KEY: payload})
  return payload


def _is_bot_running() -> bool:
  with bot_thread_lock:
    return bool(bot_thread and bot_thread.is_alive())


def _is_engagement_bot_running() -> bool:
  with engagement_thread_lock:
    return bool(engagement_thread and engagement_thread.is_alive())


def _request_local_stop() -> bool:
  """Request this node's bot loop to stop; returns True when thread was alive."""
  stop_event.set()
  force_stop_active_sessions()

  with bot_thread_lock:
    was_running = bool(bot_thread and bot_thread.is_alive())
    if was_running:
      bot_state["status"] = "stopping"
    else:
      bot_state["status"] = "stopped"
      bot_state["started_by"] = ""
      bot_state["started_by_role"] = "employee"

  return was_running


def _request_engagement_stop() -> bool:
  """Request this node's engagement loop to stop; returns True when thread was alive."""
  engagement_stop_event.set()
  force_stop_active_sessions()

  with engagement_thread_lock:
    was_running = bool(engagement_thread and engagement_thread.is_alive())
    if was_running:
      engagement_state["status"] = "stopping"
    else:
      engagement_state["status"] = "stopped"
      engagement_state["started_by"] = ""
      engagement_state["started_by_role"] = "employee"

  return was_running


def _is_heavy_cl_running() -> bool:
  with heavy_cl_thread_lock:
    return bool(heavy_cl_thread and heavy_cl_thread.is_alive())

def _is_engagement_bot_running() -> bool:
  with engagement_thread_lock:
    return bool(engagement_thread and engagement_thread.is_alive())


def _request_heavy_cl_stop() -> bool:
  """Request this node's heavy comment-liking loop to stop."""
  heavy_cl_stop_event.set()
  force_stop_active_sessions()

  with heavy_cl_thread_lock:
    was_running = bool(heavy_cl_thread and heavy_cl_thread.is_alive())
    if was_running:
      heavy_cl_state["status"] = "stopping"
    else:
      heavy_cl_state["status"] = "stopped"
      heavy_cl_state["started_by"] = ""
      heavy_cl_state["started_by_role"] = "employee"

  return was_running


def _start_heavy_cl_loop(started_by: str = "", started_by_role: str = "employee") -> bool:
  """Start heavy comment-liking loop thread once; return False when already running."""
  global heavy_cl_thread

  with heavy_cl_thread_lock:
    if heavy_cl_thread and heavy_cl_thread.is_alive():
      return False

    heavy_cl_stop_event.clear()
    heavy_cl_state["started_by"] = str(started_by or "").strip().lower()
    heavy_cl_state["started_by_role"] = _normalize_role(started_by_role, default="employee")
    heavy_cl_state["status"] = "running"

    heavy_cl_thread = threading.Thread(
      target=heavy_comment_liking_loop,
      name="heavy-comment-liking-loop",
      daemon=True,
    )
    heavy_cl_thread.start()
    return True


def _cluster_control_poll_seconds() -> float:
  raw_value = os.environ.get("BOT_CLUSTER_CONTROL_POLL_SEC", "2")
  try:
    poll_seconds = float(raw_value)
  except Exception:
    poll_seconds = 2.0

  return max(0.5, min(30.0, poll_seconds))


def _cluster_control_loop():
  global cluster_control_last_nonce

  poll_seconds = _cluster_control_poll_seconds()
  logger.info(f"Cluster control watcher started (poll every {poll_seconds:.1f}s).")

  while not cluster_control_stop_event.is_set():
    try:
      control = _get_cluster_control_payload()
      if control:
        desired_state = control["desired_state"]
        nonce = control["nonce"]

        if nonce != cluster_control_last_nonce:
          cluster_control_last_nonce = nonce
          issued_by = control.get("issued_by", "")
          issued_by_role = control.get("issued_by_role", "employee")
          logger.info(
            f"Received cluster command '{desired_state}' from @{issued_by or 'unknown'} ({issued_by_role})."
          )

        if desired_state == "running":
          if not _is_bot_running():
            started = _start_bot_loop(
              started_by=control.get("issued_by", ""),
              started_by_role=control.get("issued_by_role", "employee"),
            )
            if started:
              logger.info("Applied cluster start command on this node.")
        elif desired_state == "stopped":
          if _is_bot_running() or bot_state.get("status") in ("running", "stopping"):
            _request_local_stop()
      
    except Exception as e:
      logger.debug(f"Cluster control watcher error: {e}")

    cluster_control_stop_event.wait(poll_seconds)


def _ensure_cluster_control_watcher():
  global cluster_control_thread

  with cluster_control_thread_lock:
    if cluster_control_thread and cluster_control_thread.is_alive():
      return

    cluster_control_stop_event.clear()
    cluster_control_thread = threading.Thread(
      target=_cluster_control_loop,
      name="cluster-control-watcher",
      daemon=True,
    )
    cluster_control_thread.start()


def _start_bot_loop(started_by: str = "", started_by_role: str = "employee") -> bool:
  """Start bot loop thread once; return False when already running."""
  global bot_thread

  with bot_thread_lock:
    if bot_thread and bot_thread.is_alive():
      return False

    stop_event.clear()
    bot_state["started_by"] = str(started_by or "").strip().lower()

    clean_role = _normalize_role(started_by_role, default="employee")
    bot_state["started_by_role"] = clean_role

    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    bot_state["status"] = "running"
    return True


def _start_engagement_loop(started_by: str = "", started_by_role: str = "employee") -> bool:
  """Start engagement loop thread once; return False when already running."""
  global engagement_thread

  with engagement_thread_lock:
    if engagement_thread and engagement_thread.is_alive():
      return False

    engagement_stop_event.clear()
    engagement_state["started_by"] = str(started_by or "").strip().lower()
    engagement_state["started_by_role"] = _normalize_role(started_by_role, default="employee")
    engagement_state["status"] = "running"

    engagement_thread = threading.Thread(
      target=engagement_loop,
      name="engagement-loop",
      daemon=True,
    )
    engagement_thread.start()
    return True


# ── Authentication ──
# (Auth DB is now handled natively by config.database)


def login_required(route_func):
  @wraps(route_func)
  def wrapper(*args, **kwargs):
    if session.get("authenticated"):
      return route_func(*args, **kwargs)

    if request.path.startswith("/api/"):
      return jsonify({"success": False, "error": "Unauthorized"}), 401

    return redirect(url_for("login"))

  return wrapper


def _current_user_context():
  return {
    "username": session.get("username", ""),
    "role": session.get("role", "employee"),
  }


def _is_master():
  return session.get("role") == "master"


def master_required(route_func):
  @wraps(route_func)
  def wrapper(*args, **kwargs):
    if _is_master():
      return route_func(*args, **kwargs)

    if request.path.startswith("/api/"):
      return jsonify({"success": False, "error": "Master access required"}), 403

    return redirect(url_for("dashboard"))

  return wrapper


def _log_actor_action(action, target_type="", target_value="", details=None, employees_only=True):
  """Append an actor action to audit log. By default only logs employee actions."""
  user_ctx = _current_user_context()
  actor_username = user_ctx.get("username", "")
  actor_role = user_ctx.get("role", "employee")

  if not actor_username:
    return
  if employees_only and actor_role != "employee":
    return

  try:
    database.log_activity(
      actor_username,
      actor_role,
      action,
      target_type=target_type,
      target_value=target_value,
      details=details,
    )
  except Exception as e:
    logger.debug(f"Activity log failed for {actor_username}: {e}")


def _setting_int(key: str) -> int:
  value = get_setting(key)
  if value is None:
    raise KeyError(f"Missing required setting in database: {key}")
  try:
    return int(value)
  except (TypeError, ValueError):
    raise ValueError(f"Invalid integer setting '{key}': {value}")


def _refresh_total_dms_all_time():
  """Refresh dashboard DM metrics from DB for live dashboard/status reporting."""
  try:
    metrics = database.get_dm_dashboard_metrics()
    bot_state["total_dms_all_time"] = int(metrics.get("lifetime_total_sent", 0) or 0)
    bot_state["dms_sent_today"] = int(metrics.get("dms_sent_today", 0) or 0)
    bot_state["last_dm_sent_at"] = str(metrics.get("last_dm_sent_at", "") or "")
  except Exception as e:
    logger.debug(f"Failed to refresh DM dashboard metrics: {e}")


def _ensure_telegram_polling():
  """Keep Telegram command polling available even when automation is idle."""
  try:
    telegram_bot.start_polling()
  except Exception as e:
    logger.debug(f"Failed to ensure Telegram polling: {e}")


_refresh_total_dms_all_time()
_ensure_telegram_polling()


def _normalize_text_list(raw_items):
  if not isinstance(raw_items, list):
    return []

  clean = []
  for item in raw_items:
    if not isinstance(item, str):
      continue
    text = item.strip()
    if text:
      clean.append(text)
  return clean


def _normalize_bool_flag(raw_value, default: bool = True) -> bool:
  if isinstance(raw_value, bool):
    return raw_value
  if raw_value is None:
    return bool(default)
  if isinstance(raw_value, (int, float)):
    return int(raw_value) != 0

  text = str(raw_value).strip().lower()
  if text in ("", "none", "null"):
    return bool(default)
  if text in ("1", "true", "on", "yes", "enable", "enabled"):
    return True
  if text in ("0", "false", "off", "no", "disable", "disabled"):
    return False
  return bool(default)


def _split_proxy_entries(raw_proxy):
  text = str(raw_proxy or "")
  if not text.strip():
    return []

  clean = []
  seen = set()
  for part in re.split(r"[\r\n,;]+", text):
    proxy = str(part or "").strip()
    if not proxy:
      continue

    key = proxy.lower()
    if key in seen:
      continue
    seen.add(key)
    clean.append(proxy)

  return clean


def _normalize_proxy_value(raw_proxy, max_items: int = MAX_PROXIES_PER_ACCOUNT):
  proxy_entries = _split_proxy_entries(raw_proxy)
  too_many = len(proxy_entries) > max_items
  limited = proxy_entries[:max_items]
  return ", ".join(limited), too_many, len(proxy_entries)


def _mask_single_proxy_for_view(proxy_value: str):
  clean = str(proxy_value or "").strip()
  if not clean:
    return ""

  if "@" not in clean:
    return clean

  if "://" in clean:
    scheme, rest = clean.split("://", 1)
    prefix = f"{scheme}://"
  else:
    rest = clean
    prefix = ""

  creds, host = rest.rsplit("@", 1)
  if ":" in creds:
    username = creds.split(":", 1)[0]
    safe_creds = f"{username}:***"
  else:
    safe_creds = "***"

  return f"{prefix}{safe_creds}@{host}"


def _mask_proxy_for_view(proxy_value):
  """Mask proxy credentials for read-only queue display."""
  proxy_entries = _split_proxy_entries(proxy_value)
  if not proxy_entries:
    return ""

  masked = [_mask_single_proxy_for_view(proxy) for proxy in proxy_entries[:MAX_PROXIES_PER_ACCOUNT]]
  hidden_count = max(0, len(proxy_entries) - MAX_PROXIES_PER_ACCOUNT)
  if hidden_count:
    masked.append(f"+{hidden_count} more")
  return ", ".join(masked)


# ── Dashboard HTML ──
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Instagram DM Bot — Dashboard</title>
<meta http-equiv="refresh" content="10">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: #0a0a0f;
    color: #e0e0e0;
    min-height: 100vh;
    padding: 2rem;
  }
  .container { max-width: 900px; margin: 0 auto; }
  h1 {
    text-align: center;
    font-size: 2rem;
    background: linear-gradient(135deg, #f09433, #e6683c, #dc2743, #cc2366, #bc1888);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 2rem;
  }
  .status-bar {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 1rem;
    margin-bottom: 2rem;
  }
  .status-badge {
    padding: 0.5rem 1.5rem;
    border-radius: 50px;
    font-weight: 700;
    font-size: 0.9rem;
    text-transform: uppercase;
    letter-spacing: 1px;
  }
  .status-idle { background: #1a1a2e; color: #888; border: 1px solid #333; }
  .status-running { background: #0d3320; color: #4ade80; border: 1px solid #166534; animation: pulse 2s infinite; }
  .status-cooldown { background: #1e1b3a; color: #a78bfa; border: 1px solid #4c1d95; }
  .status-stopped { background: #2d1215; color: #f87171; border: 1px solid #7f1d1d; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.7; } }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
  }
  .card {
    background: #12121a;
    border: 1px solid #1e1e2e;
    border-radius: 12px;
    padding: 1.5rem;
    text-align: center;
  }
  .card .value {
    font-size: 2rem;
    font-weight: 800;
    color: #fff;
    margin-bottom: 0.3rem;
  }
  .card .label { font-size: 0.8rem; color: #888; text-transform: uppercase; letter-spacing: 1px; }
  .logs {
    background: #0d0d14;
    border: 1px solid #1e1e2e;
    border-radius: 12px;
    padding: 1.5rem;
    max-height: 400px;
    overflow-y: auto;
    font-family: 'Cascadia Code', 'Fira Code', monospace;
    font-size: 0.78rem;
    line-height: 1.6;
  }
  .logs .line { color: #6b7280; }
  .logs .line.info { color: #9ca3af; }
  .logs .line.success { color: #4ade80; }
  .logs .line.error { color: #f87171; }
  .logs .line.warn { color: #fbbf24; }
  .time-info {
    text-align: center;
    color: #555;
    font-size: 0.85rem;
    margin-bottom: 1.5rem;
  }
  .controls {
    display: flex;
    justify-content: center;
    gap: 1rem;
    margin-bottom: 2rem;
  }
  .btn {
    padding: 0.6rem 2rem;
    border: none;
    border-radius: 8px;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.9rem;
    transition: all 0.2s;
  }
  .btn-stop { background: #7f1d1d; color: #fca5a5; }
  .btn-stop:hover { background: #991b1b; }
  .btn-start { background: #14532d; color: #86efac; }
  .btn-start:hover { background: #166534; }
</style>
</head>
<body>
<div class="container">
  <h1>🤖 Instagram DM Bot</h1>

  <div class="status-bar">
    <span class="status-badge status-{{ state.status }}">{{ state.status }}</span>
  </div>

  <div class="time-info">
    {% if state.next_run and state.status == 'cooldown' %}
      ⏳ Next run in: <strong>{{ state.next_run }}</strong>
    {% elif state.last_run_end %}
      Last completed: {{ state.last_run_end }}
    {% else %}
      Waiting to start...
    {% endif %}
  </div>

  <div class="controls">
    {% if state.status == 'stopped' %}
      <a href="/start"><button class="btn btn-start">▶ Start Loop</button></a>
    {% else %}
      <a href="/stop"><button class="btn btn-stop">■ Stop</button></a>
    {% endif %}
  </div>

  <div class="grid">
    <div class="card">
      <div class="value">{{ state.total_sessions }}</div>
      <div class="label">Sessions Run</div>
    </div>
    <div class="card">
      <div class="value">{{ state.total_dms_all_time }}</div>
      <div class="label">Total DMs Sent</div>
    </div>
    <div class="card">
      <div class="value">{{ cooldown_range }}</div>
      <div class="label">Cooldown</div>
    </div>
    <div class="card">
      <div class="value">{{ dm_log_count }}</div>
      <div class="label">Users Reached</div>
    </div>
  </div>

  <h2 style="margin-bottom:1rem;font-size:1rem;color:#888;">📜 Recent Logs</h2>
  <div class="logs">
    {% for line in state.log_lines[-50:]|reverse %}
      <div class="line {% if '✅' in line or 'successful' in line %}success{% elif '❌' in line or 'ERROR' in line %}error{% elif '⚠️' in line or 'WARNING' in line %}warn{% else %}info{% endif %}">{{ line }}</div>
    {% endfor %}
    {% if not state.log_lines %}
      <div class="line">No logs yet. Start the bot to see activity.</div>
    {% endif %}
  </div>
</div>
</body>
</html>
"""


# ── Log Capture Handler ──
class DashboardLogHandler(logging.Handler):
    """Captures log lines into bot_state for the dashboard."""
    def emit(self, record):
        msg = self.format(record)
        bot_state["log_lines"].append(msg)
        # Keep only last 200 lines
        if len(bot_state["log_lines"]) > 200:
            bot_state["log_lines"] = bot_state["log_lines"][-200:]


def _append_dashboard_log(message: str, level: str = "INFO"):
  """Append a formatted log line directly to dashboard state."""
  clean_message = str(message or "").strip()
  if not clean_message:
    return

  clean_level = str(level or "INFO").strip().upper() or "INFO"
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  formatted = f"{timestamp} | {clean_level:<7} | {clean_message}"
  bot_state["log_lines"].append(formatted)

  if len(bot_state["log_lines"]) > 200:
    bot_state["log_lines"] = bot_state["log_lines"][-200:]


def _ensure_dashboard_log_handler():
    """Attach a single dashboard log handler instance to avoid duplicate log lines."""
    model_logger = logging.getLogger("model_dm_bot")
    for handler in model_logger.handlers:
        if isinstance(handler, DashboardLogHandler):
            return

    dash_handler = DashboardLogHandler()
    dash_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    model_logger.addHandler(dash_handler)


# ── Bot Loop ──
def bot_loop():
    """Run the bot continuously with immediate pass restarts until stopped."""
    global bot_state

    setup_logging()
    _ensure_dashboard_log_handler()

    pass_num = 0

    while not stop_event.is_set():
        pass_num += 1
        bot_state["status"] = "running"
        bot_state["current_session"] = pass_num
        bot_state["last_run_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bot_state["next_run"] = None

        logger.info(f"♻️ CONTINUOUS PASS #{pass_num} STARTING")

        started_by = bot_state.get("started_by", "")
        started_by_role = bot_state.get("started_by_role", "employee")
        account_owner = None if started_by_role == "master" else started_by
        if started_by:
          logger.info(
            f"Run operator: @{started_by} ({started_by_role})"
          )

        try:
          run_bot(stop_event=stop_event, account_owner=account_owner, continuous_mode=True)
        except Exception as e:
            logger.error(f"Continuous pass #{pass_num} crashed: {e}")
            bot_state["errors"].append(f"Pass {pass_num}: {e}")

        bot_state["total_sessions"] = pass_num
        bot_state["last_run_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        _refresh_total_dms_all_time()

        if stop_event.is_set():
            break

        # Immediate restart in 24/7 mode, with a tiny guard interval to avoid
        # tight spin loops if a pass exits instantly due to config/runtime issues.
        if stop_event.wait(1.0):
            break

    bot_state["status"] = "stopped"
    bot_state["next_run"] = None
    bot_state["started_by"] = ""
    bot_state["started_by_role"] = "employee"
    logger.info("🛑 Bot runner stopped.")


def engagement_loop():
    """Run combined engagement scraping continuously until stopped."""
    global engagement_state

    setup_logging()
    _ensure_dashboard_log_handler()

    pass_num = 0

    while not engagement_stop_event.is_set():
        pass_num += 1
        engagement_state["status"] = "running"
        engagement_state["current_session"] = pass_num
        engagement_state["last_run_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        engagement_state["next_run"] = None

        logger.info(f"♻️ LIKE-COMMENT PASS #{pass_num} STARTING")

        started_by = engagement_state.get("started_by", "")
        started_by_role = engagement_state.get("started_by_role", "employee")
        account_owner = None if started_by_role == "master" else started_by

        try:
          run_bot(
            stop_event=engagement_stop_event,
            account_owner=account_owner,
            continuous_mode=True,
            runtime_mode="target_engagement", # This is now the randomized combined mode
          )
        except Exception as e:
            logger.error(f"Engagement pass #{pass_num} crashed: {e}")
            engagement_state["errors"].append(f"Pass {pass_num}: {e}")

        engagement_state["total_sessions"] = pass_num
        engagement_state["last_run_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if engagement_stop_event.is_set():
            break

        if engagement_stop_event.wait(1.0):
            break

    engagement_state["status"] = "stopped"
    engagement_state["next_run"] = None
    engagement_state["started_by"] = ""
    engagement_state["started_by_role"] = "employee"

    try:
      _set_comment_liking_enabled(False)
    except Exception as e:
      logger.debug(f"Failed to disable comment-liking setting after stop: {e}")

    logger.info("🛑 Comment-liking runner stopped.")


def heavy_comment_liking_loop():
    """Run heavy comment-liking continuously until stopped."""
    global heavy_cl_state

    setup_logging()
    _ensure_dashboard_log_handler()

    pass_num = 0

    while not heavy_cl_stop_event.is_set():
        pass_num += 1
        heavy_cl_state["status"] = "running"
        heavy_cl_state["current_session"] = pass_num
        heavy_cl_state["last_run_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        heavy_cl_state["next_run"] = None

        logger.info(f"♻️ HEAVY COMMENT-LIKING PASS #{pass_num} STARTING")

        started_by = heavy_cl_state.get("started_by", "")
        started_by_role = heavy_cl_state.get("started_by_role", "employee")
        account_owner = None if started_by_role == "master" else started_by
        if started_by:
          logger.info(
            f"Heavy comment-liking operator: @{started_by} ({started_by_role})"
          )

        try:
          run_bot(
            stop_event=heavy_cl_stop_event,
            account_owner=account_owner,
            continuous_mode=True,
            runtime_mode="heavy_comment_liking",
          )
        except Exception as e:
            logger.error(f"Heavy comment-liking pass #{pass_num} crashed: {e}")
            heavy_cl_state["errors"].append(f"Pass {pass_num}: {e}")

        heavy_cl_state["total_sessions"] = pass_num
        heavy_cl_state["last_run_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if heavy_cl_stop_event.is_set():
            break

        if heavy_cl_stop_event.wait(1.0):
            break

    heavy_cl_state["status"] = "stopped"
    heavy_cl_state["next_run"] = None
    heavy_cl_state["started_by"] = ""
    heavy_cl_state["started_by_role"] = "employee"
    logger.info("🛑 Heavy comment-liking runner stopped.")


# ── Flask Routes ──
@app.route("/login", methods=["GET", "POST"])
def login():
  if session.get("authenticated"):
    return redirect(url_for("dashboard"))

  error = None
  if request.method == "POST":
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")

    auth_user = database.authenticate_user(username, password)
    if auth_user:
      session.clear()
      session.permanent = True
      session["authenticated"] = True
      session["username"] = auth_user["username"]
      session["role"] = auth_user["role"]

      _log_actor_action(
        "login",
        target_type="auth",
        target_value="dashboard",
        details={"ip": request.remote_addr or ""},
        employees_only=True,
      )
      return redirect(url_for("dashboard"))

    error = "Invalid username or password"

  return render_template("login.html", error=error)


@app.route("/logout")
def logout():
  session.clear()
  return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    dm_log = database.get_dm_logs()
    dm_log_count = len(dm_log)
    user_ctx = _current_user_context()

    return render_template(
        "index.html",
        state=bot_state,
        dm_log_count=dm_log_count,
        current_user=user_ctx,
    )

# ── API ──
@app.route("/api/config", methods=["GET"])
@login_required
def api_get_config():
    """Retrieve all configuration chunks to populate the UI."""
    user_ctx = _current_user_context()
    data = {
        "accounts": [],
        "accounts_queue": [],
        "models": [],
        "messages": [],
        "comments": [],
        "settings": {},
        "users": [],
        "current_user": user_ctx,
    }
    try:
        if user_ctx["role"] == "master":
            data["accounts"] = database.get_accounts(include_all=True)
            data["users"] = database.get_users()
            queue_rows = database.get_accounts(include_all=True)
            data["accounts_queue"] = [
                {
                    "username": str(acc.get("username", "")).strip(),
                    "owner_username": str(acc.get("owner_username", "")).strip() or "master",
                    "model_label": str(acc.get("model_label", "")).strip(),
                    "proxy": _mask_proxy_for_view(acc.get("proxy", "")),
              "profile_note": str(acc.get("profile_note", "")).strip(),
              "automation_enabled": _normalize_bool_flag(acc.get("automation_enabled", True), default=True),
              "is_suspended": _normalize_bool_flag(acc.get("is_suspended", False), default=False),
                }
                for acc in queue_rows
                if str(acc.get("username", "")).strip()
            ]
        else:
            data["accounts"] = database.get_accounts(owner_username=user_ctx["username"])
            queue_rows = database.get_accounts(include_all=True)
            data["accounts_queue"] = [
                {
                    "username": str(acc.get("username", "")).strip(),
                    "owner_username": str(acc.get("owner_username", "")).strip() or "master",
                    "model_label": str(acc.get("model_label", "")).strip(),
                    "proxy": _mask_proxy_for_view(acc.get("proxy", "")),
              "profile_note": str(acc.get("profile_note", "")).strip(),
              "automation_enabled": _normalize_bool_flag(acc.get("automation_enabled", True), default=True),
              "is_suspended": _normalize_bool_flag(acc.get("is_suspended", False), default=False),
                }
                for acc in queue_rows
                if str(acc.get("username", "")).strip()
            ]

        data["models"] = database.get_models()
        data["messages"] = database.get_messages()
        data["comments"] = database.get_comments()
        data["settings"] = database.get_all_settings()
    except Exception as e:
        logger.error(f"Error reading config API: {e}")
    return jsonify(data)


@app.route("/api/accounts/queue", methods=["GET"])
@login_required
@master_required
def api_accounts_queue():
    """Return sanitized cross-employee IG account queue."""
    try:
        queue_rows = database.get_accounts(include_all=True)
        accounts = [
            {
                "username": str(acc.get("username", "")).strip(),
                "owner_username": str(acc.get("owner_username", "")).strip() or "master",
            "model_label": str(acc.get("model_label", "")).strip(),
            "proxy": _mask_proxy_for_view(acc.get("proxy", "")),
            "profile_note": str(acc.get("profile_note", "")).strip(),
          "automation_enabled": _normalize_bool_flag(acc.get("automation_enabled", True), default=True),
          "is_suspended": _normalize_bool_flag(acc.get("is_suspended", False), default=False),
            }
            for acc in queue_rows
            if str(acc.get("username", "")).strip()
        ]
        return jsonify({"success": True, "accounts": accounts})
    except Exception as e:
        logger.error(f"Error reading accounts queue: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/accounts/proxy", methods=["POST"])
@login_required
def api_update_account_proxy():
  """Allow proxy updates only for accessible accounts (owner or master)."""
  try:
    payload = request.get_json(silent=True) or {}
    updates = payload.get("updates", [])
    if not isinstance(updates, list):
      return jsonify({"success": False, "error": "updates must be a list"}), 400

    user_ctx = _current_user_context()
    changed_usernames = []
    for idx, item in enumerate(updates):
      if not isinstance(item, dict):
        return jsonify({"success": False, "error": f"Invalid update entry at index {idx}"}), 400

      username = str(item.get("username", "")).strip()
      if not username:
        continue

      if not database.user_can_access_account(username, user_ctx["username"], user_ctx["role"]):
        return jsonify({"success": False, "error": f"Forbidden for account '{username}'"}), 403

      proxy_value, too_many_proxies, _ = _normalize_proxy_value(item.get("proxy", ""))
      if too_many_proxies:
        return jsonify({
          "success": False,
          "error": f"Account '{username}' supports maximum {MAX_PROXIES_PER_ACCOUNT} proxies",
        }), 400

      updated = database.update_account_proxy(username, proxy_value)
      if updated:
        changed_usernames.append(username)

    if changed_usernames:
      _log_actor_action(
        "update_account_proxy",
        target_type="config",
        target_value="accounts.proxy",
        details={
          "updated_count": len(changed_usernames),
          "usernames": ", ".join(changed_usernames[:20]) + (" ..." if len(changed_usernames) > 20 else ""),
          "actor_role": user_ctx.get("role", "employee"),
        },
        employees_only=True,
      )

    return jsonify({"success": True, "updated_count": len(changed_usernames)})
  except ValueError as e:
    return jsonify({"success": False, "error": str(e)}), 400
  except Exception as e:
    logger.error(f"Error updating account proxy values: {e}")
    return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/config/<target>", methods=["POST"])
@login_required
def api_save_config(target):
    """Save configuration changes back to database."""
    try:
        payload = request.get_json(silent=True)
        if payload is None:
            payload = {} if target in ("settings", "model_message_map") else []
        user_ctx = _current_user_context()
        is_master = user_ctx["role"] == "master"
        can_edit_targets = user_ctx["role"] in ("master", "employee")

        if target == "settings":
            if not is_master:
                return jsonify({"success": False, "error": "Only master can update settings"}), 403
            database.save_settings(payload)
        elif target == "accounts":
            if not isinstance(payload, list):
                return jsonify({"success": False, "error": "Accounts payload must be a list"}), 400

            clean_accounts = []
            for idx, raw_acc in enumerate(payload):
                if not isinstance(raw_acc, dict):
                    return jsonify({"success": False, "error": f"Invalid account entry at index {idx}"}), 400

                username = str(raw_acc.get("username", "")).strip()
                if not username:
                    continue

                password = str(raw_acc.get("password", "")).strip()
                if not password:
                    return jsonify({
                        "success": False,
                        "error": f"Password is required for account '{username}'",
                    }), 400

                profile_note = str(raw_acc.get("profile_note", "") or "").strip()
                if not profile_note:
                  return jsonify({
                    "success": False,
                    "error": f"Bio + URL is required for account '{username}'",
                  }), 400

                proxy_value, too_many_proxies, _ = _normalize_proxy_value(raw_acc.get("proxy", ""))
                if too_many_proxies:
                  return jsonify({
                    "success": False,
                    "error": f"Account '{username}' supports maximum {MAX_PROXIES_PER_ACCOUNT} proxies",
                  }), 400

                account_entry = {
                    "username": username,
                    "password": password,
                    "model_label": str(raw_acc.get("model_label", "")).strip(),
                    "custom_messages": _normalize_text_list(raw_acc.get("custom_messages", [])),
                    "proxy": proxy_value,
                    "profile_note": profile_note,
                  "automation_enabled": _normalize_bool_flag(raw_acc.get("automation_enabled", True), default=True),
                  "is_suspended": _normalize_bool_flag(raw_acc.get("is_suspended", False), default=False),
                }
                if account_entry["is_suspended"]:
                    account_entry["automation_enabled"] = False
                if is_master:
                    account_entry["owner_username"] = str(raw_acc.get("owner_username", "")).strip().lower() or "master"

                clean_accounts.append(account_entry)

            if is_master:
                database.save_accounts(clean_accounts, include_all=True)
            else:
                database.save_accounts(clean_accounts, owner_username=user_ctx["username"], include_all=False)
            _log_actor_action(
              "update_accounts",
              target_type="config",
              target_value="accounts",
              details={
                "account_count": len(clean_accounts),
              },
              employees_only=True,
            )
        elif target == "models":
            if not can_edit_targets:
                return jsonify({"success": False, "error": "Not allowed to update models"}), 403
            if not isinstance(payload, list):
                return jsonify({"success": False, "error": "Models payload must be a list"}), 400
            clean_models = []
            seen_models = set()
            for model in payload:
                name = str(model or "").strip().lstrip("@")
                key = name.lower()
                if not name or key in seen_models:
                    continue
                seen_models.add(key)
                clean_models.append(name)
            database.save_models(clean_models)
            _log_actor_action(
                "update_models",
                target_type="config",
                target_value="models",
                details={
                    "model_count": len(clean_models),
                    "model_names": ", ".join(clean_models[:10]) + (" ..." if len(clean_models) > 10 else ""),
                },
                employees_only=False,
            )
        elif target == "messages":
            if not can_edit_targets:
                return jsonify({"success": False, "error": "Not allowed to update messages"}), 403
            if not isinstance(payload, list):
                return jsonify({"success": False, "error": "Messages payload must be a list"}), 400
            clean_messages = [str(msg or "").strip() for msg in payload if str(msg or "").strip()]
            sample_messages = "; ".join(clean_messages[:3])
            if len(sample_messages) > 120:
                sample_messages = sample_messages[:117] + "..."
            database.save_messages(clean_messages)
            _log_actor_action(
                "update_messages",
                target_type="config",
                target_value="messages",
                details={
                    "message_count": len(clean_messages),
                    "message_sample": sample_messages,
                },
                employees_only=False,
            )
        elif target == "comments":
            if not can_edit_targets:
                return jsonify({"success": False, "error": "Not allowed to update comments"}), 403
            if not isinstance(payload, list):
                return jsonify({"success": False, "error": "Comments payload must be a list"}), 400
            clean_comments = [str(c or "").strip() for c in payload if str(c or "").strip()]
            sample_comments = "; ".join(clean_comments[:3])
            if len(sample_comments) > 120:
                sample_comments = sample_comments[:117] + "..."
            database.save_comments(clean_comments)
            _log_actor_action(
                "update_comments",
                target_type="config",
                target_value="comments",
                details={
                    "comment_count": len(clean_comments),
                    "comment_sample": sample_comments,
                },
                employees_only=False,
            )
        elif target == "model_message_map":
            if not can_edit_targets:
                return jsonify({"success": False, "error": "Not allowed to update model-specific messages"}), 403
            if not isinstance(payload, dict):
                return jsonify({"success": False, "error": "MODEL_MESSAGE_MAP payload must be an object"}), 400

            # Backward-compatible payload support:
            # 1) legacy: {"model": ["msg1", ...]}
            # 2) current: {"model_message_map": {...}, "model_automation_map": {...}}
            if "model_message_map" in payload or "model_automation_map" in payload:
                raw_payload_model_map = payload.get("model_message_map", {})
                raw_payload_automation_map = payload.get("model_automation_map", {})
            else:
                raw_payload_model_map = payload
                raw_payload_automation_map = {}

            if not isinstance(raw_payload_model_map, dict):
                return jsonify({"success": False, "error": "model_message_map must be an object"}), 400
            if not isinstance(raw_payload_automation_map, dict):
                return jsonify({"success": False, "error": "model_automation_map must be an object"}), 400

            actor_username = str(user_ctx.get("username") or "unknown")
            now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            existing_settings = database.get_all_settings()
            raw_existing_map = existing_settings.get("MODEL_MESSAGE_MAP", {})
            existing_map = raw_existing_map if isinstance(raw_existing_map, dict) else {}
            raw_existing_meta = existing_settings.get("MODEL_MESSAGE_META", {})
            existing_meta = raw_existing_meta if isinstance(raw_existing_meta, dict) else {}

            raw_existing_automation_map = existing_settings.get("MODEL_AUTOMATION_MAP", {})
            existing_automation_map = {}
            if isinstance(raw_existing_automation_map, dict):
                for raw_model, raw_enabled in raw_existing_automation_map.items():
                    model_key = str(raw_model or "").strip().lstrip("@").lower()
                    if not model_key:
                        continue
                    existing_automation_map[model_key] = _normalize_bool_flag(raw_enabled, default=True)

            payload_automation_map = {}
            for raw_model, raw_enabled in raw_payload_automation_map.items():
                model_key = str(raw_model or "").strip().lstrip("@").lower()
                if not model_key:
                    continue
                payload_automation_map[model_key] = _normalize_bool_flag(raw_enabled, default=True)

            normalized_map = {}
            for raw_model, raw_messages in raw_payload_model_map.items():
                model_key = str(raw_model or "").strip().lstrip("@").lower()
                if not model_key or not isinstance(raw_messages, list):
                    continue

                clean_messages = [str(msg or "").strip() for msg in raw_messages if str(msg or "").strip()]
                if not clean_messages:
                    continue

                normalized_map[model_key] = clean_messages

            normalized_automation_map = {}
            for model_key in normalized_map.keys():
                if model_key in payload_automation_map:
                    normalized_automation_map[model_key] = payload_automation_map[model_key]
                elif model_key in existing_automation_map:
                    normalized_automation_map[model_key] = existing_automation_map[model_key]
                else:
                    normalized_automation_map[model_key] = True

            model_meta = {}
            total_messages = 0
            for model_key, clean_messages in normalized_map.items():
                total_messages += len(clean_messages)

                prev_meta = existing_meta.get(model_key, {})
                if not isinstance(prev_meta, dict):
                    prev_meta = {}

                prev_messages_raw = existing_map.get(model_key, [])
                prev_messages = (
                    [str(msg or "").strip() for msg in prev_messages_raw if str(msg or "").strip()]
                    if isinstance(prev_messages_raw, list)
                    else []
                )
                is_changed = prev_messages != clean_messages

                created_by = str(prev_meta.get("created_by") or actor_username)
                created_at = str(prev_meta.get("created_at") or now_iso)

                if is_changed:
                    updated_by = actor_username
                    updated_at = now_iso
                else:
                    updated_by = str(prev_meta.get("updated_by") or created_by)
                    updated_at = str(prev_meta.get("updated_at") or created_at)

                model_meta[model_key] = {
                    "created_by": created_by,
                    "created_at": created_at,
                    "updated_by": updated_by,
                    "updated_at": updated_at,
                }

            model_names = list(normalized_map.keys())
            database.save_settings({
                "MODEL_MESSAGE_MAP": normalized_map,
                "MODEL_MESSAGE_META": model_meta,
                "MODEL_AUTOMATION_MAP": normalized_automation_map,
            })
            _log_actor_action(
                "update_model_message_map",
                target_type="config",
                target_value="MODEL_MESSAGE_MAP",
                details={
                    "model_entry_count": len(model_names),
                    "message_count": total_messages,
                    "disabled_model_count": sum(1 for v in normalized_automation_map.values() if not bool(v)),
                    "model_names": ", ".join(model_names[:10]) + (" ..." if len(model_names) > 10 else ""),
                },
                employees_only=False,
            )
            return jsonify({
                "success": True,
                "model_message_map": normalized_map,
                "model_message_meta": model_meta,
                "model_automation_map": normalized_automation_map,
            })
        else:
            return jsonify({"success": False, "error": "Invalid target"}), 400

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error saving {target}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/users", methods=["POST"])
@login_required
@master_required
def api_create_user():
    """Create a new dashboard user (master-only)."""
    try:
        payload = request.get_json() or {}
        user = database.create_user(
            payload.get("username", ""),
            payload.get("password", ""),
            payload.get("role", "employee"),
        )
        return jsonify({"success": True, "user": user})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/users/<username>/password", methods=["POST"])
@login_required
@master_required
def api_update_user_password(username):
    """Reset password for a dashboard user (master-only)."""
    try:
        payload = request.get_json() or {}
        ok = database.update_user_password(username, payload.get("password", ""))
        if not ok:
            return jsonify({"success": False, "error": "User not found"}), 404
        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error updating user password: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/users/<username>", methods=["PUT", "POST"])
@app.route("/api/users/<username>/update", methods=["POST"])
@login_required
@master_required
def api_update_user_credentials(username):
    """Update dashboard username and/or password (master-only)."""
    try:
        payload = request.get_json() or {}
        result = database.update_user_credentials(
            username,
            new_username=payload.get("username"),
            new_password=payload.get("password"),
        )
        if not result:
            return jsonify({"success": False, "error": "User not found"}), 404

        # Keep current session consistent if the active master renamed this account.
        if session.get("username", "").strip().lower() == result["old_username"]:
            session["username"] = result["username"]

        return jsonify({"success": True, "user": result})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error updating user credentials: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/users/<username>", methods=["DELETE"])
@login_required
@master_required
def api_delete_user(username):
    """Delete a dashboard user (master-only)."""
    try:
        deleted = database.delete_user(username)
        if not deleted:
            return jsonify({"success": False, "error": "User not found"}), 404
        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cookies/<username>", methods=["GET"])
@login_required
def api_get_cookies(username):
    """Retrieve raw cookies JSON for an account if it exists."""
    try:
        user_ctx = _current_user_context()
        if not database.user_can_access_account(username, user_ctx["username"], user_ctx["role"]):
            return jsonify({"success": False, "error": "Forbidden"}), 403

        cookies_list = database.get_cookies(username)
        if cookies_list:
            return jsonify({"success": True, "cookies": json.dumps(cookies_list, indent=2)})
        return jsonify({"success": True, "cookies": ""})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cookies/<username>", methods=["POST"])
@login_required
def api_save_cookies(username):
    """Save raw cookies JSON directly to the database."""
    try:
      user_ctx = _current_user_context()
      if not database.user_can_access_account(username, user_ctx["username"], user_ctx["role"]):
        return jsonify({"success": False, "error": "Forbidden"}), 403

      payload = request.get_json()
      cookies_str = payload.get("cookies", "")
      cookie_count = 0

      if not cookies_str.strip():
        database.save_cookies(username, [])
      else:
        cookies_list = json.loads(cookies_str)
        database.save_cookies(username, cookies_list)
        cookie_count = len(cookies_list) if isinstance(cookies_list, list) else 1

      _log_actor_action(
        "save_cookies",
        target_type="account",
        target_value=username,
        details={
          "cookie_count": cookie_count,
          "cleared": cookie_count == 0,
        },
        employees_only=True,
      )

      return jsonify({"success": True})
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid JSON format"}), 400
    except Exception as e:
        logger.error(f"Error saving cookies for {username}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/activity/employee", methods=["GET"])
@login_required
@master_required
def api_employee_activity():
    """Get recent employee actions for the master dashboard."""
    try:
        limit_raw = request.args.get("limit", "150")
        try:
            limit = int(limit_raw)
        except Exception:
            limit = 150

        logs = database.get_activity_logs(limit=limit, employees_only=True)
        return jsonify({"success": True, "logs": logs})
    except Exception as e:
        logger.error(f"Error loading employee activity: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/activity/all", methods=["GET"])
@login_required
@master_required
def api_all_activity():
    """Get all dashboard activity for the last N hours (default: 24)."""
    try:
        limit_raw = request.args.get("limit", "500")
        hours_raw = request.args.get("hours", "24")

        try:
            limit = int(limit_raw)
        except Exception:
            limit = 500

        try:
            hours = int(hours_raw)
        except Exception:
            hours = 24

        logs = database.get_activity_logs_recent_hours(hours=hours, limit=limit, employees_only=False)

        day_counts = {}
        for row in logs:
            day_key = str(row.get("created_at") or "").strip()[:10] or "-"
            day_counts[day_key] = day_counts.get(day_key, 0) + 1

        by_day = [
            {"day": day, "count": day_counts[day]}
            for day in sorted(day_counts.keys(), reverse=True)
        ]

        return jsonify({
            "success": True,
            "logs": logs,
            "by_day": by_day,
            "hours": hours,
        })
    except Exception as e:
        logger.error(f"Error loading all activity: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/status")
@login_required
@master_required
def api_status():
    _refresh_total_dms_all_time()
    _ensure_telegram_polling()
    response = dict(bot_state)
    response["heavy_cl_running"] = _is_heavy_cl_running()
    response["heavy_cl_status"] = heavy_cl_state.get("status", "idle")
    response["engagement_running"] = _is_engagement_bot_running()
    response["engagement_status"] = engagement_state.get("status", "idle")
    cluster_control = _get_cluster_control_payload()
    if cluster_control:
      response["cluster_desired_state"] = cluster_control.get("desired_state", "")
      response["cluster_issued_by"] = cluster_control.get("issued_by", "")
      response["cluster_issued_at"] = cluster_control.get("issued_at", "")
    return jsonify(response)


@app.after_request
def add_no_cache_headers(response):
    """Prevent stale dashboard/template content in browser cache."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/start")
@login_required
def start_bot():
    user_ctx = _current_user_context()

    if _is_comment_liking_bot_running():
      return "Comment-liking bot is running. Stop it before starting DM bot.", 409

    if _is_heavy_cl_running():
      return "Heavy comment-liking bot is running. Stop it before starting DM bot.", 409

    try:
      _publish_cluster_control(
        desired_state="running",
        issued_by=user_ctx["username"],
        issued_by_role=user_ctx["role"],
      )
    except Exception as e:
      logger.warning(f"Failed to publish cluster start command: {e}")

    started = _start_bot_loop(
        started_by=user_ctx["username"],
        started_by_role=user_ctx["role"],
    )
    if not started:
        logger.info("Start requested but bot loop is already running.")
        return "<script>window.location='/'</script>"

    _log_actor_action(
        "start_bot",
        target_type="runtime",
        target_value="bot",
      details={"status": "running", "scope": "cluster"},
        employees_only=True,
    )
    return "<script>window.location='/'</script>"


@app.route("/stop")
@login_required
def stop_bot():
    user_ctx = _current_user_context()

    try:
      _publish_cluster_control(
        desired_state="stopped",
        issued_by=user_ctx["username"],
        issued_by_role=user_ctx["role"],
      )
    except Exception as e:
      logger.warning(f"Failed to publish cluster stop command: {e}")

    _request_local_stop()

    _log_actor_action(
        "stop_bot",
        target_type="runtime",
        target_value="bot",
        details={"status": bot_state.get("status", "stopped"), "scope": "cluster"},
        employees_only=True,
    )
    return "<script>window.location='/'</script>"


# ── Inbox Scraping & Reply ──
inbox_state = {
  "status": "idle",       # idle | scraping | stopping | stopped | done | error
  "account": "",
  "progress": "",
  "error": "",
  "threads_found": 0,
  "last_scraped_at": "",
  "stop_requested": False,
}
inbox_thread = None
inbox_thread_lock = threading.Lock()
inbox_scrape_stop_event = threading.Event()

inbox_account_locks = {}
inbox_account_locks_guard = threading.Lock()
inbox_refresh_threads = {}
inbox_refresh_threads_lock = threading.Lock()


def _parse_bool_flag(raw_value, default: bool = False) -> bool:
  text = str(raw_value or "").strip().lower()
  if text in ("1", "true", "yes", "on"):
    return True
  if text in ("0", "false", "no", "off"):
    return False
  return bool(default)


def _is_inbox_scraper_enabled() -> bool:
  return _parse_bool_flag(
    database.get_setting(INBOX_SCRAPER_ENABLED_SETTING_KEY, False),
    default=False,
  )


def _is_inbox_replier_enabled() -> bool:
  return _parse_bool_flag(
    database.get_setting(INBOX_REPLIER_ENABLED_SETTING_KEY, False),
    default=False,
  )


def _is_comment_liking_enabled() -> bool:
  return _parse_bool_flag(
    database.get_setting(COMMENT_LIKING_ENABLED_SETTING_KEY, False),
    default=False,
  )


def _set_inbox_scraper_enabled(enabled: bool):
  database.save_settings({INBOX_SCRAPER_ENABLED_SETTING_KEY: bool(enabled)})


def _set_inbox_replier_enabled(enabled: bool):
  database.save_settings({INBOX_REPLIER_ENABLED_SETTING_KEY: bool(enabled)})


def _set_comment_liking_enabled(enabled: bool):
  database.save_settings({COMMENT_LIKING_ENABLED_SETTING_KEY: bool(enabled)})


def _mark_inbox_scrape_stopped(message: str = "Scrape stopped by user."):
  inbox_state["status"] = "stopped"
  inbox_state["progress"] = str(message or "Scrape stopped by user.")
  inbox_state["error"] = ""
  inbox_state["stop_requested"] = True


def _request_inbox_scrape_stop():
  inbox_scrape_stop_event.set()
  inbox_state["stop_requested"] = True
  if inbox_state.get("status") == "scraping":
    inbox_state["status"] = "stopping"
    inbox_state["progress"] = "Stopping inbox scrape..."


def _user_can_access_ig_account(user_ctx: dict, ig_account: str) -> bool:
  return database.user_can_access_account(
    ig_account,
    user_ctx.get("username", ""),
    user_ctx.get("role", "employee"),
  )


def _get_account_row_by_username(ig_account: str):
  clean_account = str(ig_account or "").strip().lower()
  if not clean_account:
    return None

  for acc in database.get_accounts(include_all=True):
    username = str(acc.get("username", "")).strip().lower()
    if username == clean_account:
      return acc
  return None


def _get_inbox_automation_state(ig_account: str, account_row=None) -> dict:
  clean_account = str(ig_account or "").strip().lower()
  row = account_row if isinstance(account_row, dict) else _get_account_row_by_username(clean_account)
  replier_enabled = _is_inbox_replier_enabled()

  if not row:
    return {
      "exists": False,
      "automation_enabled": False,
      "is_suspended": False,
      "replier_enabled": replier_enabled,
      "reply_allowed": False,
      "blocked_reason": f"Account @{clean_account} not found",
    }

  is_suspended = _normalize_bool_flag(row.get("is_suspended", False), default=False)
  automation_enabled = False if is_suspended else _normalize_bool_flag(
    row.get("automation_enabled", True),
    default=True,
  )

  blocked_reason = ""
  if is_suspended:
    blocked_reason = f"Account @{clean_account} is suspended. Inbox replies are disabled."
  elif not automation_enabled:
    blocked_reason = f"Automation is disabled for @{clean_account}. Inbox replies are disabled."
  elif not replier_enabled:
    blocked_reason = "Inbox replier is stopped. Start Replier to send replies."

  return {
    "exists": True,
    "automation_enabled": automation_enabled,
    "is_suspended": is_suspended,
    "replier_enabled": replier_enabled,
    "reply_allowed": bool(automation_enabled and not is_suspended and replier_enabled),
    "blocked_reason": blocked_reason,
  }


def _thread_has_new_messages(thread_row: dict) -> bool:
  if not isinstance(thread_row, dict):
    return False

  try:
    if int(thread_row.get("new_message_count") or 0) > 0:
      return True
  except Exception:
    pass

  preview_text = " ".join(str(thread_row.get("message_preview", "") or "").split()).lower()
  if not preview_text:
    return False

  return bool(re.search(r"\bnew\s+messages?\b", preview_text))


def _acquire_inbox_account_lock(ig_account: str):
  clean_account = str(ig_account or "").strip().lower()
  if not clean_account:
    return "", None

  with inbox_account_locks_guard:
    lock = inbox_account_locks.get(clean_account)
    if lock is None:
      lock = threading.Lock()
      inbox_account_locks[clean_account] = lock

  acquired = lock.acquire(blocking=False)
  if not acquired:
    return clean_account, None
  return clean_account, lock


def _release_inbox_account_lock(lock_obj):
  if not lock_obj:
    return
  try:
    lock_obj.release()
  except Exception:
    pass


def _safe_remove_upload(upload_path: str):
  target = str(upload_path or "").strip()
  if not target:
    return
  try:
    if os.path.isfile(target):
      os.remove(target)
  except Exception:
    pass


def _trim_inbox_reply_jobs_locked():
  return


def _create_inbox_reply_job(thread_row: dict, text_message: str, has_attachment: bool, upload_path: str = "") -> str:
  return create_inbox_reply_job_queue(
    ig_account=str(thread_row.get("ig_account", "")).strip().lower(),
    thread_id=int(thread_row.get("id", 0) or 0),
    thread_display_name=str(thread_row.get("display_name", "")).strip(),
    text_message=str(text_message or "").strip(),
    has_attachment=bool(has_attachment),
    upload_path=str(upload_path or "").strip(),
  )


def _update_inbox_reply_job(job_id: str, **updates):
  update_inbox_reply_job_queue(job_id, **updates)


def _get_inbox_reply_job(job_id: str):
  return get_inbox_reply_job_queue(job_id, include_private=False)


def _login_for_inbox_action(driver, account_row: dict, ig_account: str, progress_callback=None) -> bool:
  from core.auth import login_with_cookies, login_with_credentials

  def _report(message: str):
    if callable(progress_callback):
      try:
        progress_callback(message)
      except Exception:
        pass

  _report(f"Logging in as @{ig_account}...")

  logged_in = login_with_cookies(driver, account_row)
  if logged_in:
    return True

  password = str(account_row.get("password", "") or "").strip()
  if not password:
    return False

  _report(f"Cookie login failed, trying credentials for @{ig_account}...")
  return login_with_credentials(driver, account_row)


def _inbox_scrape_worker(ig_account: str, proxy_value: str = "", account_lock=None):
  """Background worker: launch browser, log in, scrape unread DM threads."""
  global inbox_state

  from core.browser import create_driver, close_driver
  from core.inbox_reader import scrape_inbox_threads, scrape_thread_messages

  driver = None
  try:
    inbox_state["status"] = "scraping"
    inbox_state["account"] = ig_account
    inbox_state["progress"] = "Launching browser..."
    inbox_state["error"] = ""
    inbox_state["threads_found"] = 0
    inbox_state["stop_requested"] = False

    if inbox_scrape_stop_event.is_set():
      _mark_inbox_scrape_stopped("Scrape stopped before launch.")
      return

    account_row = _get_account_row_by_username(ig_account)
    if not account_row:
      inbox_state["status"] = "error"
      inbox_state["error"] = f"Account @{ig_account} not found in database"
      return

    proxy = proxy_value or str(account_row.get("proxy", "") or "").strip()

    inbox_state["progress"] = "Starting browser session..."
    driver = create_driver(headless=False, proxy=proxy if proxy else None)

    login_ok = _login_for_inbox_action(
      driver,
      account_row,
      ig_account,
      progress_callback=lambda msg: inbox_state.__setitem__("progress", msg),
    )
    if not login_ok:
      inbox_state["status"] = "error"
      inbox_state["error"] = f"Failed to log in as @{ig_account}. Check cookies or credentials."
      return

    if inbox_scrape_stop_event.is_set():
      _mark_inbox_scrape_stopped("Scrape stopped after login.")
      return

    inbox_state["progress"] = "Navigating to DM inbox..."
    threads = scrape_inbox_threads(driver, max_threads=0, unread_only=False)

    database.save_inbox_threads(ig_account, threads)

    if inbox_scrape_stop_event.is_set():
      _mark_inbox_scrape_stopped("Scrape stopped after syncing thread list.")
      return

    # Warm chat cache so opening a thread shows real conversation bubbles.
    cached_threads = database.get_inbox_threads(ig_account=ig_account, unread_only=False)
    id_by_display = {}
    for thread_row in cached_threads:
      display_key = str(thread_row.get("display_name", "")).strip().lower()
      if not display_key or display_key in id_by_display:
        continue
      try:
        id_by_display[display_key] = int(thread_row.get("id") or 0)
      except Exception:
        continue

    new_message_threads = []
    remaining_threads = []
    for thread in threads:
      if _thread_has_new_messages(thread):
        new_message_threads.append(thread)
      else:
        remaining_threads.append(thread)

    # Always open/scrape threads that have "new messages" badges.
    if INBOX_PREFETCH_THREAD_LIMIT > 0:
      remaining_budget = max(0, INBOX_PREFETCH_THREAD_LIMIT - len(new_message_threads))
      prefetch_queue = new_message_threads + remaining_threads[:remaining_budget]
    else:
      prefetch_queue = new_message_threads + remaining_threads

    prefetch_target = len(prefetch_queue)
    if prefetch_target > 0:
      for idx, thread in enumerate(prefetch_queue, start=1):
        if inbox_scrape_stop_event.is_set():
          _mark_inbox_scrape_stopped("Scrape stopped by user.")
          return

        display_name = str(thread.get("display_name", "")).strip()
        if not display_name:
          continue

        thread_id = id_by_display.get(display_name.lower())
        if not thread_id:
          continue

        is_new_thread = _thread_has_new_messages(thread)
        try:
          badge_count = int(thread.get("new_message_count") or 0)
        except Exception:
          badge_count = 0

        if is_new_thread:
          badge_text = f" ({badge_count} new)" if badge_count > 0 else ""
          inbox_state["progress"] = f"Syncing new-message chat {idx}/{prefetch_target}{badge_text}..."
        else:
          inbox_state["progress"] = f"Caching chats {idx}/{prefetch_target}..."

        max_messages = 0 if is_new_thread else INBOX_PREFETCH_MESSAGES_LIMIT
        try:
          scrape_thread_messages(
            driver,
            display_name,
            max_messages=max_messages,
            thread_id=thread_id,
            recent_hours=(1 if is_new_thread else 0),
          )
        except Exception as cache_error:
          logger.debug(f"[Inbox] Prefetch failed for '{display_name}': {cache_error}")

    if inbox_scrape_stop_event.is_set():
      _mark_inbox_scrape_stopped("Scrape stopped by user.")
      return

    unread_threads = sum(1 for thread in threads if bool(thread.get("is_unread", False)))
    inbox_state["threads_found"] = len(threads)
    inbox_state["status"] = "done"
    inbox_state["progress"] = f"Found {len(threads)} thread(s), {unread_threads} unread"
    inbox_state["last_scraped_at"] = datetime.now().isoformat(timespec="seconds")
    inbox_state["stop_requested"] = False
    logger.info(f"[Inbox] Scraped {len(threads)} total threads ({unread_threads} unread) for @{ig_account}")

  except Exception as e:
    inbox_state["status"] = "error"
    inbox_state["error"] = str(e)[:500]
    inbox_state["stop_requested"] = False
    logger.error(f"[Inbox] Scrape error for @{ig_account}: {e}")
  finally:
    if inbox_scrape_stop_event.is_set() and inbox_state.get("status") in ("scraping", "stopping"):
      _mark_inbox_scrape_stopped("Scrape stopped by user.")

    if driver:
      try:
        close_driver(driver)
      except Exception:
        pass

    if inbox_scrape_stop_event.is_set() and inbox_state.get("status") == "stopped":
      inbox_scrape_stop_event.clear()

    _release_inbox_account_lock(account_lock)


def _inbox_thread_refresh_worker(thread_row: dict, account_lock):
  from core.browser import create_driver, close_driver
  from core.inbox_reader import INBOX_URL, scrape_thread_messages

  driver = None
  thread_id = int(thread_row.get("id") or 0)
  ig_account = str(thread_row.get("ig_account", "")).strip().lower()
  thread_display_name = str(thread_row.get("display_name", "")).strip()

  try:
    account_row = _get_account_row_by_username(ig_account)
    if not account_row:
      logger.warning(f"[Inbox] Thread refresh skipped: @{ig_account} not found")
      return

    proxy = str(account_row.get("proxy", "") or "").strip()
    driver = create_driver(headless=False, proxy=proxy if proxy else None)

    login_ok = _login_for_inbox_action(driver, account_row, ig_account)
    if not login_ok:
      logger.warning(f"[Inbox] Thread refresh login failed for @{ig_account}")
      return

    driver.get(INBOX_URL)
    messages = scrape_thread_messages(
      driver,
      thread_display_name,
        max_messages=INBOX_PREFETCH_MESSAGES_LIMIT,
      thread_id=thread_id,
    )

    if messages:
      last_msg = messages[-1]
      preview_text = str(last_msg.get("text", "")).strip()
      if not preview_text and bool(last_msg.get("has_attachment", False)):
        preview_text = "[Attachment]"
      database.update_inbox_thread_preview(
        thread_id,
        preview_text,
        str(last_msg.get("timestamp", "")).strip(),
        str(last_msg.get("timestamp", "")).strip(),
      )

    logger.info(
      f"[Inbox] Refreshed thread #{thread_id} ({thread_display_name}) with {len(messages)} message(s)"
    )

  except Exception as e:
    logger.error(f"[Inbox] Thread refresh error for thread #{thread_id}: {e}")
  finally:
    if driver:
      try:
        close_driver(driver)
      except Exception:
        pass

    with inbox_refresh_threads_lock:
      inbox_refresh_threads.pop(thread_id, None)

    _release_inbox_account_lock(account_lock)


def _start_inbox_thread_refresh(thread_row: dict):
  thread_id = int(thread_row.get("id") or 0)
  ig_account = str(thread_row.get("ig_account", "")).strip().lower()
  if thread_id <= 0 or not ig_account:
    return False, "invalid"

  with inbox_refresh_threads_lock:
    existing = inbox_refresh_threads.get(thread_id)
    if existing and existing.is_alive():
      return False, "running"

  _, account_lock = _acquire_inbox_account_lock(ig_account)
  if account_lock is None:
    return False, "busy"

  worker = threading.Thread(
    target=_inbox_thread_refresh_worker,
    args=(thread_row, account_lock),
    daemon=True,
    name=f"inbox-refresh-{thread_id}",
  )

  with inbox_refresh_threads_lock:
    inbox_refresh_threads[thread_id] = worker

  try:
    worker.start()
    return True, "started"
  except Exception:
    with inbox_refresh_threads_lock:
      inbox_refresh_threads.pop(thread_id, None)
    _release_inbox_account_lock(account_lock)
    return False, "error"


def _inbox_reply_worker(job_id: str, thread_row: dict, text_message: str, upload_path: str, account_lock):
  from core.browser import create_driver, close_driver
  from core.inbox_reader import INBOX_URL, reply_to_thread, scrape_thread_messages

  driver = None
  thread_id = int(thread_row.get("id") or 0)
  ig_account = str(thread_row.get("ig_account", "")).strip().lower()
  thread_display_name = str(thread_row.get("display_name", "")).strip()
  clean_text = str(text_message or "").strip()
  clean_upload = str(upload_path or "").strip()

  try:
    _update_inbox_reply_job(job_id, status="running", progress="Launching browser...", error="")

    account_row = _get_account_row_by_username(ig_account)
    if not account_row:
      _update_inbox_reply_job(job_id, status="error", error=f"Account @{ig_account} not found")
      return

    account_state = _get_inbox_automation_state(ig_account, account_row=account_row)
    if not bool(account_state.get("reply_allowed", False)):
      _update_inbox_reply_job(
        job_id,
        status="error",
        error=str(account_state.get("blocked_reason") or f"Inbox replies are disabled for @{ig_account}"),
      )
      return

    proxy = str(account_row.get("proxy", "") or "").strip()
    driver = create_driver(headless=False, proxy=proxy if proxy else None)

    login_ok = _login_for_inbox_action(
      driver,
      account_row,
      ig_account,
      progress_callback=lambda msg: _update_inbox_reply_job(job_id, progress=msg),
    )
    if not login_ok:
      _update_inbox_reply_job(
        job_id,
        status="error",
        error=f"Failed to log in as @{ig_account}. Check cookies or credentials.",
      )
      return

    _update_inbox_reply_job(job_id, progress=f"Sending reply to {thread_display_name}...")
    send_result = reply_to_thread(
      driver,
      thread_display_name=thread_display_name,
      text=clean_text,
      image_path=clean_upload,
    )
    if not bool(send_result.get("success", False)):
      _update_inbox_reply_job(
        job_id,
        status="error",
        error=str(send_result.get("error", "Reply failed")).strip() or "Reply failed",
      )
      return

    preview_text = clean_text
    if not preview_text and clean_upload:
      preview_text = "[Attachment]"

    now_str = datetime.now().isoformat(timespec="seconds")
    database.append_thread_message(
      thread_id=thread_id,
      sender_name="You",
      text_content=preview_text,
      has_attachment=bool(clean_upload),
      timestamp=now_str,
      is_self=True,
    )
    database.set_inbox_thread_unread(thread_id, False)
    database.update_inbox_thread_preview(
      thread_id,
      preview_text,
      "now",
      now_str,
    )

    _update_inbox_reply_job(job_id, progress="Refreshing thread messages...")
    try:
      driver.get(INBOX_URL)
      scrape_thread_messages(
        driver,
        thread_display_name,
        max_messages=INBOX_PREFETCH_MESSAGES_LIMIT,
        thread_id=thread_id,
      )
    except Exception as refresh_error:
      logger.debug(f"[Inbox] Could not refresh thread after reply: {refresh_error}")

    _update_inbox_reply_job(job_id, status="done", progress="Reply sent")

  except Exception as e:
    _update_inbox_reply_job(job_id, status="error", error=str(e)[:500])
    logger.error(f"[Inbox] Reply worker error for thread #{thread_id}: {e}")
  finally:
    if driver:
      try:
        close_driver(driver)
      except Exception:
        pass

    _safe_remove_upload(clean_upload)
    _release_inbox_account_lock(account_lock)


@app.route("/api/inbox/scrape", methods=["POST"])
@login_required
def api_inbox_scrape():
  global inbox_thread

  if not _is_inbox_scraper_enabled():
    return jsonify({
      "success": False,
      "error": "Chat scraper is stopped. Click Start Scraper first.",
    }), 409

  data = request.get_json(silent=True) or {}
  ig_account = str(data.get("username", "") or "").strip().lower()
  if not ig_account:
    return jsonify({"success": False, "error": "Missing 'username' field"}), 400

  user_ctx = _current_user_context()
  if not _user_can_access_ig_account(user_ctx, ig_account):
    return jsonify({"success": False, "error": "Forbidden for this account"}), 403

  _, account_lock = _acquire_inbox_account_lock(ig_account)
  if account_lock is None:
    return jsonify({
      "success": False,
      "error": f"Another inbox action is already running for @{ig_account}. Please wait.",
    }), 409

  with inbox_thread_lock:
    if inbox_thread and inbox_thread.is_alive():
      _release_inbox_account_lock(account_lock)
      return jsonify({
        "success": False,
        "error": f"A scrape is already running for @{inbox_state.get('account', '?')}. Please wait.",
      }), 409

    inbox_scrape_stop_event.clear()
    inbox_state["stop_requested"] = False

    inbox_thread = threading.Thread(
      target=_inbox_scrape_worker,
      args=(ig_account, "", account_lock),
      daemon=True,
    )
    inbox_thread.start()

  _log_actor_action(
    "inbox_scrape",
    target_type="ig_account",
    target_value=ig_account,
    details={"action": "scrape_started"},
    employees_only=False,
  )

  return jsonify({"success": True, "message": f"Inbox scrape started for @{ig_account}"})


@app.route("/api/inbox/scraper/control", methods=["POST"])
@login_required
def api_inbox_scraper_control():
  payload = request.get_json(silent=True) or {}
  action = str(payload.get("action", "") or "").strip().lower()
  if action not in ("start", "stop"):
    return jsonify({"success": False, "error": "action must be 'start' or 'stop'"}), 400

  user_ctx = _current_user_context()
  with inbox_thread_lock:
    is_running = bool(inbox_thread and inbox_thread.is_alive())
  running_account = str(inbox_state.get("account", "") or "").strip().lower()

  if action == "stop":
    if is_running and running_account and not _user_can_access_ig_account(user_ctx, running_account):
      return jsonify({"success": False, "error": "Forbidden for this account"}), 403

    _set_inbox_scraper_enabled(False)
    if is_running:
      _request_inbox_scrape_stop()
    else:
      inbox_scrape_stop_event.clear()
      inbox_state["status"] = "stopped"
      inbox_state["progress"] = "Chat scraper stopped"
      inbox_state["error"] = ""
      inbox_state["stop_requested"] = False
    message = "Chat scraper stopped"
  else:
    _set_inbox_scraper_enabled(True)
    inbox_scrape_stop_event.clear()
    inbox_state["stop_requested"] = False
    if inbox_state.get("status") in ("stopping", "stopped"):
      inbox_state["status"] = "idle"
      inbox_state["progress"] = ""
      inbox_state["error"] = ""
    message = "Chat scraper started"

  _log_actor_action(
    "inbox_scraper_control",
    target_type="runtime",
    target_value="inbox_scraper",
    details={"action": action, "is_running": is_running, "account": running_account},
    employees_only=False,
  )

  with inbox_thread_lock:
    is_running_now = bool(inbox_thread and inbox_thread.is_alive())

  return jsonify({
    "success": True,
    "action": action,
    "message": message,
    "scraper_enabled": _is_inbox_scraper_enabled(),
    "status": inbox_state.get("status", "idle"),
    "is_running": is_running_now,
    "stop_requested": bool(inbox_scrape_stop_event.is_set()),
  })


@app.route("/api/inbox/replier/control", methods=["POST"])
@login_required
def api_inbox_replier_control():
  payload = request.get_json(silent=True) or {}
  action = str(payload.get("action", "") or "").strip().lower()
  if action not in ("start", "stop"):
    return jsonify({"success": False, "error": "action must be 'start' or 'stop'"}), 400

  if action == "start" and _is_comment_liking_bot_running():
    return jsonify({
      "success": False,
      "error": "Comment-liking bot is running. Stop it before starting chat replier.",
    }), 409

  enabled = action == "start"
  _set_inbox_replier_enabled(enabled)

  _log_actor_action(
    "inbox_replier_control",
    target_type="runtime",
    target_value="inbox_replier",
    details={"action": action, "enabled": enabled},
    employees_only=False,
  )

  return jsonify({
    "success": True,
    "action": action,
    "message": "Chat replier started" if enabled else "Chat replier stopped",
    "replier_enabled": _is_inbox_replier_enabled(),
  })


@app.route("/api/bot/engagement/control", methods=["POST"], strict_slashes=False)
@login_required
def api_engagement_control():
  payload = request.get_json(silent=True) or {}
  action = str(payload.get("action", "") or "").strip().lower()
  if action not in ("start", "stop"):
    return jsonify({"success": False, "error": "action must be 'start' or 'stop'"}), 400
  user_ctx = _current_user_context()
  actor = str(user_ctx.get("username", "") or "").strip().lower() or "unknown"

  if action == "start":
    if _is_engagement_bot_running():
      return jsonify({"success": False, "error": "Engagement bot is already running"})

    _set_comment_liking_enabled(True)
    started = _start_engagement_loop(
      started_by=user_ctx.get("username", ""),
      started_by_role=user_ctx.get("role", "employee"),
    )
    is_running = _is_engagement_bot_running()
    if started:
      message = "Like-Comment bot started"
      _append_dashboard_log(f"✅ Like-Comment bot STARTED by @{actor}", level="INFO")
    else:
      message = "Like-Comment bot is already running"
  else:
    _set_comment_liking_enabled(False)
    was_running = _request_engagement_stop()
    is_running = _is_engagement_bot_running()
    message = "Like-Comment bot stopping" if was_running else "Like-Comment bot stopped"
    _append_dashboard_log(f"⏹️ Like-Comment bot STOP requested by @{actor}", level="INFO")

  _log_actor_action(
    "engagement_control",
    target_type="runtime",
    target_value="engagement",
    details={
      "action": action,
      "enabled": bool(is_running),
      "status": engagement_state.get("status", "idle"),
    },
    employees_only=False,
  )

  return jsonify({
    "success": True,
    "action": action,
    "message": message,
    "engagement_enabled": bool(is_running),
    "engagement_status": engagement_state.get("status", "idle"),
  })


@app.route("/api/heavy-comment-liking/control", methods=["POST"])
@login_required
def api_heavy_comment_liking_control():
  payload = request.get_json(silent=True) or {}
  action = str(payload.get("action", "") or "").strip().lower()
  if action not in ("start", "stop"):
    return jsonify({"success": False, "error": "action must be 'start' or 'stop'"}), 400
  user_ctx = _current_user_context()
  actor = str(user_ctx.get("username", "") or "").strip().lower() or "unknown"

  if action == "start" and _is_bot_running():
    return jsonify({
      "success": False,
      "error": "DM bot is already running. Stop it first.",
    }), 409

  if action == "start" and _is_comment_liking_bot_running():
    return jsonify({
      "success": False,
      "error": "Comment-liking bot is already running. Stop it first.",
    }), 409

  if action == "start":
    started = _start_heavy_cl_loop(
      started_by=user_ctx.get("username", ""),
      started_by_role=user_ctx.get("role", "employee"),
    )
    is_running = _is_heavy_cl_running()
    if started:
      message = "Heavy comment-liking bot started"
      _append_dashboard_log(f"✅ Heavy comment-liking bot STARTED by @{actor}", level="INFO")
    else:
      message = "Heavy comment-liking bot is already running"
  else:
    was_running = _request_heavy_cl_stop()
    is_running = _is_heavy_cl_running()
    message = "Heavy comment-liking bot stopping" if was_running else "Heavy comment-liking bot stopped"
    _append_dashboard_log(f"⏹️ Heavy comment-liking bot STOP requested by @{actor}", level="INFO")

  return jsonify({
    "success": True,
    "action": action,
    "message": message,
    "heavy_cl_status": heavy_cl_state.get("status", "idle"),
    "heavy_cl_running": bool(is_running),
  })


@app.route("/api/inbox/status")
@login_required
def api_inbox_status():
  with inbox_thread_lock:
    is_running = bool(inbox_thread and inbox_thread.is_alive())

  return jsonify({
    "success": True,
    "status": inbox_state.get("status", "idle"),
    "account": inbox_state.get("account", ""),
    "progress": inbox_state.get("progress", ""),
    "error": inbox_state.get("error", ""),
    "threads_found": inbox_state.get("threads_found", 0),
    "last_scraped_at": inbox_state.get("last_scraped_at", ""),
    "is_running": is_running,
    "stop_requested": bool(inbox_scrape_stop_event.is_set()) or bool(inbox_state.get("stop_requested", False)),
    "scraper_enabled": _is_inbox_scraper_enabled(),
    "replier_enabled": _is_inbox_replier_enabled(),
  })


@app.route("/api/inbox/threads", methods=["GET", "DELETE"])
@login_required
def api_inbox_threads():
  user_ctx = _current_user_context()
  is_master = user_ctx.get("role") == "master"

  owned_accounts = set()
  if not is_master:
    owner_username = str(user_ctx.get("username", "")).strip().lower()
    all_accounts = database.get_accounts(include_all=True)
    for acc in all_accounts:
      acc_owner = str(acc.get("owner_username", "") or "").strip().lower() or "master"
      if acc_owner == owner_username:
        owned_accounts.add(str(acc.get("username", "")).strip().lower())

  if request.method == "DELETE":
    payload = request.get_json(silent=True) or {}
    requested_accounts_raw = payload.get("accounts", [])

    requested_accounts = []
    if isinstance(requested_accounts_raw, (list, tuple, set)):
      seen_accounts = set()
      for raw_account in requested_accounts_raw:
        clean_account = str(raw_account or "").strip().lower()
        if clean_account and clean_account not in seen_accounts:
          seen_accounts.add(clean_account)
          requested_accounts.append(clean_account)

    threads_for_delete = database.get_inbox_threads(ig_account=None, unread_only=False)
    if not is_master:
      threads_for_delete = [
        t for t in threads_for_delete if str(t.get("ig_account", "")).strip().lower() in owned_accounts
      ]

    if requested_accounts:
      requested_set = set(requested_accounts)
      threads_for_delete = [
        t for t in threads_for_delete if str(t.get("ig_account", "")).strip().lower() in requested_set
      ]

    account_thread_counts = {}
    for thread in threads_for_delete:
      account = str(thread.get("ig_account", "")).strip().lower()
      if not account:
        continue
      account_thread_counts[account] = int(account_thread_counts.get(account, 0)) + 1

    target_accounts = sorted(account_thread_counts.keys())
    if not target_accounts:
      scope_text = "selected accounts" if requested_accounts else "accessible accounts"
      return jsonify({
        "success": True,
        "deleted_threads": 0,
        "deleted_accounts": [],
        "scope": scope_text,
        "message": f"No chats found for {scope_text}.",
      })

    acquired_locks = []
    for account in target_accounts:
      _, account_lock = _acquire_inbox_account_lock(account)
      if account_lock is None:
        for held_lock in acquired_locks:
          _release_inbox_account_lock(held_lock)
        return jsonify({
          "success": False,
          "error": f"Another inbox action is already running for @{account}. Please wait.",
        }), 409
      acquired_locks.append(account_lock)

    deleted_threads = 0
    try:
      for account in target_accounts:
        deleted_threads += int(account_thread_counts.get(account, 0))
        database.clear_inbox_threads(ig_account=account)
    finally:
      for held_lock in acquired_locks:
        _release_inbox_account_lock(held_lock)

    _log_actor_action(
      "inbox_thread_delete_all",
      target_type="inbox_threads",
      target_value=str(deleted_threads),
      details={
        "accounts": target_accounts,
        "deleted_threads": deleted_threads,
      },
      employees_only=False,
    )

    return jsonify({
      "success": True,
      "deleted_threads": deleted_threads,
      "deleted_accounts": target_accounts,
      "scope": "selected accounts" if requested_accounts else "accessible accounts",
      "message": f"Deleted {deleted_threads} chat(s)",
    })

  ig_account = request.args.get("account", "").strip().lower()
  unread_only = _parse_bool_flag(request.args.get("unread_only", "false"), default=False)

  threads = database.get_inbox_threads(
    ig_account=ig_account if ig_account else None,
    unread_only=unread_only,
  )

  # Employee scope: only show threads for accounts they own.
  if not is_master:
    threads = [t for t in threads if t.get("ig_account", "").lower() in owned_accounts]

  if is_master:
    unread_count = database.get_inbox_unread_count(ig_account if ig_account else None)
  else:
    unread_count = sum(1 for t in threads if bool(t.get("is_unread", False)))

  return jsonify({
    "success": True,
    "threads": threads,
    "total": len(threads),
    "unread_count": unread_count,
  })


@app.route("/api/inbox/thread/<int:thread_id>", methods=["GET", "DELETE"])
@app.route("/api/inbox/thread/<int:thread_id>/", methods=["GET", "DELETE"])
@login_required
def api_inbox_thread_messages(thread_id: int):
  thread_row = database.get_inbox_thread_by_id(thread_id)
  if not thread_row:
    return jsonify({"success": False, "error": "Thread not found"}), 404

  user_ctx = _current_user_context()
  if not _user_can_access_ig_account(user_ctx, thread_row.get("ig_account", "")):
    return jsonify({"success": False, "error": "Forbidden for this thread"}), 403

  if request.method == "DELETE":
    ig_account = str(thread_row.get("ig_account", "")).strip().lower()
    _, account_lock = _acquire_inbox_account_lock(ig_account)
    if account_lock is None:
      return jsonify({
        "success": False,
        "error": f"Another inbox action is already running for @{ig_account}. Please wait.",
      }), 409

    try:
      deleted = database.delete_inbox_thread(thread_id)
    finally:
      _release_inbox_account_lock(account_lock)

    if not deleted:
      return jsonify({"success": False, "error": "Thread not found"}), 404

    _log_actor_action(
      "inbox_thread_delete",
      target_type="inbox_thread",
      target_value=str(thread_id),
      details={
        "ig_account": ig_account,
        "display_name": str(thread_row.get("display_name", "")).strip(),
      },
      employees_only=False,
    )

    return jsonify({
      "success": True,
      "thread_id": thread_id,
      "message": "Thread deleted",
    })

  refresh_requested = _parse_bool_flag(request.args.get("refresh", "false"), default=False)
  refresh_started = False
  refresh_status = "idle"

  if refresh_requested:
    refresh_started, refresh_status = _start_inbox_thread_refresh(thread_row)

  with inbox_refresh_threads_lock:
    refresh_thread = inbox_refresh_threads.get(thread_id)
    is_refresh_running = bool(refresh_thread and refresh_thread.is_alive())

  messages = database.get_thread_messages(thread_id)

  return jsonify({
    "success": True,
    "thread": thread_row,
    "messages": messages,
    "refresh_requested": refresh_requested,
    "refresh_started": refresh_started,
    "refresh_status": refresh_status,
    "refresh_running": is_refresh_running,
  })


@app.route("/api/inbox/reply", methods=["POST"])
@login_required
def api_inbox_reply():
  if request.is_json:
    payload = request.get_json(silent=True) or {}
    thread_id_raw = payload.get("thread_id", "")
    text_message = str(payload.get("text_message", "") or "").strip()
  else:
    payload = {}
    thread_id_raw = request.form.get("thread_id", "")
    text_message = str(request.form.get("text_message", "") or "").strip()

  try:
    thread_id = int(thread_id_raw)
  except Exception:
    return jsonify({"success": False, "error": "Invalid thread_id"}), 400

  thread_row = database.get_inbox_thread_by_id(thread_id)
  if not thread_row:
    return jsonify({"success": False, "error": "Thread not found"}), 404

  user_ctx = _current_user_context()
  ig_account = str(thread_row.get("ig_account", "")).strip().lower()
  if not _user_can_access_ig_account(user_ctx, ig_account):
    return jsonify({"success": False, "error": "Forbidden for this thread"}), 403

  if not _is_inbox_replier_enabled():
    return jsonify({
      "success": False,
      "error": "Chat replier is stopped. Click Start Replier first.",
    }), 409

  account_row = _get_account_row_by_username(ig_account)
  account_state = _get_inbox_automation_state(ig_account, account_row=account_row)
  if not bool(account_state.get("reply_allowed", False)):
    return jsonify({
      "success": False,
      "error": str(account_state.get("blocked_reason") or f"Inbox replies are disabled for @{ig_account}"),
    }), 403

  upload_file = request.files.get("file") if not request.is_json else None
  upload_path = ""
  has_attachment = False

  if upload_file and str(upload_file.filename or "").strip():
    filename = secure_filename(str(upload_file.filename or "").strip())
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_REPLY_IMAGE_EXTENSIONS:
      return jsonify({
        "success": False,
        "error": "Only image files are allowed (.jpg, .jpeg, .png, .gif, .webp, .bmp)",
      }), 400

    mime = str(upload_file.mimetype or "").strip().lower()
    if mime and not mime.startswith("image/"):
      return jsonify({"success": False, "error": "Uploaded file must be an image"}), 400

    os.makedirs(UPLOADS_DIR, exist_ok=True)
    stamped_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid4().hex[:8]}{ext}"
    upload_path = os.path.join(UPLOADS_DIR, stamped_name)
    upload_file.save(upload_path)
    has_attachment = True

  if not text_message and not has_attachment:
    _safe_remove_upload(upload_path)
    return jsonify({"success": False, "error": "Reply must include text or an image"}), 400

  job_id = _create_inbox_reply_job(
    thread_row,
    text_message=text_message,
    has_attachment=has_attachment,
    upload_path=upload_path,
  )

  _log_actor_action(
    "inbox_reply",
    target_type="inbox_thread",
    target_value=str(thread_id),
    details={
      "ig_account": ig_account,
      "has_text": bool(text_message),
      "has_attachment": has_attachment,
      "job_id": job_id,
    },
    employees_only=False,
  )

  return jsonify({
    "success": True,
    "job_id": job_id,
    "thread_id": thread_id,
    "message": "Reply queued for bot runtime",
  })




@app.route("/api/inbox/reply/status/<job_id>", methods=["GET"])
@login_required
def api_inbox_reply_status(job_id: str):
  job = _get_inbox_reply_job(job_id)
  if not job:
    return jsonify({"success": False, "error": "Reply job not found"}), 404

  user_ctx = _current_user_context()
  if not _user_can_access_ig_account(user_ctx, job.get("ig_account", "")):
    return jsonify({"success": False, "error": "Forbidden"}), 403

  return jsonify({
    "success": True,
    "job": job,
  })


# Main
if __name__ == "__main__":
    database.init_db()

    print("=" * 60)
    print("  INSTAGRAM DM BOT - FLASK SERVER")
    print("=" * 60)
    print("  Dashboard: http://localhost:5000")
    print(f"  System Booting...")
    print("=" * 60)
    print()

    # Bot will remain idle until started via the web UI dashboard.
    bot_state["status"] = "idle"

    _ensure_cluster_control_watcher()

    if _env_is_true("BOT_AUTO_START", default=False):
        started = _start_bot_loop(started_by="system", started_by_role="master")
        if started:
            logger.info("BOT_AUTO_START enabled. Bot loop started automatically.")
        else:
            logger.info("BOT_AUTO_START enabled but bot loop is already running.")

    app.run(host="0.0.0.0", port=5000, debug=False)

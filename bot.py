"""
Main orchestrator — the brain of the Model DM Bot.
Coordinates accounts, models, scraping, DMs, and Telegram alerts.
"""
import json
import os
import sys
import time
import random
import re
import logging
import threading
from datetime import datetime, timedelta
from uuid import uuid4
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from config.settings import LOGS_DIR, INSTAGRAM_BASE_URL
from config import database
from config.database import get_required_setting
from core.browser import create_driver, close_driver, _mask_proxy_for_log
from core.cookie_manager import save_cookies, refresh_cookies
from core.auth import (
    login_with_cookies, login_with_credentials,
    detect_challenge, handle_two_factor,
    is_logged_in, human_delay, ChallengeType,
    type_like_human, human_scroll,
)
from core.scraper import get_recent_posts, get_post_interactors, sort_posts_by_priority
from core.followers import get_followers
from core.dm_sender import send_dm, DMResult, wait_between_dms
from core.inbox_reply_queue import (
    claim_next_reply_for_account,
    has_queued_reply_for_account,
    update_reply_job,
)
from core.distributed_coordination import DistributedCoordinator
from telegram.bot import telegram_bot

logger = logging.getLogger("model_dm_bot")
_active_drivers = set()
_active_drivers_lock = threading.Lock()
DM_SUMMARY_WINDOW_HOURS = 24
MAX_ACCOUNT_PROXIES = 5
CLUSTER_NOTIFICATION_COOLDOWN_SEC = 24 * 60 * 60
CLUSTER_NOTIFICATION_FALLBACK_BUCKET_SEC = 10 * 60
INBOX_REPLIER_ENABLED_SETTING_KEY = "INBOX_REPLIER_ENABLED"
DM_BATCH_PAUSE_ENABLED_SETTING_KEY = "DM_BATCH_PAUSE_ENABLED"
DM_BATCH_SIZE = 10
DM_BATCH_COOLDOWN_SECONDS = 5 * 60


def _setting_int(key: str) -> int:
    """Read an integer setting from the database."""
    value = get_required_setting(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer setting '{key}': {value}")


def _setting_float(key: str) -> float:
    """Read a float setting from the database."""
    value = get_required_setting(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid numeric setting '{key}': {value}")


def _setting_bool(key: str, default: bool = False) -> bool:
    """Read a boolean setting from the database."""
    value = database.get_setting(key, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    if isinstance(value, (int, float)):
        return int(value) != 0

    text = str(value).strip().lower()
    if text in ("1", "true", "on", "yes", "enable", "enabled"):
        return True
    if text in ("0", "false", "off", "no", "disable", "disabled", "", "none", "null"):
        return False
    return bool(default)

def _setting_int_default(key: str, default: int) -> int:
    """Read an integer setting with a hard fallback when parsing fails."""
    value = database.get_setting(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _setting_text_list(key: str, default_values: list) -> list:
    """Read a text list setting from either list JSON or comma/newline text."""
    value = database.get_setting(key, default_values)

    items = []
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = re.split(r"[\r\n,;]+", value)
    else:
        items = list(default_values or [])

    clean = []
    seen = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue

        key_text = text.lower()
        if key_text in seen:
            continue

        seen.add(key_text)
        clean.append(text)

    if clean:
        return clean

    return [str(item or "").strip() for item in (default_values or []) if str(item or "").strip()]


def _interruptible_sleep(seconds: float, stop_event=None, tick: float = 0.5) -> bool:
    """Sleep in short ticks so stop requests can interrupt long waits."""
    end_time = time.time() + max(0, seconds)
    while time.time() < end_time:
        if stop_event and stop_event.is_set():
            return True
        remaining = end_time - time.time()
        time.sleep(min(tick, max(0.0, remaining)))
    return False






def _maybe_wait_for_dm_batch_cooldown(sender: str, dm_batch_state: dict, stop_event=None):
    """Pause after each full DM batch for one account when safety toggle is enabled."""
    if not isinstance(dm_batch_state, dict):
        return

    if not _setting_bool(DM_BATCH_PAUSE_ENABLED_SETTING_KEY, default=False):
        return

    sent_count = int(dm_batch_state.get("sent_count", 0) or 0)
    if sent_count <= 0 or (sent_count % DM_BATCH_SIZE) != 0:
        return

    last_cooldown_count = int(dm_batch_state.get("last_cooldown_count", 0) or 0)
    if last_cooldown_count == sent_count:
        return

    cooldown_minutes = int(DM_BATCH_COOLDOWN_SECONDS // 60)
    log_and_telegram(
        f"[{sender}] 🛡️ Safety pause: {DM_BATCH_SIZE} DMs sent. Waiting {cooldown_minutes} minutes before next batch..."
    )
    interrupted = _interruptible_sleep(DM_BATCH_COOLDOWN_SECONDS, stop_event=stop_event)
    dm_batch_state["last_cooldown_count"] = sent_count

    if interrupted:
        log_and_telegram(f"[{sender}] 🛑 Safety pause interrupted by stop request.")
    else:
        log_and_telegram(f"[{sender}] ▶️ Safety pause complete. Continuing DM batch.")


def _register_driver(driver):
    with _active_drivers_lock:
        _active_drivers.add(driver)


def _unregister_driver(driver):
    with _active_drivers_lock:
        _active_drivers.discard(driver)


def force_stop_active_sessions():
    """Force-close active browsers so stop requests interrupt current Selenium tasks."""
    with _active_drivers_lock:
        drivers = list(_active_drivers)
        _active_drivers.clear()

    for drv in drivers:
        try:
            close_driver(drv)
        except Exception:
            pass


def _safe_remove_file(file_path: str):
    target = str(file_path or "").strip()
    if not target:
        return

    try:
        if os.path.isfile(target):
            os.remove(target)
    except Exception:
        pass


def setup_logging():
    """Configure logging to file and console (only once)."""
    root_logger = logging.getLogger("model_dm_bot")
    if root_logger.handlers:
        return  # Already set up

    log_file = os.path.join(LOGS_DIR, "bot.log")
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(fh)
    root_logger.addHandler(ch)


# Old JSON functions removed. Bot now relies on Database.


def log_and_telegram(msg: str):
    """Log a message and add it to Telegram's log buffer."""
    logger.info(msg)
    telegram_bot.add_log(msg)


def _check_for_challenges_and_alert(driver, username, context="during interaction") -> bool:
    """Check for challenges and send Telegram alerts if found."""
    challenge = detect_challenge(driver)
    if challenge == ChallengeType.NONE:
        return False
    
    log_and_telegram(f"[{username}] ⚠️ Challenge detected {context}: {challenge.value}")
    telegram_bot.send_challenge_alert(username, challenge.value, driver.current_url)
    
    if challenge == ChallengeType.LOCKED:
        telegram_bot.send_lockout_alert(username, f"Account locked {context}")
        _mark_account_suspended(username, f"locked {context}")
        
    return True


def _is_page_unavailable(driver) -> bool:
    """Detect 'Sorry, this page isn't available' Instagram error."""
    try:
        # 1. Check title (Instagram usually sets title to 'Page not found • Instagram' or just 'Instagram')
        title = str(driver.title or "").lower()
        if "page not found" in title or title == "instagram":
            return True

        # 2. Check for the specific error text in the page source
        # This covers the exact HTML snippet provided by the user
        source = driver.page_source.lower()
        error_markers = [
            "sorry, this page isn't available",
            "the link you followed may be broken",
            "page may have been removed",
        ]
        
        for marker in error_markers:
            if marker in source:
                return True
                
        # 3. Check for specific CSS classes or layout if text check is too broad
        # (Optional: can add specific selector checks here if needed)
        
    except Exception:
        pass
    return False


def _mark_account_suspended(username: str, reason: str = ""):
    """Persist account as suspended so future sessions skip it automatically."""
    clean_username = str(username or "").strip().lstrip("@")
    if not clean_username:
        return

    try:
        already_suspended = database.is_account_suspended(clean_username, default=False)
    except Exception:
        already_suspended = False

    try:
        database.set_account_suspended(clean_username, True)
    except Exception as e:
        logger.warning(f"Failed to mark @{clean_username} as suspended: {e}")
        return

    if not already_suspended:
        note = f" ({reason})" if str(reason or "").strip() else ""
        log_and_telegram(f"⛔ @{clean_username} moved to Suspended Accounts{note}.")


def _is_expected_driver_shutdown_error(err: Exception) -> bool:
    """Return True for common Selenium transport errors triggered by forced stop."""
    text = str(err or "").strip().lower()
    if not text:
        return False

    markers = (
        "httpconnectionpool(host='localhost'",
        "max retries exceeded with url: /session/",
        "newconnectionerror",
        "failed to establish a new connection",
        "winerror 10061",
        "connection refused",
        "invalid session id",
        "no such window",
        "target window already closed",
        "chrome not reachable",
        "not connected to devtools",
        "disconnected:",
        "connection aborted",
        "remote end closed connection",
    )
    return any(marker in text for marker in markers)


def _parse_iso_datetime(raw_value):
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


def _is_dm_summary_due(hours: int = DM_SUMMARY_WINDOW_HOURS) -> bool:
    safe_hours = max(1, int(hours or DM_SUMMARY_WINDOW_HOURS))
    last_sent_raw = database.get_setting("DM_24H_REPORT_LAST_SENT_AT", "")
    last_sent = _parse_iso_datetime(last_sent_raw)

    # First run initializes the timer; the first summary will be sent after the window elapses.
    if last_sent is None:
        database.save_settings({"DM_24H_REPORT_LAST_SENT_AT": datetime.now().isoformat(timespec="seconds")})
        return False

    return (datetime.now() - last_sent) >= timedelta(hours=safe_hours)


def _maybe_send_24h_dm_summary(hours: int = DM_SUMMARY_WINDOW_HOURS, force: bool = False) -> bool:
    safe_hours = max(1, int(hours or DM_SUMMARY_WINDOW_HOURS))

    try:
        if not force and not _is_dm_summary_due(safe_hours):
            return False

        summary = database.get_dm_sent_summary_last_hours(
            hours=safe_hours,
            include_all_accounts=True,
        )
        telegram_bot.send_24h_dm_summary(summary)
        database.save_settings({"DM_24H_REPORT_LAST_SENT_AT": datetime.now().isoformat(timespec="seconds")})

        logger.info(
            "24h DM summary sent to Telegram (window=%sh, total_sent=%s, lifetime_total_sent=%s)",
            safe_hours,
            summary.get("total_sent", 0),
            summary.get("lifetime_total_sent", 0),
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to send 24h DM summary: {e}")
        return False


def _normalize_model_key(model_username: str) -> str:
    """Normalize model usernames to a stable lookup key."""
    return str(model_username or "").strip().lstrip("@").lower()


def _normalize_account_model_label(raw_label: str) -> str:
    """Normalize an account model label; empty means generic account."""
    key = _normalize_model_key(raw_label)
    if key in ("", "generic", "any", "all", "*", "none"):
        return ""
    return key


def _cluster_control_snapshot() -> dict:
    raw = database.get_setting("BOT_CLUSTER_CONTROL", {})
    if not isinstance(raw, dict):
        return {}

    desired_state = str(raw.get("desired_state", "") or "").strip().lower()
    nonce = str(raw.get("nonce", "") or "").strip().lower()

    return {
        "desired_state": desired_state,
        "nonce": nonce,
    }


def _claim_cluster_notification(event_name: str, expected_state: str = "") -> bool:
    """Claim one cluster-wide notification slot using DB-backed dedupe."""
    clean_event_name = str(event_name or "").strip().lower()
    if not clean_event_name:
        return False

    control = _cluster_control_snapshot()
    desired_state = str(control.get("desired_state", "") or "").strip().lower()
    nonce = str(control.get("nonce", "") or "").strip().lower()

    if expected_state and desired_state != str(expected_state).strip().lower():
        return False

    if nonce:
        dedupe_key = f"telegram:{clean_event_name}:{nonce}"
        return database.claim_notification_event(dedupe_key, cooldown_seconds=CLUSTER_NOTIFICATION_COOLDOWN_SEC)

    # Fallback when cluster state has no nonce (e.g. legacy/manual start).
    time_bucket = int(time.time() // CLUSTER_NOTIFICATION_FALLBACK_BUCKET_SEC)
    dedupe_key = f"telegram:{clean_event_name}:fallback:{time_bucket}"
    return database.claim_notification_event(
        dedupe_key,
        cooldown_seconds=CLUSTER_NOTIFICATION_FALLBACK_BUCKET_SEC,
    )


def _account_label_meta(account: dict):
    """Return normalized label key and display name for an account row."""
    raw_label = str((account or {}).get("model_label", "")).strip().lstrip("@")
    label_key = _normalize_account_model_label(raw_label)
    if label_key:
        return label_key, (raw_label or label_key)
    return "", "Generic"


def _sort_accounts_for_label_batches(accounts: list) -> list:
    """Randomize account order per run while keeping same-label accounts contiguous."""
    rows = list(accounts or [])
    if not rows:
        return []

    grouped = {}
    for account in rows:
        label_key, _ = _account_label_meta(account)
        group_key = label_key or "generic"
        grouped.setdefault(group_key, []).append(account)

    group_keys = list(grouped.keys())
    random.shuffle(group_keys)

    randomized = []
    for group_key in group_keys:
        group_rows = list(grouped.get(group_key, []))
        random.shuffle(group_rows)
        randomized.extend(group_rows)

    return randomized


def _count_accounts_by_label(accounts: list) -> dict:
    """Return {label_key_or_generic: {display, count}} for active accounts."""
    counts = {}
    for account in accounts or []:
        label_key, label_display = _account_label_meta(account)
        key = label_key or "generic"
        if key not in counts:
            counts[key] = {"display": label_display, "count": 0}
        counts[key]["count"] += 1
    return counts


def _models_for_account(account: dict, all_models: list) -> list:
    """Return target models for an account.

    Account labels are campaign/model-owner tags, not target usernames,
    so they should not restrict which targets this account can process.
    """
    models = list(all_models or [])
    random.shuffle(models)
    return models


def _build_account_pool_summary(accounts: list, models: list) -> str:
    """Build Telegram text for per-label and generic account availability."""
    display_by_key = {}
    for model_name in models:
        key = _normalize_model_key(model_name)
        if key:
            display_by_key[key] = str(model_name or "").strip().lstrip("@")

    counts_by_model = {}
    generic_count = 0

    for account in accounts:
        label_raw = str(account.get("model_label", "")).strip().lstrip("@")
        label_key = _normalize_account_model_label(label_raw)
        if not label_key:
            generic_count += 1
            continue

        counts_by_model[label_key] = counts_by_model.get(label_key, 0) + 1
        if label_key not in display_by_key:
            display_by_key[label_key] = label_raw or label_key

    ordered_keys = sorted(counts_by_model.keys(), key=lambda k: display_by_key.get(k, k).lower())
    label_width = len("Generic")
    for key in ordered_keys:
        label_width = max(label_width, len(str(display_by_key.get(key, key))))

    lines = ["Model Labels:"]
    for key in ordered_keys:
        display_name = str(display_by_key.get(key, key)).strip() or key
        lines.append(f"{display_name.ljust(label_width)} : ({counts_by_model[key]}) IG Accounts Alive")
    lines.append(f"{'Generic'.ljust(label_width)} : ({generic_count}) IG Accounts Alive")
    return "\n".join(lines)


def _normalize_message_list(raw_messages) -> list:
    """Normalize a raw messages array into non-empty trimmed strings."""
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


def _normalize_account_proxy_candidates(raw_proxy, max_items: int = MAX_ACCOUNT_PROXIES) -> list:
    """Parse account proxy input into an ordered unique list (up to max_items)."""
    raw_text = str(raw_proxy or "")
    if not raw_text.strip():
        return []

    candidates = []
    seen = set()
    for part in re.split(r"[\r\n,;]+", raw_text):
        proxy = str(part or "").strip()
        if not proxy:
            continue

        key = proxy.lower()
        if key in seen:
            continue

        seen.add(key)
        candidates.append(proxy)
        if len(candidates) >= max_items:
            break

    return candidates


def _normalize_model_message_map(raw_map) -> dict:
    """Normalize MODEL_MESSAGE_MAP from settings into {model_key: [messages]} format."""
    if not isinstance(raw_map, dict):
        return {}

    normalized = {}
    for raw_model, raw_messages in raw_map.items():
        model_key = _normalize_model_key(raw_model)
        if not model_key:
            continue

        messages = _normalize_message_list(raw_messages)
        if messages:
            normalized[model_key] = messages

    return normalized


def _normalize_model_automation_map(raw_map) -> dict:
    """Normalize MODEL_AUTOMATION_MAP from settings into {model_key: bool} format."""
    if not isinstance(raw_map, dict):
        return {}

    normalized = {}
    for raw_model, raw_enabled in raw_map.items():
        model_key = _normalize_model_key(raw_model)
        if not model_key:
            continue

        if isinstance(raw_enabled, bool):
            normalized[model_key] = raw_enabled
            continue

        if raw_enabled is None:
            normalized[model_key] = True
            continue

        if isinstance(raw_enabled, (int, float)):
            normalized[model_key] = int(raw_enabled) != 0
            continue

        text = str(raw_enabled).strip().lower()
        if text in ("0", "false", "off", "no", "disable", "disabled"):
            normalized[model_key] = False
        elif text in ("", "none", "null"):
            normalized[model_key] = True
        else:
            normalized[model_key] = True

    return normalized


def _messages_for_model(model_username: str, default_messages: list, model_message_map: dict) -> list:
    """Return custom messages for a model when available, otherwise global defaults."""
    custom_messages = model_message_map.get(_normalize_model_key(model_username), [])
    return custom_messages if custom_messages else default_messages


def run_bot(
    stop_event=None,
    account_owner=None,
    continuous_mode: bool = False,
    runtime_mode: str = "dm",
):
    """Main bot orchestration loop.

    runtime_mode:
        - "dm": regular DM + inbox reply flow
        - "comment_liking": run comment-liking scraping only (no DMs, no inbox replies)
    """
    database.init_db()
    setup_logging()
    coordinator = None
    distributed_session_id = uuid4().hex

    normalized_runtime_mode = str(runtime_mode or "dm").strip().lower()
    runtime_title = "INSTAGRAM MODEL DM BOT"

    logger.info("=" * 60)
    logger.info(f"  {runtime_title} — STARTING")
    logger.info("=" * 60)

    # Load config from Database
    try:
        settings_cache = database.get_all_settings()

        if account_owner:
            accounts = database.get_accounts(owner_username=account_owner)
        else:
            accounts = database.get_accounts(include_all=True)

        models = database.get_models()
        messages = _normalize_message_list(database.get_messages())
        model_message_map = _normalize_model_message_map(
            settings_cache.get("MODEL_MESSAGE_MAP") or {}
        )
        model_automation_map = _normalize_model_automation_map(
            settings_cache.get("MODEL_AUTOMATION_MAP") or {}
        )
        coordinator = DistributedCoordinator.from_settings(
            settings=settings_cache,
            logger=logger,
            account_owner=account_owner or "",
        )

        # If explicit model list is empty, derive targets from model-specific sets.
        if not models and model_message_map:
            models = sorted(model_message_map.keys())

        disabled_models = []
        enabled_models = []
        for model_name in models:
            model_key = _normalize_model_key(model_name)
            if model_key and not bool(model_automation_map.get(model_key, True)):
                disabled_models.append(str(model_name or "").strip().lstrip("@") or model_key)
                continue
            enabled_models.append(model_name)
        models = enabled_models
    except Exception as e:
        logger.error(f"Failed to load config from database: {e}")
        if coordinator:
            coordinator.shutdown()
        return

    suspended_accounts = [
        acc for acc in accounts
        if bool(acc.get("is_suspended", False))
    ]
    disabled_accounts = [
        acc for acc in accounts
        if not bool(acc.get("automation_enabled", True)) and not bool(acc.get("is_suspended", False))
    ]
    accounts = [
        acc for acc in accounts
        if bool(acc.get("automation_enabled", True)) and not bool(acc.get("is_suspended", False))
    ]
    accounts = _sort_accounts_for_label_batches(accounts)
    label_batch_counts = _count_accounts_by_label(accounts)

    if not accounts:
        if account_owner:
            logger.error(f"No automation-enabled accounts configured for employee @{account_owner}")
        else:
            logger.error("No automation-enabled accounts configured")
        if coordinator:
            coordinator.shutdown()
        return
    if not models:
        logger.error("No automation-enabled models configured in database")
        if coordinator:
            coordinator.shutdown()
        return
    if not messages and not model_message_map:
        logger.error("No messages configured (general or model-specific)")
        if coordinator:
            coordinator.shutdown()
        return

    logger.info(
        f"Loaded {len(accounts)} active accounts, {len(models)} active models, "
        f"{len(messages)} general messages, {len(model_message_map)} model-specific sets"
    )
    if disabled_accounts:
        logger.info(f"Automation disabled for {len(disabled_accounts)} account(s)")
    if suspended_accounts:
        logger.info(f"Suspended for safety: {len(suspended_accounts)} account(s)")
    if disabled_models:
        preview = ", ".join(f"@{str(model or '').strip().lstrip('@')}" for model in disabled_models[:20])
        suffix = " ..." if len(disabled_models) > 20 else ""
        logger.info(f"Automation disabled for {len(disabled_models)} model target(s): {preview}{suffix}")
    if account_owner:
        logger.info(f"Account scope: employee @{account_owner}")
    if label_batch_counts:
        ordered_label_items = sorted(
            label_batch_counts.items(),
            key=lambda item: (1 if item[0] == "generic" else 0, str(item[1].get("display", "")).lower()),
        )
        label_preview = ", ".join(
            f"{item[1].get('display', 'Generic')}({int(item[1].get('count', 0))})"
            for item in ordered_label_items
        )
        logger.info(f"Label batch order: {label_preview}")
    if coordinator and coordinator.enabled:
        if coordinator.is_active:
            logger.info(
                "Distributed coordination active (instance=%s, namespace=%s)",
                coordinator.instance_id,
                coordinator.namespace,
            )
        else:
            mode_label = "fail-closed" if coordinator.fail_closed else "best-effort"
            logger.warning(
                "Distributed coordination is enabled but Redis is unavailable (%s mode)",
                mode_label,
            )

    use_global_target_dedupe = _setting_bool("GLOBAL_TARGET_DEDUP_ENABLED", default=False)

    # Build per-session DM exclusion set. In global mode this includes the last
    # 24h of cluster DM history; otherwise it is local to this bot process.
    dm_log = {}
    already_dmd = set()
    if use_global_target_dedupe:
        dm_log = database.get_dm_logs()
        cutoff_time = datetime.now() - timedelta(hours=24)

        for user_dmd, timestamp_str in dm_log.items():
            try:
                if not timestamp_str:
                    already_dmd.add(user_dmd)
                    continue

                # support fromisoformat compatibility
                safe_ts = timestamp_str.replace("Z", "+00:00")
                dmd_time = datetime.fromisoformat(safe_ts)
                if dmd_time > cutoff_time:
                    already_dmd.add(user_dmd)
            except (ValueError, TypeError):
                # Fallback for old/corrupted formats
                already_dmd.add(user_dmd)

        logger.info("Global target dedupe enabled (24h cross-VPS target suppression)")
    else:
        logger.info("Global target dedupe disabled (each VPS/account chases its own DM quota)")

    logger.info("Runtime mode active: standard model DM flow")

    # Start Telegram
    telegram_bot.start_polling()
    should_send_startup_bundle = _claim_cluster_notification(
        event_name="bot_start",
        expected_state="running",
    )
    if should_send_startup_bundle:
        telegram_bot.send_startup()
        telegram_bot.send_account_pool_summary(_build_account_pool_summary(accounts, models))
        telegram_bot.send_account_profile_summary(accounts, limit=3, recent_only=True)
    else:
        logger.info("Skipping duplicate cluster startup Telegram notifications on this VPS")
    if disabled_accounts:
        disabled_preview = ", ".join(
            f"@{str(acc.get('username', '')).strip().lstrip('@')}"
            for acc in disabled_accounts[:15]
            if str(acc.get("username", "")).strip()
        )
        suffix = " ..." if len(disabled_accounts) > 15 else ""
        log_and_telegram(
            f"👁️ Automation disabled for {len(disabled_accounts)} account(s): {disabled_preview}{suffix}"
        )
    if suspended_accounts:
        suspended_preview = ", ".join(
            f"@{str(acc.get('username', '')).strip().lstrip('@')}"
            for acc in suspended_accounts[:15]
            if str(acc.get("username", "")).strip()
        )
        suffix = " ..." if len(suspended_accounts) > 15 else ""
        log_and_telegram(
            f"⛔ Suspended accounts skipped for safety ({len(suspended_accounts)}): {suspended_preview}{suffix}"
        )
    _maybe_send_24h_dm_summary(hours=DM_SUMMARY_WINDOW_HOURS)
    telegram_bot.stats["status"] = "Running"
    telegram_bot.stats["current_account"] = "—"
    telegram_bot.stats["current_model"] = "—"

    total_dms_sent = 0
    completed_model_keys = set()
    session_account_dm_counts = {}
    active_label_key = None
    active_label_display = ""

    try:
        for account_index, account in enumerate(accounts):
            _maybe_send_24h_dm_summary(hours=DM_SUMMARY_WINDOW_HOURS)

            if stop_event and stop_event.is_set():
                scope_label = "pass" if continuous_mode else "session"
                log_and_telegram(f"🛑 Stop requested. Ending current {scope_label}.")
                break

            label_key, label_display = _account_label_meta(account)
            normalized_label_key = label_key or "generic"
            if normalized_label_key != active_label_key:
                if active_label_key is not None:
                    log_and_telegram(f"✅ Finished label batch: {active_label_display}")

                active_label_key = normalized_label_key
                active_label_display = label_display
                label_total = int((label_batch_counts.get(normalized_label_key) or {}).get("count", 0))
                log_and_telegram(
                    f"🏷️ Starting label batch: {active_label_display} ({label_total} account(s))"
                )

            username = account["username"]
            is_suspended_now = database.is_account_suspended(
                username,
                default=bool(account.get("is_suspended", False)),
            )
            if is_suspended_now:
                log_and_telegram(f"[{username}] ⛔ Account suspended, skipping account")
                continue

            is_enabled_now = database.is_account_automation_enabled(
                username,
                default=bool(account.get("automation_enabled", True)),
            )
            if not is_enabled_now:
                log_and_telegram(f"[{username}] 👁️‍🗨️ Automation disabled, skipping account")
                continue

            account_lock_acquired = False
            if coordinator and coordinator.enabled:
                owner_for_lock = (
                    str(account.get("owner_username", "")).strip().lower()
                    or str(account_owner or "").strip().lower()
                    or "master"
                )
                account_lock_acquired, lock_reason = coordinator.acquire_account_lock(
                    username=username,
                    owner_username=owner_for_lock,
                    session_id=distributed_session_id,
                )
                if not account_lock_acquired:
                    if lock_reason == "already_locked":
                        log_and_telegram(f"[{username}] ⏭️ Account locked by another VPS, skipping")
                    else:
                        log_and_telegram(
                            f"[{username}] ⚠️ Could not acquire distributed lock ({lock_reason}), skipping for safety"
                        )
                    continue

            account_model_key = _normalize_account_model_label(account.get("model_label", ""))
            account_models = _models_for_account(account, models)
            
            engagement_schedule = []
            if heavy_comment_liking_mode:
                account_models = ["home_feed"]
                
            account_custom_messages = _normalize_message_list(account.get("custom_messages"))
            account_label_display = label_display

            log_and_telegram(f"━━━ Switching to account: @{username} ━━━")
            if account_model_key:
                log_and_telegram(f"[{username}] 🏷️ Marketing label: {account_label_display}")
            else:
                log_and_telegram(f"[{username}] 🏷️ Marketing label: Generic")

            telegram_bot.stats["current_account"] = username
            telegram_bot.stats["accounts_used"] += 1

            account_dm_batch_state = {
                "sent_count": 0,
                "last_cooldown_count": 0,
                "sent_since_human_break": 0,
            }

            # Create browser + login with proxy failover (up to MAX_ACCOUNT_PROXIES)
            driver = None
            logged_in = False
            proxy_candidates = _normalize_account_proxy_candidates(account.get("proxy", ""))
            connection_candidates = list(proxy_candidates) if proxy_candidates else [None]

            if proxy_candidates:
                proxy_preview = ", ".join(_mask_proxy_for_log(proxy) for proxy in proxy_candidates)
                log_and_telegram(
                    f"[{username}] 🌐 Proxy pool loaded ({len(proxy_candidates)}/{MAX_ACCOUNT_PROXIES}): {proxy_preview}"
                )
            else:
                log_and_telegram(f"[{username}] 🌐 No proxy configured, using direct connection")

            for attempt_index, candidate_proxy in enumerate(connection_candidates, start=1):
                if stop_event and stop_event.is_set():
                    break

                try:
                    if candidate_proxy:
                        log_and_telegram(
                            f"[{username}] 🌐 Attempt {attempt_index}/{len(connection_candidates)} with proxy: "
                            f"{_mask_proxy_for_log(candidate_proxy)}"
                        )
                    else:
                        log_and_telegram(
                            f"[{username}] 🌐 Attempt {attempt_index}/{len(connection_candidates)} with direct connection"
                        )

                    driver = create_driver(headless=False, proxy=candidate_proxy)
                    _register_driver(driver)
                except Exception as e:
                    error_text = str(e).strip() or repr(e)
                    log_and_telegram(
                        f"❌ Failed to create browser for @{username} on attempt "
                        f"{attempt_index}/{len(connection_candidates)}: {error_text}"
                    )
                    if attempt_index == len(connection_candidates):
                        log_and_telegram(
                            "⚠️ Browser bootstrap failed. Auto ChromeDriver download may be blocked on this VPS."
                        )
                        log_and_telegram(
                            "💡 Tip: install a matching ChromeDriver binary and set CHROMEDRIVER_PATH for this host."
                        )
                    continue

                try:
                    logged_in = _perform_login(driver, account)
                except Exception as e:
                    logged_in = False
                    log_and_telegram(
                        f"❌ Login error for @{username} on attempt "
                        f"{attempt_index}/{len(connection_candidates)}: {e}"
                    )

                if logged_in:
                    break

                log_and_telegram(
                    f"⚠️ Login failed for @{username} on attempt "
                    f"{attempt_index}/{len(connection_candidates)}"
                )
                close_driver(driver)
                _unregister_driver(driver)
                driver = None

            if not logged_in or not driver:
                log_and_telegram(
                    f"❌ Failed to login @{username} after trying {len(connection_candidates)} connection option(s), skipping"
                )
                if account_lock_acquired and coordinator:
                    coordinator.release_account_lock(username)
                continue


            try:
                # ── Sequential Model Loop (DM mode) ──
                for model_username in account_models:
                    _maybe_send_24h_dm_summary(hours=DM_SUMMARY_WINDOW_HOURS)

                    if stop_event and stop_event.is_set():
                        log_and_telegram("🛑 Stop requested, breaking model loop.")
                        break

                    if coordinator and coordinator.enabled and not coordinator.has_account_lock(username):
                        log_and_telegram(f"[{username}] ⚠️ Distributed lock lost, stopping account session")
                        break

                    if not telegram_bot._polling:
                        log_and_telegram("🛑 Stop requested, finishing up...")
                        break

                    log_and_telegram(f"🎯 Targeting model: @{model_username}")
                    telegram_bot.stats["current_model"] = model_username

                    model_key = _normalize_model_key(model_username) or str(model_username or "").strip().lower()

                    # Default DM/Model flow
                    custom_messages = model_message_map.get(_normalize_model_key(model_username), [])
                    if account_model_key and account_custom_messages:
                        messages_for_model = account_custom_messages
                        log_and_telegram(
                            f"[{username}] Using {len(messages_for_model)} account custom messages for @{model_username}"
                        )
                    elif account_model_key:
                        messages_for_model = custom_messages if custom_messages else messages
                        if not messages_for_model:
                            log_and_telegram(
                                f"[{username}] ⚠️ No generic messages configured for @{model_username}, skipping"
                            )
                            continue
                    else:
                        messages_for_model = custom_messages if custom_messages else messages
                        if not messages_for_model:
                            log_and_telegram(f"[{username}] ⚠️ No messages configured for @{model_username}, skipping")
                            continue

                    dms_for_model = _process_model(
                        driver,
                        account,
                        model_username,
                        messages_for_model,
                        dm_log,
                        already_dmd,
                        dm_batch_state=account_dm_batch_state,
                        stop_event=stop_event,
                        coordinator=coordinator,
                        use_global_target_dedupe=use_global_target_dedupe,
                    )

                    total_dms_sent += dms_for_model
                    telegram_bot.stats["dms_sent"] = total_dms_sent

                    if dms_for_model > 0:
                        session_account_dm_counts[username] = int(session_account_dm_counts.get(username, 0)) + int(dms_for_model)
                        if model_key:
                            completed_model_keys.add(model_key)
                        telegram_bot.stats["models_processed"] = len(completed_model_keys)
                        telegram_bot.send_model_complete(model_username, dms_for_model, sender_account=username)

                    # Check if still logged in
                    if not is_logged_in(driver):
                        log_and_telegram(f"⚠️ Lost login for @{username} during model processing")
                        break

                    # Delay before next model
                    model_delay_min = _setting_float("MODEL_SWITCH_DELAY_MIN")
                    model_delay_max = _setting_float("MODEL_SWITCH_DELAY_MAX")
                    if model_delay_max < model_delay_min:
                        model_delay_min, model_delay_max = model_delay_max, model_delay_min

                    delay = random.uniform(model_delay_min, model_delay_max)
                    log_and_telegram(f"⏳ Waiting {delay:.0f}s before next model...")
                    if _interruptible_sleep(delay, stop_event=stop_event):
                        break

                if not (stop_event and stop_event.is_set()):
                    if is_logged_in(driver):
                        queued_replies_ready = has_queued_reply_for_account(username)
                        unread_result = _auto_sync_unread_inbox_for_account(
                            driver,
                            account,
                            stop_event=stop_event,
                            prefetch_recent_messages=queued_replies_ready,
                        )
                        unread_count = int(unread_result.get("threads") or 0)

                        if not queued_replies_ready:
                            queued_replies_ready = has_queued_reply_for_account(username)

                        if queued_replies_ready:
                            _process_queued_replies_for_account(
                                driver,
                                account,
                                stop_event=stop_event,
                            )
                        elif unread_count <= 0:
                            log_and_telegram(
                                f"[{username}] 📭 No unread inbox messages and no queued replies. Moving to next account."
                            )
                        else:
                            log_and_telegram(
                                f"[{username}] 📥 Unread inbox indexed, but no queued replies available. Moving to next account."
                            )
                    else:
                        log_and_telegram(f"[{username}] ⚠️ Skipping inbox/reply cycle: login session not active.")

                # Refresh cookies after session
                refresh_cookies(driver, username)

            except Exception as e:
                if stop_event and stop_event.is_set() and _is_expected_driver_shutdown_error(e):
                    log_and_telegram(f"🛑 Stop requested while closing @{username} browser session")
                else:
                    log_and_telegram(f"❌ Error with @{username}: {e}")
                    telegram_bot.send_error(str(e))
            finally:
                close_driver(driver)
                _unregister_driver(driver)
                if account_lock_acquired and coordinator:
                    coordinator.release_account_lock(username)

            # Delay before switching accounts
            if account_index < len(accounts) - 1 and not (stop_event and stop_event.is_set()):
                account_delay_min = _setting_float("ACCOUNT_SWITCH_DELAY_MIN")
                account_delay_max = _setting_float("ACCOUNT_SWITCH_DELAY_MAX")
                if account_delay_max < account_delay_min:
                    account_delay_min, account_delay_max = account_delay_max, account_delay_min

                delay = random.uniform(account_delay_min, account_delay_max)
                log_and_telegram(f"⏳ Waiting {delay:.0f}s before switching accounts...")
                if _interruptible_sleep(delay, stop_event=stop_event):
                    break

        if active_label_key is not None and not (stop_event and stop_event.is_set()):
            log_and_telegram(f"✅ Finished label batch: {active_label_display}")

    except KeyboardInterrupt:
        log_and_telegram("🛑 Bot stopped by user (Ctrl+C)")
    except Exception as e:
        if stop_event and stop_event.is_set() and _is_expected_driver_shutdown_error(e):
            log_and_telegram("🛑 Stop requested. Browser connections were terminated.")
        else:
            log_and_telegram(f"❌ Fatal error: {e}")
            telegram_bot.send_error(str(e))
    finally:
        if coordinator:
            try:
                coordinator.shutdown()
            except Exception as e:
                logger.debug(f"Failed to shutdown distributed coordinator cleanly: {e}")

        _maybe_send_24h_dm_summary(hours=DM_SUMMARY_WINDOW_HOURS)
        should_send_session_complete = not continuous_mode
        if stop_event and stop_event.is_set():
            should_send_stop_notice = _claim_cluster_notification(
                event_name="bot_stop",
                expected_state="stopped",
            )
            if continuous_mode:
                if should_send_stop_notice:
                    telegram_bot.send("🛑 *BOT STOPPED*")
                else:
                    logger.info("Skipping duplicate cluster stop Telegram notification on this VPS")
            else:
                should_send_session_complete = should_send_stop_notice

        if should_send_session_complete:
            telegram_bot.send_session_complete(
                total_dms_sent,
                len(completed_model_keys),
                by_account=session_account_dm_counts,
            )
        elif stop_event and stop_event.is_set() and not continuous_mode:
            logger.info("Skipping duplicate cluster stop Telegram notification on this VPS")

        telegram_bot.stats["current_account"] = "—"
        telegram_bot.stats["current_model"] = "—"
        telegram_bot.stats["status"] = "Stopped"

    logger.info("=" * 60)
    completion_label = "PASS COMPLETE" if continuous_mode else "SESSION COMPLETE"
    logger.info(f"  {completion_label} — {total_dms_sent} DMs sent, {len(completed_model_keys)} unique models done")
    logger.info("=" * 60)


def _perform_login(driver, account: dict) -> bool:
    """
    Attempt login: cookies first, then credentials, handle challenges.
    """
    username = account["username"]

    # Try cookie login
    if login_with_cookies(driver, account):
        return True

    # Check if cookie login failed because it hit a challenge
    challenge = detect_challenge(driver)
    if challenge != ChallengeType.NONE:
        logger.warning(f"[{username}] Challenge detected after cookie injection, skipping credential login.")
    else:
        # Only try credential login if no challenge is blocking us
        if login_with_credentials(driver, account):
            return True
        
    # Final check for challenges (from either cookie or credential login)
    challenge = detect_challenge(driver)

    if challenge == ChallengeType.NONE:
        return False

    log_and_telegram(f"🔒 Challenge for @{username}: {challenge.value}")
    telegram_bot.send_challenge_alert(username, challenge.value, driver.current_url)
    if challenge == ChallengeType.LOCKED:
        telegram_bot.send_lockout_alert(username, "Account locked during login")
        _mark_account_suspended(username, "locked during login")
        log_and_telegram(f"⏭️ @{username} is locked. Skipping account automatically.")
        return False

    if challenge in (ChallengeType.TWO_FACTOR, ChallengeType.SUSPICIOUS_LOGIN, ChallengeType.CHECKPOINT):
        log_and_telegram(
            f"⏭️ @{username} challenge ({challenge.value}) is auto-skipped. Continuing with next account."
        )
        return False

    return False


def _normalize_inbox_thread_payload(raw_thread: dict) -> dict:
    """Normalize inbox thread rows into the payload expected by save_inbox_threads."""
    if not isinstance(raw_thread, dict):
        return {}

    display_name = str(raw_thread.get("display_name", "")).strip()
    if not display_name:
        return {}

    return {
        "display_name": display_name,
        "username": str(raw_thread.get("username", "")).strip(),
        "message_preview": str(raw_thread.get("message_preview", "")).strip(),
        "timestamp": str(raw_thread.get("timestamp", "")).strip(),
        "timestamp_label": str(raw_thread.get("timestamp_label", "")).strip(),
        "profile_pic_url": str(raw_thread.get("profile_pic_url", "")).strip(),
        "is_unread": bool(raw_thread.get("is_unread", False)),
    }


def _merge_inbox_cache_with_unread_threads(existing_threads: list, unread_threads: list) -> list:
    """Merge unread scrape snapshots into existing cache without dropping read threads."""
    merged_by_key = {}
    existing_order = []

    for row in existing_threads or []:
        payload = _normalize_inbox_thread_payload(row)
        if not payload:
            continue
        display_key = str(payload.get("display_name", "")).strip().lower()
        if not display_key:
            continue

        if display_key not in merged_by_key:
            existing_order.append(display_key)
        merged_by_key[display_key] = payload

    unread_order = []
    for row in unread_threads or []:
        payload = _normalize_inbox_thread_payload(row)
        if not payload:
            continue
        payload["is_unread"] = True

        display_key = str(payload.get("display_name", "")).strip().lower()
        if not display_key:
            continue

        if display_key not in unread_order:
            unread_order.append(display_key)

        merged_by_key[display_key] = payload
        if display_key not in existing_order:
            existing_order.append(display_key)

    final_order = unread_order + [key for key in existing_order if key not in unread_order]
    return [merged_by_key[key] for key in final_order if key in merged_by_key]


def _auto_sync_unread_inbox_for_account(
    driver,
    account: dict,
    stop_event=None,
    prefetch_recent_messages: bool = False,
) -> dict:
    """Scrape unread inbox threads for one account and cache them for dashboard UI."""
    from core.inbox_reader import scrape_inbox_threads, scrape_thread_messages

    username = str((account or {}).get("username", "")).strip().lower()
    if not username:
        return {"success": False, "error": "missing_username", "threads": 0, "prefetched": 0}

    if stop_event and stop_event.is_set():
        return {"success": False, "error": "stopped", "threads": 0, "prefetched": 0}

    log_and_telegram(f"[{username}] 📥 Auto-syncing unread inbox threads...")

    unread_threads = scrape_inbox_threads(
        driver,
        max_threads=0,
        unread_only=True,
        recent_hours=1,
    )
    unread_count = len(unread_threads)
    if unread_count <= 0:
        log_and_telegram(f"[{username}] 📥 Inbox auto-sync complete: no unread chats.")
        return {"success": True, "error": "", "threads": 0, "prefetched": 0}

    existing_threads = database.get_inbox_threads(ig_account=username, unread_only=False)
    merged_threads = _merge_inbox_cache_with_unread_threads(existing_threads, unread_threads)
    if merged_threads:
        database.save_inbox_threads(username, merged_threads)

    if not prefetch_recent_messages:
        log_and_telegram(
            f"[{username}] 📥 Inbox auto-sync complete: {unread_count} unread thread(s) indexed. No queued replies yet."
        )
        return {"success": True, "error": "", "threads": unread_count, "prefetched": 0}

    cached_threads = database.get_inbox_threads(ig_account=username, unread_only=False)
    id_by_display = {}
    for thread_row in cached_threads:
        display_key = str(thread_row.get("display_name", "")).strip().lower()
        if not display_key or display_key in id_by_display:
            continue
        try:
            id_by_display[display_key] = int(thread_row.get("id") or 0)
        except Exception:
            continue

    prefetched = 0
    for thread in unread_threads:
        if stop_event and stop_event.is_set():
            break

        display_name = str(thread.get("display_name", "")).strip()
        if not display_name:
            continue

        thread_id = id_by_display.get(display_name.lower())
        if not thread_id:
            continue

        try:
            scrape_thread_messages(
                driver,
                display_name,
                max_messages=0,
                thread_id=thread_id,
                recent_hours=1,
            )
            prefetched += 1
        except Exception as cache_error:
            logger.debug(f"[{username}] Inbox auto-sync failed for '{display_name}': {cache_error}")

    log_and_telegram(
        f"[{username}] 📥 Inbox auto-sync complete: {unread_count} unread thread(s), {prefetched} cached."
    )
    return {"success": True, "error": "", "threads": unread_count, "prefetched": prefetched}


def _process_queued_replies_for_account(driver, account: dict, stop_event=None) -> dict:
    """Send queued GUI replies for one account using the active bot browser session."""
    from core.inbox_reader import reply_to_thread, scrape_thread_messages

    username = str((account or {}).get("username", "")).strip().lower()
    if not username:
        return {"processed": 0, "sent": 0, "failed": 0}

    if not _setting_bool(INBOX_REPLIER_ENABLED_SETTING_KEY, default=False):
        return {"processed": 0, "sent": 0, "failed": 0}

    processed = 0
    sent = 0
    failed = 0

    while True:
        if stop_event and stop_event.is_set():
            break

        if not _setting_bool(INBOX_REPLIER_ENABLED_SETTING_KEY, default=False):
            break

        job = claim_next_reply_for_account(username)
        if not job:
            break

        processed += 1

        job_id = str(job.get("job_id", "")).strip()
        thread_id = int(job.get("thread_id") or 0)
        thread_display_name = str(job.get("thread_display_name", "")).strip()
        text_message = str(job.get("text_message", "")).strip()
        upload_path = str(job.get("upload_path", "")).strip()
        has_attachment = bool(job.get("has_attachment", False))

        if not thread_display_name and thread_id > 0:
            thread_row = database.get_inbox_thread_by_id(thread_id)
            if thread_row and str(thread_row.get("ig_account", "")).strip().lower() == username:
                thread_display_name = str(thread_row.get("display_name", "")).strip()

        if not thread_display_name:
            update_reply_job(
                job_id,
                status="error",
                error="Missing thread display name for queued reply",
            )
            _safe_remove_file(upload_path)
            failed += 1
            continue

        update_reply_job(job_id, progress=f"Sending reply to {thread_display_name}...")

        try:
            send_result = reply_to_thread(
                driver,
                thread_display_name=thread_display_name,
                text=text_message,
                image_path=upload_path,
            )

            if not bool(send_result.get("success", False)):
                update_reply_job(
                    job_id,
                    status="error",
                    error=str(send_result.get("error", "Reply failed")).strip() or "Reply failed",
                )
                failed += 1
                continue

            if thread_id <= 0:
                cached_threads = database.get_inbox_threads(ig_account=username, unread_only=False)
                display_key = thread_display_name.lower()
                for row in cached_threads:
                    if str(row.get("display_name", "")).strip().lower() == display_key:
                        try:
                            thread_id = int(row.get("id") or 0)
                        except Exception:
                            thread_id = 0
                        break

            preview_text = text_message if text_message else ("[Attachment]" if has_attachment else "")
            now_str = datetime.now().isoformat(timespec="seconds")

            if thread_id > 0:
                database.append_thread_message(
                    thread_id=thread_id,
                    sender_name="You",
                    text_content=preview_text,
                    has_attachment=has_attachment,
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

                try:
                    scrape_thread_messages(
                        driver,
                        thread_display_name,
                        max_messages=0,
                        thread_id=thread_id,
                    )
                except Exception as refresh_error:
                    logger.debug(
                        f"[{username}] Could not refresh queued reply thread '{thread_display_name}': {refresh_error}"
                    )

            update_reply_job(job_id, status="done", progress="Reply sent")
            sent += 1

        except Exception as e:
            update_reply_job(job_id, status="error", error=str(e)[:500])
            failed += 1
        finally:
            _safe_remove_file(upload_path)

    if processed > 0:
        log_and_telegram(
            f"[{username}] ✉️ Processed queued replies: {sent} sent, {failed} failed."
        )

    return {"processed": processed, "sent": sent, "failed": failed}


def _process_model(
    driver, account: dict, model_username: str,
    messages: list, dm_log: dict, already_dmd: set,
    dm_batch_state: dict = None,
    stop_event=None,
    coordinator=None,
    use_global_target_dedupe: bool = False,
) -> int:
    """
    Process a single model target:
    1. Get recent posts
    2. Sort by age (< 4hr first)
    3. DM post interactors (likers/commenters)
    4. If quota not met, DM followers
    
    Returns number of DMs successfully sent.
    """
    username = account["username"]
    dm_min = _setting_int("DM_MIN_PER_MODEL")
    dm_max = _setting_int("DM_MAX_PER_MODEL")
    if dm_max < dm_min:
        dm_min, dm_max = dm_max, dm_min
    dm_target = random.randint(dm_min, dm_max)
    dms_sent = 0

    log_and_telegram(f"[{username}] Target: send {dm_target} DMs for @{model_username}")

    # Step 1: Get recent posts
    posts = get_recent_posts(driver, model_username)
    

    if not posts:
        if _is_page_unavailable(driver):
            log_and_telegram(f"[{username}] ⚠️ Profile @{model_username} is unavailable (deleted/broken). Skipping.")
            return 0

        log_and_telegram(f"[{username}] No posts found for @{model_username}, going to followers")
        # Skip to followers
        followers = get_followers(driver, model_username, already_dmd, max_count=dm_target)
        dms_sent += _dm_list(
            driver,
            followers,
            messages,
            dm_log,
            already_dmd,
            dm_target,
            username,
            model_username,
            dm_batch_state=dm_batch_state,
            coordinator=coordinator,
            use_global_target_dedupe=use_global_target_dedupe,
        )
        return dms_sent

    # Step 2: Sort posts by age priority
    sorted_posts = sort_posts_by_priority(posts, driver)
    
    # We want to dedicate at least 50% of DMs to followers, so cap post DMs
    post_dm_target = max(1, dm_target // 2)
    post_dms_sent = 0

    # Step 3: DM post interactors
    for post in sorted_posts:
        if stop_event and stop_event.is_set():
            break

        if dms_sent >= dm_target or post_dms_sent >= post_dm_target:
            break

        age_label = f"{post['age_hours']}h" if post['age_hours'] < 999 else "unknown"

        # Skip posts older than 24 hours
        post_age_limit = _setting_int("POST_AGE_PRIORITY_HOURS")
        if post['age_hours'] > post_age_limit:
            log_and_telegram(f"[{username}] ⏭️ Skipping post ({age_label} old) — too old: {post['url'][-20:]}")
            continue

        log_and_telegram(f"[{username}] Scraping post ({age_label} old): {post['url'][-20:]}")

        interactors = get_post_interactors(driver, post["url"], already_dmd, model_username)

        if interactors:
            remaining_for_posts = post_dm_target - post_dms_sent
            remaining = min(remaining_for_posts, dm_target - dms_sent)
            
            targets = interactors[:remaining]
            sent = _dm_list(
                driver,
                targets,
                messages,
                dm_log,
                already_dmd,
                remaining,
                username,
                model_username,
                dm_batch_state=dm_batch_state,
                stop_event=stop_event,
                coordinator=coordinator,
                use_global_target_dedupe=use_global_target_dedupe,
            )
            dms_sent += sent
            post_dms_sent += sent

            # Progress update
            telegram_bot.send_progress(username, model_username, dms_sent, dm_target)

    # Step 4: If still under quota, DM followers
    if dms_sent < dm_target:
        remaining = dm_target - dms_sent
        log_and_telegram(f"[{username}] Need {remaining} more DMs, switching to followers of @{model_username}")

        followers = get_followers(driver, model_username, already_dmd, max_count=remaining)
        if followers:
            sent = _dm_list(
                driver,
                followers,
                messages,
                dm_log,
                already_dmd,
                remaining,
                username,
                model_username,
                dm_batch_state=dm_batch_state,
                stop_event=stop_event,
                coordinator=coordinator,
                use_global_target_dedupe=use_global_target_dedupe,
            )
            dms_sent += sent

    log_and_telegram(f"[{username}] ✅ Completed @{model_username}: {dms_sent}/{dm_target} DMs sent")
    return dms_sent


def _build_engagement_schedule(models: list) -> list:
    """Build a randomized engagement schedule focusing only on model targets.

    This version removes home feed sessions and idle browsing as requested,
    focusing entirely on model profile visits.
    """
    if not models:
        # If no models, we still need something to do, but user requested no home feed.
        # We'll return an empty list or a log warning.
        return []

    schedule = []

    # Each model gets multiple short visits
    model_visits = []
    for model in models:
        num_visits = random.randint(2, 4)
        for _ in range(num_visits):
            model_visits.append(model)

    random.shuffle(model_visits)

    for model in model_visits:
        schedule.append(("model", model))

    # De-duplicate consecutive visits to the same model
    deduped = []
    for action in schedule:
        if (
            action[0] == "model"
            and deduped
            and deduped[-1][0] == "model"
            and deduped[-1][1] == action[1]
        ):
            continue # Skip consecutive visits to the same model
        deduped.append(action)

    return deduped


def _idle_browse_home_feed(driver, username: str, stop_event=None):
    """Simulate idle browsing — scroll the feed without interacting.

    This mimics a human passively scrolling through their feed,
    reading posts without liking or commenting.
    """
    scroll_count = random.randint(3, 7)
    log_and_telegram(f"[{username}] 📱 Browsing feed...")

    driver.get(f"{INSTAGRAM_BASE_URL}/")
    human_delay(2, 4)
    _dismiss_home_feed_dialogs(driver)

    for _ in range(scroll_count):
        if stop_event and stop_event.is_set():
            break
        _scroll_home_feed(driver)
        # Simulate reading time — longer pauses than active engagement
        if _interruptible_sleep(random.uniform(2.0, 5.0), stop_event=stop_event):
            break
        
        # Check for lockout during browsing
        if _check_for_challenges_and_alert(driver, username, context="during idle browsing"):
            break

    log_and_telegram(f"[{username}] 📱 Finished browsing ({scroll_count} scrolls)")


def _process_comment_liking_model(
    driver,
    account: dict,
    model_username: str,
    already_dmd: set,
    stop_event=None,
    max_posts: int = None,
) -> int:
    """Run comment-liking flow on the Instagram home feed without sending DMs."""
    username = account["username"]

    # Bypass global toggle for internal human-like breaks or model warm-ups (max_posts < 5)
    is_internal_safety = (model_username == "human_break") or (max_posts is not None and max_posts < 5)

    if not _setting_bool("COMMENT_LIKING_ENABLED", default=False) and not is_internal_safety:
        log_and_telegram(
            f"[{username}] ⚠️ COMMENT_LIKING_ENABLED is OFF. Skipping comment-liking run for @{model_username}."
        )
        return 0

    comment_pool = database.get_comments()
    if not comment_pool:
        comment_pool = [
            "Nice post!",
            "Love this!",
            "Great shot!",
            "Amazing vibe!",
            "So good!",
            "This looks awesome!",
        ]

    target = max_posts if max_posts and max_posts > 0 else 50

    log_and_telegram(
        f"[{username}] 🏠 Comment-liking: targeting {target} feed posts (like + comment each)"
    )

    driver.get(f"{INSTAGRAM_BASE_URL}/")
    human_delay(3, 5)
    _dismiss_home_feed_dialogs(driver)

    processed = 0
    liked_posts = 0
    commented_posts = 0
    seen_post_urls = set()
    consecutive_empty_scrolls = 0

    while processed < target:
        if stop_event and stop_event.is_set():
            break

        # Safety: Ensure we are not stuck on a post page or in a modal
        try:
            curr_url = str(driver.current_url or "")
            if ("/p/" in curr_url or "/reel/" in curr_url) and _find_post_dialog_container(driver) is None:
                log_and_telegram(f"[{username}] ⚠️ Stuck on post page. Forcing return to feed.")
                driver.get(f"{INSTAGRAM_BASE_URL}/")
                human_delay(3, 5)
                _dismiss_home_feed_dialogs(driver)
        except Exception:
            pass

        feed_posts = driver.find_elements(By.XPATH, "//article")
        found_new = False

        for post_element in feed_posts:
            if stop_event and stop_event.is_set():
                break
            if processed >= target:
                break

            post_url = _extract_home_feed_post_url(post_element)
            if not post_url or post_url in seen_post_urls:
                continue

            # Scroll to it to avoid "top of page" jumps
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", post_element)
                human_delay(0.8, 1.5)
            except Exception:
                pass

            seen_post_urls.add(post_url)
            found_new = True
            processed += 1

            opened_post_view = False
            try:
                opened_post_view = _open_comment_popup_from_feed(driver, post_element)
                if not opened_post_view:
                    logger.debug(f"[{username}] Could not open post popup from Comment icon, skipping post")
                    continue

                interaction_scope = _resolve_post_interaction_scope(driver, post_element)

                # Check if already liked — skip entire post if so
                already_liked = _scope_has_svg_label(interaction_scope, "Unlike")
                if not already_liked:
                    already_liked = _dialog_has_svg_label(driver, "Unlike")

                if already_liked:
                    logger.debug(f"[{username}] Post {processed}/{target} already liked, skipping")
                else:
                    # Randomize actions for more human-like behavior
                    # 80% chance to like, 40% chance to comment
                    do_like = random.random() < 0.8
                    do_comment = random.random() < 0.4
                    
                    if not do_like and not do_comment:
                        # Just "read" it for a moment
                        human_delay(2.0, 5.0)

                    # 1. Like the post
                    if do_like:
                        liked_now = _like_home_feed_post(driver, interaction_scope)
                        if liked_now:
                            liked_posts += 1
                            telegram_bot.stats["likes_sent"] += 1
                            # Attempt to log to DB
                            try:
                                owner = _extract_post_owner_username(post_element) or "feed_post"
                                database.log_engagement(username, owner, 'like')
                            except Exception:
                                pass
                            log_and_telegram(f"[{username}] ❤️ Liked feed post ({liked_posts}/{target})")
                            if _sleep_after_comment_like_action(stop_event=stop_event):
                                break
                        else:
                            human_delay(0.5, 1.0)

                    # 2. Comment on the post
                    if do_comment and comment_pool:
                        comment_text = random.choice(comment_pool)
                        commented_now = _comment_on_home_feed_post(
                            driver,
                            interaction_scope,
                            comment_text,
                            open_comment_section=False,
                        )
                        if commented_now:
                            commented_posts += 1
                            telegram_bot.stats["comments_sent"] += 1
                            # Attempt to log to DB
                            try:
                                owner = _extract_post_owner_username(post_element) or "feed_post"
                                database.log_engagement(username, owner, 'comment')
                            except Exception:
                                pass
                            log_and_telegram(
                                f"[{username}] 💬 Commented on feed post ({commented_posts}/{target})"
                            )
                            if _sleep_after_comment_like_action(stop_event=stop_event):
                                break
                        else:
                            human_delay(0.5, 1.0)

            except Exception as post_error:
                logger.debug(f"[{username}] Feed interaction failed: {post_error}")
            finally:
                should_close_post_view = opened_post_view
                if not should_close_post_view:
                    try:
                        current_url = str(driver.current_url or "")
                    except Exception:
                        current_url = ""

                    if "/p/" in current_url or "/reel/" in current_url:
                        should_close_post_view = True
                    elif _find_post_dialog_container(driver) is not None:
                        should_close_post_view = True

                if should_close_post_view:
                    _close_post_view_and_return_feed(driver)
                
                # Check for lockout after interaction
                if _check_for_challenges_and_alert(driver, username, context="during home feed engagement"):
                    return liked_posts + commented_posts

                if _interruptible_sleep(random.uniform(0.4, 0.9), stop_event=stop_event):
                    break

            if _interruptible_sleep(random.uniform(0.4, 0.9), stop_event=stop_event):
                break

        if stop_event and stop_event.is_set():
            break
        if processed >= target:
            break

        _scroll_home_feed(driver)
        if _interruptible_sleep(random.uniform(1.0, 1.8), stop_event=stop_event):
            break

        if found_new:
            consecutive_empty_scrolls = 0
        else:
            consecutive_empty_scrolls += 1
            if consecutive_empty_scrolls >= 6:
                log_and_telegram(f"[{username}] ⚠️ No new posts after 6 scrolls, stopping early")
                break

    log_and_telegram(
        f"[{username}] ✅ Comment-liking complete: processed {processed}/{target}, "
        f"liked {liked_posts}, commented {commented_posts}"
    )
    return liked_posts + commented_posts


def _process_heavy_comment_liking(
    driver,
    account: dict,
    stop_event=None,
) -> int:
    """Run heavy comment-liking: like + comment on 180-200 feed posts in sequence."""
    username = account["username"]

    default_comment_pool = [
        "Nice post!",
        "Love this!",
        "Great shot!",
        "Amazing vibe!",
        "So good!",
        "This looks awesome!",
        "🔥🔥🔥",
        "Wow!",
    ]
    comment_pool = _setting_text_list("COMMENT_LIKING_COMMENT_POOL", default_comment_pool)

    target_posts = random.randint(HEAVY_COMMENT_LIKING_MIN_POSTS, HEAVY_COMMENT_LIKING_MAX_POSTS)

    log_and_telegram(
        f"[{username}] 🏠 Heavy comment-liking: targeting {target_posts} feed posts (like + comment each)"
    )

    driver.get(f"{INSTAGRAM_BASE_URL}/")
    human_delay(3, 5)
    _dismiss_home_feed_dialogs(driver)

    processed_posts = 0
    liked_posts = 0
    commented_posts = 0
    seen_post_urls = set()
    consecutive_empty_scrolls = 0

    while processed_posts < target_posts:
        if stop_event and stop_event.is_set():
            break

        feed_posts = driver.find_elements(By.XPATH, "//article")
        found_new = False

        for post_element in feed_posts:
            if stop_event and stop_event.is_set():
                break
            if processed_posts >= target_posts:
                break

            post_url = _extract_home_feed_post_url(post_element)
            if not post_url or post_url in seen_post_urls:
                continue

            # Scroll to it
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", post_element)
                human_delay(0.8, 1.5)
            except Exception:
                pass

            seen_post_urls.add(post_url)
            found_new = True
            processed_posts += 1

            try:
                # Step 1: Click Comment to open popup/post view
                opened_post_view = _heavy_click_comment_button(driver, post_element)
                if not opened_post_view:
                    human_delay(0.8, 1.5)
                    continue

                interaction_scope = _resolve_post_interaction_scope(driver, post_element)

                # Step 2: Click Like inside popup/post view
                liked = _like_home_feed_post(driver, interaction_scope)
                if liked:
                    liked_posts += 1
                    if _sleep_after_comment_like_action(stop_event=stop_event):
                        break
                else:
                    human_delay(0.8, 1.5)

                # Step 3: Type comment in textarea and submit with Enter
                comment_text = random.choice(comment_pool)
                commented = _heavy_type_and_post_comment(driver, interaction_scope, comment_text)
                if commented:
                    commented_posts += 1
                    if _sleep_after_comment_like_action(stop_event=stop_event):
                        break
                else:
                    human_delay(0.8, 1.5)

                # Step 4: Close the popup/modal
                closed_popup = _heavy_click_close_popup(driver)
                if not closed_popup:
                    try:
                        current_url = str(driver.current_url or "")
                    except Exception:
                        current_url = ""

                    if _find_post_dialog_container(driver) is not None or "/p/" in current_url or "/reel/" in current_url:
                        _close_post_view_and_return_feed(driver)
                human_delay(0.5, 1.0)

                # Check for lockout after interaction
                if _check_for_challenges_and_alert(driver, username, context="during heavy comment-liking"):
                    return liked_posts + commented_posts

                log_and_telegram(
                    f"[{username}] ✅ Post {processed_posts}/{target_posts} — "
                    f"liked: {'yes' if liked else 'no'}, commented: {'yes' if commented else 'no'}"
                )

            except Exception as e:
                logger.debug(f"[{username}] Heavy feed interaction error on post {processed_posts}: {e}")

            # Delay between posts for human-like pacing
            if _interruptible_sleep(random.uniform(2.0, 4.0), stop_event=stop_event):
                break

        if stop_event and stop_event.is_set():
            break
        if processed_posts >= target_posts:
            break

        # Scroll for more posts
        _scroll_home_feed(driver)
        if _interruptible_sleep(random.uniform(1.5, 2.5), stop_event=stop_event):
            break

        if found_new:
            consecutive_empty_scrolls = 0
        else:
            consecutive_empty_scrolls += 1
            if consecutive_empty_scrolls >= 6:
                log_and_telegram(f"[{username}] ⚠️ No new posts after 6 scrolls, stopping early")
                break

    log_and_telegram(
        f"[{username}] ✅ Heavy comment-liking complete: "
        f"processed {processed_posts}/{target_posts}, "
        f"liked {liked_posts}, commented {commented_posts}"
    )
    return processed_posts




def _dm_list(
    driver, usernames: list, messages: list,
    dm_log: dict, already_dmd: set,
    max_dms: int, sender: str, model: str,
    dm_batch_state: dict = None,
    stop_event=None,
    coordinator=None,
    use_global_target_dedupe: bool = False,
) -> int:
    """
    Send DMs to a list of usernames.
    
    Returns number of DMs successfully sent.
    """
    sent = 0
    sent_since_break = 0
    # break_threshold is strictly 3 per user request
    break_threshold = 3

    for target_user in usernames:
        if stop_event and stop_event.is_set():
            log_and_telegram(f"[{sender}] 🛑 Stop requested during DM queue")
            break

        if sent >= max_dms:
            break

        if target_user in already_dmd:
            continue

        if coordinator and coordinator.enabled and not coordinator.has_account_lock(sender):
            log_and_telegram(f"[{sender}] ⚠️ Distributed lock lost while DMing. Stopping queue.")
            break

        _maybe_wait_for_dm_batch_cooldown(sender, dm_batch_state, stop_event=stop_event)
        if stop_event and stop_event.is_set():
            log_and_telegram(f"[{sender}] 🛑 Stop requested during DM queue")
            break

        claim_token = None
        if use_global_target_dedupe and coordinator and coordinator.enabled:
            claimed, claim_token, claim_reason = coordinator.claim_target(
                target_username=target_user,
                sender_account=sender,
                model_username=model,
            )
            if not claimed:
                if claim_reason == "already_claimed":
                    already_dmd.add(target_user)
                    log_and_telegram(f"[{sender}] ⏭️ @{target_user} already claimed by another VPS")
                    continue

                log_and_telegram(
                    f"[{sender}] ⚠️ Could not claim @{target_user} ({claim_reason}). Stopping queue for safety."
                )
                break

        # Pick a random message template
        message = random.choice(messages)

        log_and_telegram(f"[{sender}] DMing @{target_user}...")
        max_send_attempts = 2
        result = DMResult.ERROR
        for send_attempt in range(1, max_send_attempts + 1):
            try:
                result = send_dm(driver, target_user, message)
            except Exception as e:
                result = f"exception: {e}"

            if result in (DMResult.SENT, DMResult.CANT_MESSAGE, DMResult.USER_NOT_FOUND):
                if send_attempt > 1:
                    log_and_telegram(
                        f"[{sender}] ✅ Recovered on retry {send_attempt}/{max_send_attempts} for @{target_user}"
                    )
                break

            if send_attempt < max_send_attempts:
                log_and_telegram(
                    f"[{sender}] ⚠️ Submit failed for @{target_user} (attempt {send_attempt}/{max_send_attempts}). Retrying same follower..."
                )
                human_delay(1.5, 2.5)
        event_status = "failed"

        if result == DMResult.SENT:
            sent += 1
            if isinstance(dm_batch_state, dict):
                dm_batch_state["sent_count"] = int(dm_batch_state.get("sent_count", 0) or 0) + 1
            already_dmd.add(target_user)
            database.log_dm_sent(target_user)
            event_status = "sent"
            log_and_telegram(f"[{sender}] ✅ DM sent to @{target_user} ({sent}/{max_dms})")
        elif result == DMResult.CANT_MESSAGE:
            event_status = "cant_message"
            log_and_telegram(f"[{sender}] ⚠️ Can't message @{target_user}")
            already_dmd.add(target_user)  # Don't retry
        elif result == DMResult.USER_NOT_FOUND:
            event_status = "user_not_found"
            log_and_telegram(f"[{sender}] ❌ @{target_user} not found")
            already_dmd.add(target_user)
        else:
            event_status = str(result).strip().lower() or "failed"
            telegram_bot.stats["dms_failed"] += 1
            log_and_telegram(f"[{sender}] ❌ DM to @{target_user} failed: {result}")

        try:
            database.log_dm_event(
                sender_account=sender,
                target_username=target_user,
                model_username=model,
                status=event_status,
            )
        except Exception as e:
            logger.debug(f"[{sender}] Failed to log DM event for @{target_user}: {e}")

        if (
            claim_token
            and coordinator
            and coordinator.enabled
            and event_status not in ("sent", "cant_message", "user_not_found")
        ):
            coordinator.release_target_claim(target_user, claim_token)

        # Check for challenges mid-session
        if _check_for_challenges_and_alert(driver, sender, context="during DM session"):
            break

        # Optional pause after 3 DMs.
        if event_status == "sent":
            if isinstance(dm_batch_state, dict):
                current_c = int(dm_batch_state.get("sent_since_human_break", 0) or 0) + 1
                dm_batch_state["sent_since_human_break"] = current_c
                if current_c >= break_threshold:
                    log_and_telegram(f"[{sender}] ☕ Taking a short break after {break_threshold} DMs...")
                    _interruptible_sleep(random.randint(30, 60), stop_event=stop_event)
                    dm_batch_state["sent_since_human_break"] = 0
            else:
                sent_since_break += 1
                if sent_since_break >= break_threshold:
                    log_and_telegram(f"[{sender}] ☕ Taking a short break after {break_threshold} DMs...")
                    _interruptible_sleep(random.randint(30, 60), stop_event=stop_event)
                    sent_since_break = 0

        # Random delay between DMs
        if sent < max_dms and target_user != usernames[-1]:
            wait_between_dms(stop_event=stop_event)

    return sent

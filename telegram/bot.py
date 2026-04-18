"""
Telegram bot for alerting employees and receiving 2FA codes / approvals.
Runs a polling loop in a background thread.
"""
import time
import threading
import queue
import logging
import requests
from collections import deque
from datetime import datetime

from config import database
from config.database import get_setting

logger = logging.getLogger("model_dm_bot")
COMMAND_DEDUPE_COOLDOWN_SEC = 24 * 60 * 60


class TelegramBot:
    """
    Telegram bot that:
    - Sends alerts to employees (challenges, status, errors)
    - Polls for employee responses (2FA codes, approvals)
    - Provides /status command support
    """

    def __init__(self, token: str = None, chat_ids: list = None):
        self.token = str(token or "").strip()
        self.chat_ids = self._normalize_chat_ids(chat_ids)
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""
        self.lock_alert_token = ""
        self.lock_alert_chat_ids = []
        self.lock_alert_base_url = ""

        self.last_update_id = 0
        self.code_queue = queue.Queue()        # Queue for 2FA codes
        self.approval_queue = queue.Queue()    # Queue for manual approvals
        self.logs = deque(maxlen=20)           # Recent log lines
        self.start_time = time.time()
        self.stats = {
            "accounts_used": 0,
            "models_processed": 0,
            "dms_sent": 0,
            "dms_failed": 0,
            "current_account": "—",
            "current_model": "—",
            "status": "Initializing",
        }

        self._polling = False
        self._poll_thread = None

    @staticmethod
    def _normalize_chat_ids(raw_chat_ids) -> list:
        if isinstance(raw_chat_ids, str):
            return [cid.strip() for cid in raw_chat_ids.split(",") if cid.strip()]
        if isinstance(raw_chat_ids, list):
            return [str(cid).strip() for cid in raw_chat_ids if str(cid).strip()]
        return []

    def _reload_config_from_db(self):
        """Refresh bot token/chat IDs from database settings."""
        try:
            database.init_db()
            raw_token = get_setting("TELEGRAM_BOT_TOKEN")
            raw_chat_ids = get_setting("TELEGRAM_CHAT_IDS")
            raw_lock_alert_token = get_setting("LOCK_ALERT_BOT_TOKEN")
            raw_lock_alert_chat_ids = get_setting("LOCK_ALERT_CHAT_IDS")

            if raw_token is not None:
                self.token = str(raw_token).strip()
                self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""

            if raw_chat_ids is not None:
                self.chat_ids = self._normalize_chat_ids(raw_chat_ids)

            if raw_lock_alert_token is not None:
                self.lock_alert_token = str(raw_lock_alert_token).strip()
                self.lock_alert_base_url = (
                    f"https://api.telegram.org/bot{self.lock_alert_token}" if self.lock_alert_token else ""
                )

            if raw_lock_alert_chat_ids is not None:
                self.lock_alert_chat_ids = self._normalize_chat_ids(raw_lock_alert_chat_ids)

            if self.lock_alert_token and not self.lock_alert_chat_ids:
                # Default lock-alert audience to normal Telegram audience when dedicated list is unset.
                self.lock_alert_chat_ids = list(self.chat_ids)
        except Exception as e:
            logger.warning(f"[Telegram] Failed to load config from DB: {e}")

    def _send_with_bot(self, token: str, chat_ids, message: str, tag: str = "[Telegram]") -> bool:
        clean_token = str(token or "").strip()
        clean_chat_ids = self._normalize_chat_ids(chat_ids)
        if not clean_token or not clean_chat_ids:
            return False

        base_url = f"https://api.telegram.org/bot{clean_token}"
        sent_any = False
        for chat_id in clean_chat_ids:
            try:
                requests.post(
                    f"{base_url}/sendMessage",
                    data={
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "Markdown",
                    },
                    timeout=10,
                )
                sent_any = True
            except Exception as e:
                logger.error(f"{tag} Failed to send to {chat_id}: {e}")

        return sent_any

    @staticmethod
    def _is_locked_challenge_type(challenge_type: str) -> bool:
        text = str(challenge_type or "").strip().lower()
        if not text:
            return False

        if "unlock" in text or "unlocked" in text:
            return False

        return "lock" in text

    def _send_lock_alert_message(self, message: str) -> bool:
        """Send challenge/lock alerts only through the dedicated lock alert bot."""
        if not self.lock_alert_token:
            self._reload_config_from_db()

        target_chat_ids = self.lock_alert_chat_ids or self.chat_ids
        sent_with_lock_bot = self._send_with_bot(
            self.lock_alert_token,
            target_chat_ids,
            message,
            tag="[Telegram-LockBot]",
        )

        if not sent_with_lock_bot:
            logger.error("[Telegram-LockBot] Lock alert message was not delivered (check LOCK_ALERT_BOT_TOKEN / LOCK_ALERT_CHAT_IDS)")

        return sent_with_lock_bot

    def _claim_command_slot(self, update_id, action: str, cooldown_seconds: int = COMMAND_DEDUPE_COOLDOWN_SEC) -> bool:
        """Return True only on the first VPS allowed to respond for this Telegram update/action."""
        clean_update_id = str(update_id or "").strip()
        clean_action = str(action or "").strip().lower()
        if not clean_update_id or not clean_action:
            return True

        dedupe_key = f"telegram:command:{clean_action}:{clean_update_id}"
        try:
            return database.claim_notification_event(
                dedupe_key,
                cooldown_seconds=cooldown_seconds,
            )
        except Exception as e:
            logger.debug(f"[Telegram] Command dedupe bypassed ({clean_action}): {e}")
            return True

    # ──────────────────────────────────────────
    # Sending Messages
    # ──────────────────────────────────────────

    def send(self, message: str):
        """Send a message to all configured chats."""
        if not self.token or not self.chat_ids:
            self._reload_config_from_db()

        if not self.token or not self.chat_ids:
            return

        self._send_with_bot(self.token, self.chat_ids, message, tag="[Telegram]")

    def send_startup(self):
        """Send bot startup notification."""
        self.start_time = time.time()
        self.send(
            "🚀 *MODEL DM BOT STARTED*\n\n"
            f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}\n"
            "Use `/status` for current state\n"
            "Use `/target` for active account + model target\n"
            "Use `/automation @username on|off` for account automation\n"
            "Use `/summary` for DM summary\n"
            "Use `/accounts` for IG bios + urls\n"
            "Use `/stop` to request stop"
        )

    def send_account_pool_summary(self, summary_text: str):
        """Send an IG account availability summary grouped by model label."""
        clean_summary = str(summary_text or "").strip()
        if not clean_summary:
            return

        self.send(
            "📊 *IG ACCOUNT POOL*\n\n"
            f"{clean_summary}"
        )

    @staticmethod
    def _compact_profile_note(raw_text: str, max_len: int = 260) -> str:
        text = str(raw_text or "").replace("\r", "\n")
        chunks = [part.strip() for part in text.split("\n") if part.strip()]
        merged = " | ".join(chunks)
        merged = merged.replace("`", "'")
        if len(merged) > max_len:
            return merged[: max_len - 3].rstrip() + "..."
        return merged

    @staticmethod
    def _coerce_positive_int(raw_value):
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        return value

    def send_account_profile_summary(self, accounts: list, limit: int = None, recent_only: bool = False):
        """Send IG account bio/url preset text.

        Args:
            accounts: Account rows to format.
            limit: Optional max number of rows to include.
            recent_only: If True, keep the newest rows based on incoming order.
        """
        rows = accounts if isinstance(accounts, list) else []
        if not rows:
            self.send("🧷 *IG ACCOUNT BIOS + URLS*\n\n_No accounts configured yet._")
            return

        normalized_rows = []
        for account in rows:
            username = str(account.get("username") or "").strip().lstrip("@")
            if not username:
                continue

            normalized_rows.append(
                {
                    "username": username,
                    "profile_note": self._compact_profile_note(account.get("profile_note", "")),
                }
            )

        if not normalized_rows:
            self.send("🧷 *IG ACCOUNT BIOS + URLS*\n\n_No accounts configured yet._")
            return

        safe_limit = self._coerce_positive_int(limit)
        if recent_only:
            selected_rows = list(normalized_rows)
            if safe_limit is not None:
                selected_rows = selected_rows[-safe_limit:]
        else:
            selected_rows = sorted(normalized_rows, key=lambda item: item["username"].lower())
            if safe_limit is not None:
                selected_rows = selected_rows[:safe_limit]

        lines = []
        for idx, row in enumerate(selected_rows, start=1):
            username = row["username"]
            profile_note = row["profile_note"]
            if profile_note:
                lines.append(f"{idx}. `@{username}`\n   `{profile_note}`")
            else:
                lines.append(f"{idx}. `@{username}`\n   `_No bio/url preset_`")

        if not lines:
            lines = ["_No accounts configured yet._"]

        chunks = []
        current_chunk = []
        current_len = 0
        max_chunk_chars = 3200
        for line in lines:
            projected = current_len + len(line) + 1
            if current_chunk and projected > max_chunk_chars:
                chunks.append(current_chunk)
                current_chunk = [line]
                current_len = len(line) + 1
                continue

            current_chunk.append(line)
            current_len = projected

        if current_chunk:
            chunks.append(current_chunk)

        use_accounts_hint = (
            recent_only
            and safe_limit is not None
            and len(normalized_rows) > len(selected_rows)
        )
        intro = ""
        if recent_only and safe_limit is not None:
            intro = f"_Showing latest {len(selected_rows)} accounts._\n\n"

        title = "🧷 *IG ACCOUNT BIOS + URLS*"
        if len(chunks) == 1:
            suffix = "\n\n_Use /accounts to view all bio + url presets._" if use_accounts_hint else ""
            self.send(
                f"{title}\n\n"
                f"{intro}"
                + "\n".join(chunks[0])
                + suffix
            )
            return

        for idx, chunk in enumerate(chunks, start=1):
            part_title = f"{title} ({idx}/{len(chunks)})"
            part_intro = intro if idx == 1 else ""
            part_suffix = "\n\n_Use /accounts to view all bio + url presets._" if (use_accounts_hint and idx == len(chunks)) else ""
            self.send(
                f"{part_title}\n\n"
                f"{part_intro}"
                + "\n".join(chunk)
                + part_suffix
            )

    def send_challenge_alert(self, account: str, challenge_type: str, url: str = ""):
        """Alert employees about challenge detection via dedicated challenge bot."""
        challenge_label = str(challenge_type or "").strip()
        is_locked = self._is_locked_challenge_type(challenge_label)

        if is_locked:
            actions = [
                "• Locked account is skipped automatically",
                "• Account is moved to Suspended Accounts",
                "• No `/code`, `/approve`, or `/skip` needed",
            ]
        else:
            actions = [
                "• Challenge account is skipped automatically",
                "• No `/code`, `/approve`, or `/skip` needed",
            ]

        message = (
            f"⚠️ *CHALLENGE DETECTED*\n\n"
            f"👤 Account: `{account}`\n"
            f"🔒 Type: `{challenge_label}`\n\n"
            f"*Actions:*\n"
            + "\n".join(actions)
        )

        self._send_lock_alert_message(message)

    def send_lockout_alert(self, account: str, description: str):
        """Alert employees that an account is locked out."""
        message = (
            f"🚨 *ACCOUNT LOCKED*\n\n"
            f"👤 Account: `{account}`\n"
            f"📝 Details: {description}\n\n"
            f"Bot will skip this account and continue.\n"
            f"Please unlock manually and reply `/approve` when ready."
        )
        self._send_lock_alert_message(message)

    def send_progress(self, account: str, model: str, dms_sent: int, total_target: int):
        """Send a progress update. Muted by user request to prevent spam."""
        pass

    def send_model_complete(self, model: str, dms_sent: int, sender_account: str = ""):
        """Notify that a model target is complete."""
        sender = str(sender_account or "").strip().lstrip("@")
        sender_line = f"👤 IG Account: `@{sender}`\n" if sender else ""

        self.send(
            f"✅ *MODEL COMPLETE*\n\n"
            f"🎯 Model: `@{model}`\n"
            f"{sender_line}"
            f"✉️ DMs sent: {dms_sent}"
        )

    def send_session_complete(self, total_dms: int, models_done: int, by_account=None):
        """Notify that the entire bot session is done."""
        rows = by_account if isinstance(by_account, dict) else {}
        account_lines = []
        for account, count in sorted(rows.items(), key=lambda item: (-int(item[1] or 0), str(item[0]).lower())):
            safe_account = str(account or "").strip().lstrip("@") or "unknown"
            try:
                safe_count = int(count or 0)
            except Exception:
                safe_count = 0
            account_lines.append(f"• `@{safe_account}`: `{safe_count}`")

        account_section = ""
        if account_lines:
            account_section = "\n\n*DMs By Our Accounts:*\n" + "\n".join(account_lines[:40])

        self.send(
            f"🏁 *SESSION COMPLETE*\n\n"
            f"✉️ Total DMs: {total_dms}\n"
            f"🎯 Models: {models_done}\n"
            f"⏰ Started : {self._started_ago()}"
            f"{account_section}"
        )

    def send_24h_dm_summary(self, summary: dict):
        """Send last-24h DM totals with per-account breakdown."""
        payload = summary if isinstance(summary, dict) else {}

        try:
            hours = int(payload.get("hours", 24) or 24)
        except Exception:
            hours = 24

        try:
            total_sent = int(payload.get("total_sent", 0) or 0)
        except Exception:
            total_sent = 0

        try:
            lifetime_total_sent = int(payload.get("lifetime_total_sent", 0) or 0)
        except Exception:
            lifetime_total_sent = 0

        raw_by_account = payload.get("by_account", [])
        by_account = raw_by_account if isinstance(raw_by_account, list) else []

        label_by_account = {}
        try:
            account_rows = database.get_accounts(include_all=True)
            for row in account_rows:
                username = str(row.get("username") or "").strip().lstrip("@").lower()
                if not username:
                    continue

                raw_label = str(row.get("model_label") or "").strip().lstrip("@")
                label_key = raw_label.lower()
                if label_key in ("", "generic", "any", "all", "*", "none"):
                    label_display = "Generic"
                else:
                    label_display = raw_label

                label_by_account[username] = label_display
        except Exception as e:
            logger.debug(f"[Telegram] Failed to map account labels for DM summary: {e}")

        line_limit = 60
        normalized_rows = []
        for item in by_account:
            if not isinstance(item, dict):
                continue

            account = str(item.get("sender_account") or "").strip() or "unknown"
            try:
                count = int(item.get("count", 0) or 0)
            except Exception:
                count = 0

            account_key = account.lstrip("@").lower()
            label = label_by_account.get(account_key, "Unknown")
            normalized_rows.append(
                {
                    "account": account,
                    "count": count,
                    "label": label,
                }
            )

        visible_rows = normalized_rows[:line_limit]
        hidden_count = max(0, len(normalized_rows) - len(visible_rows))

        grouped = {}
        for row in visible_rows:
            label = str(row.get("label") or "Unknown").strip() or "Unknown"
            grouped.setdefault(label, []).append(row)

        lines = []
        if grouped:
            ordered_labels = sorted(
                grouped.keys(),
                key=lambda label: (
                    -sum(int(item.get("count", 0) or 0) for item in grouped.get(label, [])),
                    str(label).lower(),
                ),
            )
            for idx, label in enumerate(ordered_labels):
                if idx > 0:
                    lines.append("")
                lines.append(f"*{label}:*")

                label_rows = sorted(
                    grouped.get(label, []),
                    key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("account", "")).lower()),
                )
                for row in label_rows:
                    account = str(row.get("account") or "").strip() or "unknown"
                    count = int(row.get("count", 0) or 0)
                    lines.append(f"• `@{account}`: `{count}`")

        if hidden_count > 0:
            if lines:
                lines.append("")
            lines.append(f"• ... and `{hidden_count}` more accounts")

        if not lines:
            lines.append("• _No DM activity recorded in this window._")

        self.send(
            f"🧾 *DM SUMMARY*\n\n"
            f"✉️ *Sent In Last {hours}H:* `{total_sent}`\n"
            f"🏁 *Lifetime Total Sent:* `{lifetime_total_sent}`\n\n"
            f"*Per Account:*\n"
            + "\n".join(lines)
        )

    def send_error(self, error: str):
        """Send an error alert."""
        self.send(f"❌ *ERROR*\n\n```\n{error[:500]}\n```")

    # ──────────────────────────────────────────
    # Receiving Responses
    # ──────────────────────────────────────────

    def wait_for_code(self, timeout: int = 300) -> str:
        """
        Wait for an employee to send a 2FA code via Telegram.
        
        Returns:
            The code string, or empty string if timeout
        """
        logger.info(f"[Telegram] Waiting up to {timeout}s for 2FA code...")
        try:
            code = self.code_queue.get(timeout=timeout)
            return code
        except queue.Empty:
            logger.warning("[Telegram] Timed out waiting for 2FA code")
            return ""

    def wait_for_approval(self, timeout: int = 300) -> bool:
        """
        Wait for an employee to approve a manual action.
        
        Returns:
            True if approved, False if timed out or skipped
        """
        logger.info(f"[Telegram] Waiting up to {timeout}s for approval...")
        try:
            result = self.approval_queue.get(timeout=timeout)
            return result == "approve"
        except queue.Empty:
            logger.warning("[Telegram] Timed out waiting for approval")
            return False

    # ──────────────────────────────────────────
    # Polling Thread
    # ──────────────────────────────────────────

    def start_polling(self):
        """Start the background polling thread."""
        self._reload_config_from_db()
        if not self.token:
            logger.warning("[Telegram] Polling not started: TELEGRAM_BOT_TOKEN is missing in DB settings")
            return

        if self._polling:
            return
        self._polling = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        logger.info("[Telegram] Polling thread started")

    def stop_polling(self):
        """Stop the background polling thread."""
        self._polling = False
        if self._poll_thread:
            self._poll_thread.join(timeout=5)
        logger.info("[Telegram] Polling thread stopped")

    def _poll_loop(self):
        """Background loop that polls Telegram for new messages."""
        while self._polling:
            try:
                url = f"{self.base_url}/getUpdates"
                params = {
                    "offset": self.last_update_id + 1,
                    "timeout": 5,
                }
                r = requests.get(url, params=params, timeout=15)
                data = r.json()

                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        self.last_update_id = update["update_id"]
                        self._handle_update(update)

            except Exception as e:
                logger.debug(f"[Telegram] Poll error: {e}")

            time.sleep(2)

    def _handle_update(self, update: dict):
        """Process a single Telegram update."""
        update_id = str(update.get("update_id", "") or "").strip()
        message = update.get("message", {})
        text = message.get("text", "").strip()
        sender_id = str(message.get("chat", {}).get("id", ""))

        if sender_id not in self.chat_ids:
            return

        if not text:
            return

        text_lower = text.lower()

        # /code 123456
        if text_lower.startswith("/code "):
            code = text[6:].strip()
            if code and code.isdigit() and len(code) == 6:
                self.code_queue.put(code)
                if self._claim_command_slot(update_id, "code_ack"):
                    self.send(f"✅ Code `{code}` received. Processing...")
                logger.info(f"[Telegram] Received 2FA code: {code}")
            else:
                if self._claim_command_slot(update_id, "code_invalid"):
                    self.send("❌ Invalid code. Send `/code` followed by a 6-digit number.")

        # /approve
        elif text_lower == "/approve":
            self.approval_queue.put("approve")
            if self._claim_command_slot(update_id, "approve_ack"):
                self.send("✅ Approval received. Resuming bot...")
            logger.info("[Telegram] Received approval")

        # /skip
        elif text_lower == "/skip":
            self.approval_queue.put("skip")
            self.code_queue.put("")  # Unblock code wait too
            if self._claim_command_slot(update_id, "skip_ack"):
                self.send("⏭️ Skipping current account...")
            logger.info("[Telegram] Received skip command")

        # /status
        elif text_lower == "/status":
            if self._claim_command_slot(update_id, "status"):
                self.send(self._get_status_text())

        # /target | /current | /running
        elif text_lower in ("/target", "/current", "/running"):
            if self._claim_command_slot(update_id, "target"):
                self.send(self._get_targeting_text())

        # /summary or /summary 48
        elif text_lower.startswith("/summary"):
            if not self._claim_command_slot(update_id, "summary"):
                return

            parts = text.split()
            hours = 24

            if len(parts) > 1:
                try:
                    hours = int(parts[1])
                except Exception:
                    self.send("❌ Invalid summary range. Use `/summary` or `/summary 48`.")
                    return

            try:
                summary = database.get_dm_sent_summary_last_hours(
                    hours=hours,
                    include_all_accounts=True,
                )
                self.send_24h_dm_summary(summary)
                logger.info(
                    "[Telegram] Sent manual summary request (hours=%s, total_sent=%s, lifetime_total_sent=%s)",
                    summary.get("hours", hours),
                    summary.get("total_sent", 0),
                    summary.get("lifetime_total_sent", 0),
                )
            except Exception as e:
                logger.error(f"[Telegram] Failed to build summary on /summary command: {e}")
                self.send("❌ Failed to generate summary right now.")

        # /accounts
        elif text_lower == "/accounts":
            if not self._claim_command_slot(update_id, "accounts"):
                return

            try:
                accounts = database.get_accounts(include_all=True)
                self.send_account_profile_summary(accounts)
            except Exception as e:
                logger.error(f"[Telegram] Failed to load accounts on /accounts command: {e}")
                self.send("❌ Failed to fetch account bios/urls right now.")

        # /automation <@account> <on|off>
        elif text_lower.startswith("/automation") or text_lower.startswith("/auto"):
            if self._claim_command_slot(update_id, "automation"):
                self._handle_automation_command(text)

        # /stop
        elif text_lower == "/stop":
            if self._claim_command_slot(update_id, "stop_ack"):
                self.send("🛑 Stop requested. Bot will finish current DM and stop.")
            self._polling = False

    def _get_status_text(self) -> str:
        """Generate a status message."""
        return (
            f"🤖 *MODEL DM BOT STATUS*\n\n"
            f"🟢 Status: `{self.stats['status']}`\n"
            f"⏱️ Uptime: `{self._uptime()}`\n\n"
            f"👤 Account: `{self.stats['current_account']}`\n"
            f"🎯 Model: `{self.stats['current_model']}`\n\n"
            f"✉️ DMs Sent: `{self.stats['dms_sent']}`\n"
            f"❌ DMs Failed: `{self.stats['dms_failed']}`\n"
            f"🎯 Models Done: `{self.stats['models_processed']}`\n\n"
            f"📋 *Recent Logs:*\n"
            f"```\n" + "\n".join(list(self.logs)[-5:]) + "\n```"
        )

    @staticmethod
    def _format_handle(raw_value: str) -> str:
        clean = str(raw_value or "").strip().lstrip("@")
        if not clean or clean == "—":
            return "—"
        return f"@{clean}"

    def _get_targeting_text(self) -> str:
        """Generate concise live targeting text for Telegram commands."""
        status = str(self.stats.get("status", "Initializing") or "Initializing")
        account = self._format_handle(self.stats.get("current_account", ""))
        model = self._format_handle(self.stats.get("current_model", ""))

        if status.strip().lower() != "running" or account == "—" or model == "—":
            return (
                "🎯 *LIVE TARGETING*\n\n"
                f"🟢 Status: `{status}`\n"
                "No active account/model targeting right now."
            )

        return (
            "🎯 *LIVE TARGETING*\n\n"
            f"🟢 Status: `{status}`\n"
            f"👤 Running IG Account: `{account}`\n"
            f"🧲 Targeting Followers Of: `{model}`"
        )

    @staticmethod
    def _normalize_account_username(raw_value: str) -> str:
        return str(raw_value or "").strip().lstrip("@")

    def _send_automation_usage(self):
        self.send(
            "👁️ *ACCOUNT AUTOMATION CONTROL*\n\n"
            "Use commands below:\n"
            "• `/automation @username off`\n"
            "• `/automation @username on`"
        )

    def _handle_automation_command(self, text: str):
        parts = [part.strip() for part in str(text or "").split() if part.strip()]
        if not parts:
            self._send_automation_usage()
            return

        if len(parts) == 1:
            self._send_automation_usage()
            return

        if len(parts) < 3:
            self._send_automation_usage()
            return

        username = self._normalize_account_username(parts[1])
        action = parts[2].lower()
        if not username:
            self.send("❌ Missing account username. Example: `/automation @alice off`")
            return

        if action in ("off", "disable", "disabled", "0", "false"):
            enable_automation = False
        elif action in ("on", "enable", "enabled", "1", "true"):
            enable_automation = True
        else:
            self.send("❌ Invalid action. Use `on` or `off`.")
            return

        try:
            updated = database.set_account_automation_enabled(username, enable_automation)
        except Exception as e:
            logger.error(f"[Telegram] Failed to update automation state for @{username}: {e}")
            self.send("❌ Failed to update account automation status right now.")
            return

        if not updated:
            self.send(f"❌ Account `@{username}` not found.")
            return

        if enable_automation:
            self.send(f"👁️ Automation *ENABLED* for `@{username}`.")
            logger.info(f"[Telegram] Automation enabled for @{username} by command")
        else:
            self.send(f"🙈 Automation *DISABLED* for `@{username}`.")
            logger.info(f"[Telegram] Automation disabled for @{username} by command")

    def _uptime(self) -> str:
        """Get formatted uptime string."""
        elapsed = time.time() - self.start_time
        h, r = divmod(elapsed, 3600)
        m, s = divmod(r, 60)
        return f"{int(h)}h {int(m)}m {int(s)}s"

    def _started_ago(self) -> str:
        """Get a compact relative start string (e.g. 7h ago)."""
        elapsed = max(0, int(time.time() - self.start_time))
        days, rem = divmod(elapsed, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)

        if days > 0:
            return f"{days}d ago"
        if hours > 0:
            return f"{hours}h ago"
        if minutes > 0:
            return f"{minutes}m ago"
        return "just now"

    def add_log(self, message: str):
        """Add a log line to the recent logs buffer."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")


# Global instance
telegram_bot = TelegramBot()

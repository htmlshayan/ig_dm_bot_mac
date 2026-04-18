"""Redis-backed coordination helpers for multi-VPS bot safety."""

import json
import logging
import os
import random
import socket
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

try:
    import redis
except Exception:  # pragma: no cover - handled by runtime checks
    redis = None


def _as_bool(raw_value, default=False):
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


def _as_int(raw_value, default=0, minimum=0):
    try:
        value = int(raw_value)
    except Exception:
        value = int(default)
    return max(int(minimum), value)


def _normalize_username(raw_value):
    return str(raw_value or "").strip().lstrip("@").lower()


@dataclass
class AccountLockLease:
    username_key: str
    lock_key: str
    meta_key: str
    token: str
    owner_username: str
    session_id: str


class DistributedCoordinator:
    """Coordinates account ownership and target claims across multiple bot servers."""

    _ACCOUNT_RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[2])
  return 1
end
return 0
"""

    _ACCOUNT_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
"""

    _TARGET_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

    @classmethod
    def from_settings(cls, settings: dict, logger=None, account_owner: str = ""):
        raw_settings = settings if isinstance(settings, dict) else {}
        enabled = _as_bool(raw_settings.get("REDIS_COORDINATION_ENABLED", False), default=False)

        instance_id = str(raw_settings.get("REDIS_INSTANCE_ID", "") or "").strip()
        if not instance_id:
            instance_id = f"{socket.gethostname()}-{os.getpid()}"

        return cls(
            enabled=enabled,
            redis_url=str(raw_settings.get("REDIS_URL", "") or "").strip(),
            namespace=str(raw_settings.get("REDIS_LOCK_NAMESPACE", "igbot:v1") or "igbot:v1").strip(),
            account_lock_ttl_sec=_as_int(raw_settings.get("REDIS_ACCOUNT_LOCK_TTL_SEC", 180), default=180, minimum=30),
            account_heartbeat_sec=_as_int(raw_settings.get("REDIS_ACCOUNT_HEARTBEAT_SEC", 30), default=30, minimum=5),
            instance_heartbeat_ttl_sec=_as_int(
                raw_settings.get("REDIS_INSTANCE_HEARTBEAT_TTL_SEC", 90),
                default=90,
                minimum=15,
            ),
            target_claim_ttl_sec=_as_int(raw_settings.get("REDIS_TARGET_CLAIM_TTL_SEC", 86400), default=86400, minimum=60),
            fail_closed=_as_bool(raw_settings.get("REDIS_FAIL_CLOSED", True), default=True),
            instance_id=instance_id,
            session_owner=str(account_owner or "").strip().lower(),
            logger=logger,
        )

    def __init__(
        self,
        enabled: bool,
        redis_url: str,
        namespace: str,
        account_lock_ttl_sec: int,
        account_heartbeat_sec: int,
        instance_heartbeat_ttl_sec: int,
        target_claim_ttl_sec: int,
        fail_closed: bool,
        instance_id: str,
        session_owner: str,
        logger=None,
    ):
        self.enabled = bool(enabled)
        self.redis_url = str(redis_url or "").strip()
        self.namespace = str(namespace or "igbot:v1").strip() or "igbot:v1"
        self.account_lock_ttl_sec = max(30, int(account_lock_ttl_sec or 180))
        self.account_heartbeat_sec = max(5, int(account_heartbeat_sec or 30))
        self.instance_heartbeat_ttl_sec = max(15, int(instance_heartbeat_ttl_sec or 90))
        self.target_claim_ttl_sec = max(60, int(target_claim_ttl_sec or 86400))
        self.fail_closed = bool(fail_closed)
        self.instance_id = str(instance_id or "").strip() or f"{socket.gethostname()}-{os.getpid()}"
        self.session_owner = str(session_owner or "").strip().lower()
        self.logger = logger or logging.getLogger("model_dm_bot")

        self._client = None
        self._owned_lock = threading.Lock()
        self._owned_accounts = {}
        self._stop_event = threading.Event()
        self._heartbeat_thread = None

        if not self.enabled:
            return

        self._connect()
        if self._client is not None:
            self._touch_instance_heartbeat()
            self._start_heartbeat_thread()

    def _connect(self):
        if redis is None:
            self.logger.warning("Redis coordination enabled but redis package is not installed")
            self._client = None
            return

        if not self.redis_url:
            self.logger.warning("Redis coordination enabled but REDIS_URL is empty")
            self._client = None
            return

        try:
            self._client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30,
                retry_on_timeout=True,
            )
            self._client.ping()
        except Exception as e:
            self.logger.warning(f"Failed to connect to Redis coordination backend: {e}")
            self._client = None

    @property
    def is_active(self) -> bool:
        return bool(self.enabled and self._client is not None)

    def _instance_key(self):
        return f"{self.namespace}:instance:{self.instance_id}:alive"

    def _account_lock_key(self, username_key: str):
        return f"{self.namespace}:lock:account:{username_key}"

    def _account_meta_key(self, username_key: str):
        return f"{self.namespace}:lockmeta:account:{username_key}"

    def _target_claim_key(self, target_key: str):
        return f"{self.namespace}:dmclaim:target:{target_key}"

    def _lease_meta_json(self, lease: AccountLockLease):
        payload = {
            "instance_id": self.instance_id,
            "session_owner": self.session_owner,
            "owner_username": lease.owner_username,
            "session_id": lease.session_id,
            "username": lease.username_key,
            "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        return json.dumps(payload)

    def _touch_instance_heartbeat(self):
        if not self._client:
            return

        payload = {
            "instance_id": self.instance_id,
            "session_owner": self.session_owner,
            "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

        try:
            self._client.set(
                self._instance_key(),
                json.dumps(payload),
                ex=self.instance_heartbeat_ttl_sec,
            )
        except Exception as e:
            self.logger.debug(f"Failed to refresh instance heartbeat: {e}")

    def _start_heartbeat_thread(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="distributed-coordination-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        while not self._stop_event.is_set():
            jitter = random.uniform(-3.0, 3.0)
            sleep_for = max(2.0, float(self.account_heartbeat_sec) + jitter)
            if self._stop_event.wait(sleep_for):
                break

            if not self.is_active:
                continue

            self._touch_instance_heartbeat()

            with self._owned_lock:
                leases = list(self._owned_accounts.values())

            for lease in leases:
                if self._stop_event.is_set():
                    break
                if self._renew_account_lock(lease):
                    continue

                with self._owned_lock:
                    current = self._owned_accounts.get(lease.username_key)
                    if current and current.token == lease.token:
                        self._owned_accounts.pop(lease.username_key, None)

                self.logger.warning(
                    f"Distributed account lock lost for @{lease.username_key}; account processing should stop"
                )

    def _renew_account_lock(self, lease: AccountLockLease) -> bool:
        if not self._client:
            return False

        try:
            result = self._client.eval(
                self._ACCOUNT_RENEW_SCRIPT,
                2,
                lease.lock_key,
                lease.meta_key,
                lease.token,
                str(self.account_lock_ttl_sec),
                self._lease_meta_json(lease),
            )
            return int(result or 0) == 1
        except Exception as e:
            self.logger.warning(f"Failed to renew account lock for @{lease.username_key}: {e}")
            return False

    def acquire_account_lock(self, username: str, owner_username: str = "", session_id: str = ""):
        username_key = _normalize_username(username)
        if not username_key:
            return False, "invalid_username"

        if not self.enabled:
            return True, "disabled"

        if not self._client:
            if self.fail_closed:
                return False, "redis_unavailable"
            return True, "redis_unavailable_bypass"

        with self._owned_lock:
            existing = self._owned_accounts.get(username_key)
            if existing:
                return True, "already_owned"

        token = uuid4().hex
        lease = AccountLockLease(
            username_key=username_key,
            lock_key=self._account_lock_key(username_key),
            meta_key=self._account_meta_key(username_key),
            token=token,
            owner_username=str(owner_username or "").strip().lower(),
            session_id=str(session_id or "").strip(),
        )

        try:
            acquired = self._client.set(
                lease.lock_key,
                token,
                nx=True,
                ex=self.account_lock_ttl_sec,
            )
            if not acquired:
                return False, "already_locked"

            self._client.set(
                lease.meta_key,
                self._lease_meta_json(lease),
                ex=self.account_lock_ttl_sec,
            )

            with self._owned_lock:
                self._owned_accounts[username_key] = lease
            return True, "acquired"
        except Exception as e:
            self.logger.warning(f"Failed to acquire distributed account lock for @{username_key}: {e}")
            if self.fail_closed:
                return False, "redis_error"
            return True, "redis_error_bypass"

    def has_account_lock(self, username: str) -> bool:
        if not self.enabled:
            return True
        if not self._client:
            return not self.fail_closed

        username_key = _normalize_username(username)
        if not username_key:
            return False

        with self._owned_lock:
            return username_key in self._owned_accounts

    def release_account_lock(self, username: str) -> bool:
        username_key = _normalize_username(username)
        if not username_key:
            return False

        with self._owned_lock:
            lease = self._owned_accounts.pop(username_key, None)

        if not lease:
            return False

        if not self.enabled:
            return True

        if not self._client:
            return False

        try:
            result = self._client.eval(
                self._ACCOUNT_RELEASE_SCRIPT,
                2,
                lease.lock_key,
                lease.meta_key,
                lease.token,
            )
            return int(result or 0) == 1
        except Exception as e:
            self.logger.debug(f"Failed to release account lock for @{username_key}: {e}")
            return False

    def release_all_owned(self):
        with self._owned_lock:
            usernames = list(self._owned_accounts.keys())
        for username_key in usernames:
            try:
                self.release_account_lock(username_key)
            except Exception:
                pass

    def claim_target(self, target_username: str, sender_account: str = "", model_username: str = ""):
        target_key = _normalize_username(target_username)
        if not target_key:
            return False, None, "invalid_target"

        if not self.enabled:
            return True, None, "disabled"

        if not self._client:
            if self.fail_closed:
                return False, None, "redis_unavailable"
            return True, None, "redis_unavailable_bypass"

        claim_token = uuid4().hex
        claim_key = self._target_claim_key(target_key)
        try:
            acquired = self._client.set(
                claim_key,
                claim_token,
                nx=True,
                ex=self.target_claim_ttl_sec,
            )
            if not acquired:
                return False, None, "already_claimed"

            return True, claim_token, "claimed"
        except Exception as e:
            self.logger.warning(
                f"Failed to create DM claim for @{target_key} from @{sender_account} on @{model_username}: {e}"
            )
            if self.fail_closed:
                return False, None, "redis_error"
            return True, None, "redis_error_bypass"

    def release_target_claim(self, target_username: str, claim_token: str) -> bool:
        target_key = _normalize_username(target_username)
        token = str(claim_token or "").strip()
        if not target_key or not token:
            return False

        if not self.enabled or not self._client:
            return False

        try:
            result = self._client.eval(
                self._TARGET_RELEASE_SCRIPT,
                1,
                self._target_claim_key(target_key),
                token,
            )
            return int(result or 0) == 1
        except Exception as e:
            self.logger.debug(f"Failed to release DM claim for @{target_key}: {e}")
            return False

    def shutdown(self):
        self._stop_event.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2.0)
        self.release_all_owned()

"""
Cookie manager for per-account session persistence.
Saves, loads, and refreshes cookies to avoid repeated logins.
"""
import json
import os
import time
import logging

from config import database

logger = logging.getLogger("model_dm_bot")

# Keep only login-critical Instagram cookies so stale account-switcher/browser
# state from old sessions is not re-injected.
_IG_AUTH_COOKIE_NAMES = {
    "sessionid",
    "ds_user_id",
    "csrftoken",
    "rur",
    "mid",
    "ig_did",
    "ig_nrcb",
    "shbid",
    "shbts",
    "datr",
    "dpr",
    "wd",
}


def _normalize_cookie_domain(raw_domain) -> str:
    domain = str(raw_domain or "").strip().lower()
    if domain and "instagram.com" in domain:
        return domain
    return ".instagram.com"


def _cookie_is_expired(raw_cookie: dict, now_ts: int) -> bool:
    if not isinstance(raw_cookie, dict):
        return True

    if "expiry" not in raw_cookie:
        return False

    try:
        expiry = int(raw_cookie.get("expiry") or 0)
    except Exception:
        return False

    return bool(expiry and expiry <= int(now_ts))


def _sanitize_cookie(raw_cookie: dict, now_ts: int):
    if not isinstance(raw_cookie, dict):
        return None

    name = str(raw_cookie.get("name") or "").strip()
    value = str(raw_cookie.get("value") or "")
    if not name or value == "":
        return None

    if name.lower() not in _IG_AUTH_COOKIE_NAMES:
        return None

    if _cookie_is_expired(raw_cookie, now_ts):
        return None

    clean = {
        "name": name,
        "value": value,
        "domain": _normalize_cookie_domain(raw_cookie.get("domain")),
        "path": str(raw_cookie.get("path") or "/") or "/",
        "secure": bool(raw_cookie.get("secure", True)),
    }

    if "expiry" in raw_cookie:
        try:
            clean["expiry"] = int(raw_cookie.get("expiry"))
        except Exception:
            pass

    if "httpOnly" in raw_cookie:
        clean["httpOnly"] = bool(raw_cookie.get("httpOnly"))

    same_site = str(raw_cookie.get("sameSite") or "").strip()
    if same_site in ("Strict", "Lax", "None"):
        clean["sameSite"] = same_site

    return clean


def _sanitize_cookie_list(raw_cookies) -> list:
    now_ts = int(time.time())
    sanitized = []
    seen = set()

    rows = raw_cookies if isinstance(raw_cookies, list) else []
    for raw_cookie in rows:
        clean = _sanitize_cookie(raw_cookie, now_ts)
        if not clean:
            continue

        key = (
            clean.get("name", "").lower(),
            clean.get("domain", "").lower(),
            clean.get("path", "/"),
        )
        if key in seen:
            continue
        seen.add(key)
        sanitized.append(clean)

    return sanitized


def cookies_exist(account_name: str) -> bool:
    """Check if a cookie exists mapping for the given account."""
    data = database.get_cookies(account_name)
    return isinstance(data, list) and len(data) > 0


def save_cookies(driver, account_name: str) -> bool:
    """
    Save all browser cookies to a JSON file for the given account.
    
    Args:
        driver: Selenium WebDriver instance
        account_name: Instagram username
    
    Returns:
        True if cookies were saved successfully
    """
    try:
        cookies = _sanitize_cookie_list(driver.get_cookies())
        if not cookies:
            logger.warning(f"[{account_name}] No cookies to save")
            return False

        if not any(str(c.get("name", "")).lower() == "sessionid" for c in cookies):
            logger.warning(f"[{account_name}] No sessionid cookie found; skipping cookie save")
            return False

        database.save_cookies(account_name, cookies)

        logger.info(f"[{account_name}] Saved {len(cookies)} cookies to Database")
        return True
    except Exception as e:
        logger.error(f"[{account_name}] Failed to save cookies: {e}")
        return False


def load_cookies(driver, account_name: str) -> bool:
    """
    Load cookies from file and inject them into the browser.
    The browser must already be on instagram.com before calling this.
    
    Args:
        driver: Selenium WebDriver instance (must be on instagram.com)
        account_name: Instagram username
    
    Returns:
        True if cookies were loaded and injected successfully
    """
    cookies = _sanitize_cookie_list(database.get_cookies(account_name))
    if not cookies:
        logger.info(f"[{account_name}] No cookies found in database")
        return False

    if not any(str(cookie.get("name", "")).lower() == "sessionid" for cookie in cookies):
        logger.info(f"[{account_name}] Stored cookies missing sessionid; cookie login skipped")
        return False

    try:

        # Clear existing cookies first
        driver.delete_all_cookies()

        injected = 0
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
                injected += 1
            except Exception:
                pass  # Some cookies may fail (cross-domain, etc.)

        logger.info(f"[{account_name}] Injected {injected}/{len(cookies)} cookies")
        return injected > 0

    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"[{account_name}] Failed to load cookies: {e}")
        return False


def refresh_cookies(driver, account_name: str) -> bool:
    """
    Re-save cookies after a successful session to extend their lifetime.
    Call this after verified login to keep the session fresh.
    """
    logger.info(f"[{account_name}] Refreshing cookies...")
    return save_cookies(driver, account_name)


def delete_cookies(account_name: str) -> bool:
    """Delete the cookie entry for an account."""
    if cookies_exist(account_name):
        database.save_cookies(account_name, [])
        logger.info(f"[{account_name}] Deleted cookies from Database")
        return True
    return False

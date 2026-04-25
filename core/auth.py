"""
Authentication module — handles login via cookies or credentials,
detects Instagram challenges (2FA, suspicious login, lockout),
and coordinates with the Telegram bot for human intervention.
"""
import time
import random
import logging
import traceback
import os
from enum import Enum

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from config.settings import (
    INSTAGRAM_LOGIN_URL, INSTAGRAM_BASE_URL,
    LOGS_DIR,
)
from config.database import get_required_setting
from core.cookie_manager import save_cookies, load_cookies, delete_cookies, cookies_exist

logger = logging.getLogger("model_dm_bot")


def _setting_float(key: str) -> float:
    value = get_required_setting(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid numeric setting '{key}': {value}")


class ChallengeType(Enum):
    NONE = "none"
    TWO_FACTOR = "two_factor"
    SUSPICIOUS_LOGIN = "suspicious_login"
    LOCKED = "locked"
    CHECKPOINT = "checkpoint"
    UNKNOWN = "unknown"


def human_delay(min_t=None, max_t=None):
    """Random delay to simulate human behavior."""
    min_t = min_t if min_t is not None else _setting_float("ACTION_DELAY_MIN")
    max_t = max_t if max_t is not None else _setting_float("ACTION_DELAY_MAX")
    if max_t < min_t:
        min_t, max_t = max_t, min_t
    time.sleep(random.uniform(min_t, max_t))


def type_like_human(element, text):
    """Type text like a human: char by char, random delays, occasional typos/corrections."""
    typing_min = _setting_float("TYPING_DELAY_MIN")
    typing_max = _setting_float("TYPING_DELAY_MAX")
    if typing_max < typing_min:
        typing_min, typing_max = typing_max, typing_min

    typo_chance = 0.04  # 4% chance of a typo per character
    
    i = 0
    text_str = str(text)
    while i < len(text_str):
        char = text_str[i]
        
        # Simulate typo (only on alphabetic characters for realism)
        if char.isalpha() and random.random() < typo_chance:
            # Type a nearby or random character
            wrong_char = random.choice("abcdefghijklmnopqrstuvwxyz")
            element.send_keys(wrong_char)
            time.sleep(random.uniform(typing_min, typing_max * 1.2))
            
            # Pause as if "realizing" the mistake
            if random.random() < 0.7:
                time.sleep(random.uniform(0.2, 0.6))
            
            # Backspace to fix it
            element.send_keys(Keys.BACKSPACE)
            time.sleep(random.uniform(typing_min, typing_max))
            
            # Now type the correct one
            element.send_keys(char)
        else:
            element.send_keys(char)
            
        # Random delay between characters
        delay = random.uniform(typing_min, typing_max)
        
        # Occasional longer pause between words (space) or at the end of sentences
        if char == ' ':
            delay += random.uniform(0.05, 0.2)
        elif char in ('.', '!', '?'):
            delay += random.uniform(0.3, 0.7)
            
        time.sleep(delay)
        i += 1


def human_scroll(driver, distance):
    """Scroll in small, variable increments to simulate human scrolling."""
    current_scroll = 0
    while current_scroll < distance:
        # Random step size
        step = random.randint(150, 400)
        # Random duration for this step
        duration = random.uniform(0.1, 0.4)
        
        driver.execute_script(f"window.scrollBy(0, {step});")
        current_scroll += step
        time.sleep(duration)
        
        # Occasional "pause to read"
        if random.random() < 0.15:
            time.sleep(random.uniform(1.0, 3.0))


def detect_challenge(driver) -> ChallengeType:
    """
    Detect if Instagram is showing a challenge/verification screen.
    
    Returns:
        ChallengeType enum indicating what kind of challenge is present
    """
    current_url = driver.current_url.lower()

    # Check URL patterns
    if "challenge" in current_url:
        return ChallengeType.CHECKPOINT
    if "two_factor" in current_url or "codeentry" in current_url:
        return ChallengeType.TWO_FACTOR
    if "suspended" in current_url or "help" in current_url:
        return ChallengeType.LOCKED

    # Check for on-page challenge elements
    try:
        page_source = driver.page_source.lower()

        if "security code" in page_source or "verification code" in page_source:
            return ChallengeType.TWO_FACTOR
        if "suspicious" in page_source or "unusual login" in page_source:
            return ChallengeType.SUSPICIOUS_LOGIN
        if "your account has been locked" in page_source or "we detected an unusual login" in page_source:
            return ChallengeType.LOCKED
        if "confirm it" in page_source and "challenge" in current_url:
            return ChallengeType.CHECKPOINT
    except Exception:
        pass

    return ChallengeType.NONE


def _has_sessionid_cookie(driver) -> bool:
    """Return True when browser currently has a non-empty Instagram sessionid cookie."""
    try:
        cookies = driver.get_cookies()
    except Exception:
        return False

    for cookie in cookies:
        name = str(cookie.get("name", "")).strip().lower()
        if name != "sessionid":
            continue

        value = str(cookie.get("value", "") or "").strip()
        if value:
            return True

    return False


def is_logged_in(driver) -> bool:
    """Check if we're currently logged into Instagram."""
    try:
        current_url = str(driver.current_url or "").lower()

        # Explicit unauthenticated surfaces.
        unauth_url_markers = (
            "/accounts/login",
            "/accounts/onetap",
            "/accounts/emailsignup",
            "/accounts/password/reset",
        )
        if any(marker in current_url for marker in unauth_url_markers):
            return False

        challenge = detect_challenge(driver)
        if challenge != ChallengeType.NONE:
            return False

        # If login form fields are visible, we are still logged out.
        try:
            login_form_fields = driver.find_elements(
                By.CSS_SELECTOR,
                "input[name='username'], input[name='email'], input[name='password'], input[name='pass']",
            )
        except Exception:
            login_form_fields = []

        for field in login_form_fields:
            try:
                if field.is_displayed():
                    return False
            except Exception:
                continue

        # Require authenticated browser cookie state.
        if not _has_sessionid_cookie(driver):
            return False

        # Look for logged-in indicators
        indicators = [
            "//a[contains(@href, '/direct/')]",
            "//*[@aria-label='Home']",
            "//*[@aria-label='New post']",
            "//a[contains(@href, '/explore/')]",
        ]
        for xpath in indicators:
            try:
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                return True
            except Exception:
                continue

        # Fallback: URL/login-form/challenge checks plus sessionid cookie are enough.
        return True
    except Exception:
        return False


def login_with_cookies(driver, account: dict) -> bool:
    """
    Attempt to log into Instagram by injecting saved cookies.
    
    Args:
        driver: WebDriver instance
        account: dict with 'username' key
    
    Returns:
        True if cookie login was successful
    """
    username = account["username"]

    if not cookies_exist(username):
        logger.info(f"[{username}] No cookies found, skipping cookie login")
        return False

    logger.info(f"[{username}] Attempting cookie login...")

    # Navigate to Instagram first (required before setting cookies)
    driver.get(INSTAGRAM_BASE_URL)
    human_delay(2, 4)

    # Ensure the browser state is clean before cookie injection.
    try:
        driver.delete_all_cookies()
    except Exception:
        pass
    try:
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd(
            "Storage.clearDataForOrigin",
            {
                "origin": INSTAGRAM_BASE_URL.rstrip("/"),
                "storageTypes": "all",
            },
        )
    except Exception:
        pass

    # Inject cookies
    if not load_cookies(driver, username):
        logger.warning(f"[{username}] Failed to inject cookies")
        return False

    if not _has_sessionid_cookie(driver):
        logger.warning(f"[{username}] Cookie injection did not apply sessionid in browser")
        delete_cookies(username)
        return False

    # Reload page with cookies
    driver.get(INSTAGRAM_BASE_URL)
    human_delay(3, 5)

    # Check if login worked
    if is_logged_in(driver):
        logger.info(f"[{username}] ✅ Cookie login successful!")
        # Refresh cookies to extend session
        save_cookies(driver, username)
        return True

    # Check for challenges
    challenge = detect_challenge(driver)
    if challenge != ChallengeType.NONE:
        logger.warning(f"[{username}] Cookie login triggered challenge: {challenge.value}")
        return False

    logger.warning(f"[{username}] Cookie login failed (session may be expired)")
    delete_cookies(username)
    return False


def login_with_credentials(driver, account: dict) -> bool:
    """
    Log into Instagram with username/password with human-like typing.
    
    Args:
        driver: WebDriver instance
        account: dict with 'username' and 'password' keys
    
    Returns:
        True if login was successful (may still need challenge handling)
    """
    username = account["username"]
    password = account["password"]

    logger.info(f"[{username}] Logging in with credentials...")

    try:
        driver.get(INSTAGRAM_LOGIN_URL)
        logger.info(f"[{username}] Navigated to login page, waiting for load...")
    except Exception as e:
        logger.error(f"[{username}] Failed to navigate to login page: {e}")
        return False

    # Wait for page to fully load
    try:
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        logger.info(f"[{username}] Page loaded, current URL: {driver.current_url}")
    except Exception as e:
        logger.error(f"[{username}] Page load timeout: {e}")
        return False

    human_delay(3, 5)

    # Accept cookies banner if present
    _accept_cookie_banner(driver)
    _open_login_form_if_account_picker(driver, username)

    try:
        # Wait for login form
        logger.info(f"[{username}] Looking for login form...")
        username_input, password_input = _locate_login_form_inputs(driver, username)
        logger.info(f"[{username}] Found login form fields")

        # Clear and type username
        username_input.clear()
        human_delay(0.5, 1)
        type_like_human(username_input, username)
        logger.info(f"[{username}] Typed username")
        human_delay(0.5, 1)

        # Clear and type password
        password_input.clear()
        human_delay(0.5, 1)
        type_like_human(password_input, password)
        logger.info(f"[{username}] Typed password")
        human_delay(1, 2)

        # Submit
        password_input.send_keys(Keys.RETURN)
        human_delay(0.5, 1)
        try:
            # Fallback to intentionally click the login button just in case ENTER didn't submit
            button_selectors = [
                "button[type='submit']",
                "div[role='button'][aria-label='Log in']",
                "div[role='button'][aria-label='Log In']"
            ]
            for sel in button_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                if elements and elements[0].is_displayed():
                    # Only click if it's not disabled
                    if elements[0].get_attribute("aria-disabled") != "true":
                        driver.execute_script("arguments[0].click();", elements[0])
                        break
        except Exception:
            pass
        
        logger.info(f"[{username}] Submitted login form, waiting for response...")

        # Wait for page to change (up to 30 seconds for slow connections)
        try:
            WebDriverWait(driver, 30).until(
                lambda d: "/accounts/login" not in d.current_url
                or detect_challenge(d) != ChallengeType.NONE
            )
        except TimeoutException:
            logger.info(f"[{username}] Still on login page after 30s, checking state...")

        human_delay(3, 5)
        logger.info(f"[{username}] Post-login URL: {driver.current_url}")

        # Dismiss popups
        _dismiss_post_login_popups(driver)

        # Check result
        if is_logged_in(driver):
            logger.info(f"[{username}] ✅ Credential login successful!")
            save_cookies(driver, username)
            return True

        # Check for challenges
        challenge = detect_challenge(driver)
        if challenge != ChallengeType.NONE:
            logger.warning(f"[{username}] Login triggered challenge: {challenge.value}")
            return False

        logger.warning(f"[{username}] Login may have failed — URL: {driver.current_url}")
        return False

    except TimeoutException as e:
        logger.error(f"[{username}] Timeout waiting for login elements: {e}")
        try:
            screenshot_path = os.path.join(LOGS_DIR, f"login_error_{username}.png")
            driver.save_screenshot(screenshot_path)
            logger.info(f"[{username}] Error screenshot saved to: {screenshot_path}")
        except Exception:
            pass
        return False

    except Exception as e:
        logger.error(f"[{username}] Login error: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        return False


def handle_two_factor(driver, account: dict, code: str) -> bool:
    """
    Submit a 2FA verification code on the Instagram challenge screen.
    
    Args:
        driver: WebDriver on the 2FA screen
        account: dict with 'username'
        code: 6-digit verification code from employee
    
    Returns:
        True if 2FA was successfully verified
    """
    username = account["username"]
    logger.info(f"[{username}] Submitting 2FA code: {code}")

    try:
        # Find the code input field
        code_input = None
        selectors = [
            (By.NAME, "verificationCode"),
            (By.NAME, "security_code"),
            (By.NAME, "email"),
            (By.XPATH, "//input[@name='verificationCode']"),
            (By.XPATH, "//input[@name='security_code']"),
            (By.XPATH, "//input[@name='email']"),
            (By.XPATH, "//input[contains(@placeholder, 'Code')]"),
            (By.XPATH, "//input[@type='number']"),
            (By.XPATH, "//input[contains(@aria-label, 'code')]"),
        ]

        for by, selector in selectors:
            try:
                code_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((by, selector))
                )
                if code_input:
                    break
            except Exception:
                continue

        if not code_input:
            logger.error(f"[{username}] Could not find 2FA code input field")
            return False

        code_input.clear()
        type_like_human(code_input, code)
        human_delay(1, 2)

        # Click confirm/submit button
        submit_selectors = [
            "//button[contains(text(), 'Confirm')]",
            "//button[contains(text(), 'Submit')]",
            "//button[contains(text(), 'Verify')]",
            "//div[@role='button' and contains(., 'Continue')]",
            "//div[@role='button']//span[contains(text(), 'Continue')]",
            "//button[@type='button' and not(contains(text(), 'Back'))]",
        ]
        for xpath in submit_selectors:
            try:
                btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                # Sometimes div buttons need JS click if they are obstructed
                driver.execute_script("arguments[0].click();", btn)
                break
            except Exception:
                continue

        human_delay(5, 8)
        _dismiss_post_login_popups(driver)

        if is_logged_in(driver):
            logger.info(f"[{username}] ✅ 2FA verification successful!")
            save_cookies(driver, username)
            return True

        logger.warning(f"[{username}] 2FA code may have been incorrect")
        return False

    except Exception as e:
        logger.error(f"[{username}] 2FA handling error: {e}")
        return False


def _accept_cookie_banner(driver):
    """Accept the cookie consent banner if it appears (common in EU regions)."""
    cookie_selectors = [
        "//button[contains(text(), 'Allow')]" ,
        "//button[contains(text(), 'Accept')]" ,
        "//button[contains(text(), 'allow essential and optional cookies')]" ,
        "//button[contains(text(), 'Allow all cookies')]" ,
    ]
    for xpath in cookie_selectors:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", btn)
            human_delay(1, 2)
            logger.info("Accepted cookie banner")
            return
        except Exception:
            continue


def _open_login_form_if_account_picker(driver, username: str = "") -> bool:
    """Open the username/password login form when account-switcher UI is shown."""
    chooser_xpaths = [
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'use another profile')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'use another account')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'log into another account')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'switch accounts')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'use another profile')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'use another account')]",
        "//div[@role='button' and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'use another')]",
    ]

    for xpath in chooser_xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
        except Exception:
            continue

        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                driver.execute_script("arguments[0].click();", element)
                human_delay(1, 2)
                if username:
                    logger.info(f"[{username}] Opened login form from account picker")
                else:
                    logger.info("Opened login form from account picker")
                return True
            except Exception:
                continue

    return False


def _locate_login_form_inputs(driver, username: str):
    """Locate login form fields, retrying once after dismissing account picker UI."""
    last_error = None
    for attempt in range(2):
        try:
            timeout = 20 if attempt == 0 else 12
            username_input = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], input[name='email']"))
            )
            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='password'], input[name='pass']"))
            )
            return username_input, password_input
        except TimeoutException as exc:
            last_error = exc
            if attempt == 0 and _open_login_form_if_account_picker(driver, username):
                continue
            break

    if last_error:
        raise last_error

    raise TimeoutException("Login form fields not found")


def _dismiss_post_login_popups(driver):
    """Dismiss common post-login popups (Save Login, Notifications)."""
    popup_xpaths = [
        "//button[contains(text(), 'Not Now')]",
        "//button[normalize-space()='Not Now']",
        "//button[contains(text(), 'Save Info')]",
        "//div[contains(@class, '_a9-z')]//button[normalize-space()='Not Now']",
    ]

    for xpath in popup_xpaths:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", btn)
            human_delay(1, 2)
            logger.info("Dismissed popup")
        except Exception:
            continue

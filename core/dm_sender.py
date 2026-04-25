"""
DM sender module — navigates to a user's profile, opens the DM dialog,
types and sends a message with human-like behavior.
"""
import time
import random
import logging
import pyperclip

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config.settings import (
    INSTAGRAM_BASE_URL,
)
from config.database import get_required_setting, get_setting
from core.auth import human_delay, type_like_human

logger = logging.getLogger("model_dm_bot")
EMOJI_SUFFIX_ENABLED_KEY = "DM_RANDOM_EMOJI_SUFFIX_ENABLED"
EMOJI_SUFFIX_POOL_KEY = "DM_RANDOM_EMOJI_SUFFIX_POOL"
DEFAULT_EMOJI_SUFFIX_POOL = ["🙂", "😊", "😉", "✨", "🌸", "🙏"]


def _setting_float(key: str) -> float:
    value = get_required_setting(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid numeric setting '{key}': {value}")


def _setting_bool(key: str, default: bool = False) -> bool:
    value = get_setting(key, default)
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


def _setting_string_list(key: str, default=None) -> list:
    fallback = list(default or [])
    raw_value = get_setting(key, fallback)

    if isinstance(raw_value, list):
        clean_items = [str(item).strip() for item in raw_value if str(item).strip()]
        return clean_items if clean_items else fallback

    if isinstance(raw_value, str):
        clean_items = [item.strip() for item in raw_value.split(",") if item.strip()]
        return clean_items if clean_items else fallback

    return fallback


def _with_random_emoji_suffix(message: str) -> str:
    clean_message = str(message or "").strip()
    if not clean_message:
        return clean_message

    if not _setting_bool(EMOJI_SUFFIX_ENABLED_KEY, default=False):
        return clean_message

    emoji_pool = _setting_string_list(EMOJI_SUFFIX_POOL_KEY, default=DEFAULT_EMOJI_SUFFIX_POOL)
    if not emoji_pool:
        return clean_message

    return f"{clean_message} {random.choice(emoji_pool)}"


class DMResult:
    """Result status for a DM attempt."""
    SENT = "sent"
    ALREADY_SENT = "already_sent"
    CANT_MESSAGE = "cant_message"
    USER_NOT_FOUND = "user_not_found"
    ERROR = "error"


def send_dm(driver, username: str, message: str) -> str:
    """
    Send a direct message to a user via the new message modal flow.
    
    Args:
        driver: WebDriver instance (must be logged in)
        username: Target user's Instagram username
        message: The message text to send
    
    Returns:
        DMResult status string
    """
    logger.info(f"[DM] Initiating DM flow for @{username}...")

    try:
        # Step 1: Navigate to inbox and click "Send message" button
        driver.get(f"{INSTAGRAM_BASE_URL}/direct/inbox/")
        human_delay(3, 5)
        _dismiss_popups(driver)

        # Click the "Send message" button to open the new message modal
        try:
            send_msg_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                    "//div[@role='button' and contains(text(), 'Send message')]"
                    " | //div[@role='button'][contains(., 'Send message')]"
                ))
            )
            driver.execute_script("arguments[0].click();", send_msg_btn)
            human_delay(2, 3)
        except TimeoutException:
            logger.warning(f"[DM] 'Send message' button not found, trying compose icon...")
            # Fallback: try the compose/pencil icon
            try:
                compose_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH,
                        "//*[@aria-label='New message']//ancestor::*[@role='button']"
                        " | //a[contains(@href, '/direct/new')]"
                    ))
                )
                driver.execute_script("arguments[0].click();", compose_btn)
                human_delay(2, 3)
            except TimeoutException:
                logger.error(f"[DM] Could not open new message dialog for @{username}")
                return DMResult.ERROR

        # Step 2: Type user name in the search box
        logger.info(f"[DM] Searching for user @{username}...")
        try:
            query_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "queryBox"))
            )
            # Clear thoroughly using JS since background tabs ignore Ctrl+A
            query_box.click()
            human_delay(0.3, 0.5)
            driver.execute_script("arguments[0].value = ''; arguments[0].dispatchEvent(new Event('input', {bubbles:true}));", query_box)
            query_box.clear()
            human_delay(0.5, 1)
            type_like_human(query_box, username)
            human_delay(3, 5)
        except TimeoutException:
            logger.error(f"[DM] Could not find recipient search box for @{username}")
            return DMResult.ERROR

        # Step 3: Select first from list
        logger.info(f"[DM] Selecting user @{username} from search results...")
        try:
            # In unfocused tabs, React renders very slowly. 
            # We MUST wait for the actual username text to appear in the modal before clicking checkboxes, 
            # otherwise it clicks the first "Suggested" user from the stale results!
            username_xpath = f"//div[@role='dialog']//span[text()='{username}' or text()='{username.lower()}']"
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, username_xpath))
            )
            
            checkbox = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.NAME, "ContactSearchResultCheckbox"))
            )
            # Find the parent wrapper to click safely
            parent_clickable = driver.execute_script(
                "return arguments[0].closest('[role=\"button\"]') || arguments[0].parentElement;", checkbox
            )
            driver.execute_script("arguments[0].click();", parent_clickable)
            human_delay(1, 2)
        except TimeoutException:
            logger.warning(f"[DM] User @{username} not found in search results")
            return DMResult.USER_NOT_FOUND

        # Step 4: Click 'Chat' button
        logger.info(f"[DM] Clicking Chat button...")
        try:
            chat_btn = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='button']//span[text()='Chat' or text()='Next'] | //div[contains(@class, 'x1i10hfl') and contains(., 'Chat')]"))
            )
            driver.execute_script("arguments[0].click();", chat_btn)
            human_delay(4, 6)
        except TimeoutException:
            logger.error(f"[DM] Could not click Chat button for @{username}")
            return DMResult.ERROR

        # Step 5: Type message
        logger.info(f"[DM] Typing message to @{username}...")
        text_area = _find_message_input(driver)
        if not text_area:
            logger.error(f"[DM] Could not find message input for @{username} (might be restricted)")
            return DMResult.CANT_MESSAGE

        try:
            text_area.click()
            human_delay(0.5, 1)
        except Exception:
            driver.execute_script("arguments[0].focus();", text_area)
            human_delay(0.5, 1)

        final_message = _with_random_emoji_suffix(message)

        # User requested: "fully like human ... don't copy paste"
        # We use advanced typing which includes simulated typos and backspacing.
        type_like_human(text_area, final_message)
        human_delay(1, 2)
        logger.info(f"[DM] Message typed like human for @{username}")

        # Step 6: Submit using Enter key (more reliable than UI Send button in long runs)
        logger.info(f"[DM] Submitting message with Enter key...")
        if _send_message(driver, text_area, expected_message=final_message):
            logger.info(f"[DM] ✅ Message sent to @{username}")
            return DMResult.SENT
        else:
            logger.error(f"[DM] Failed to submit message with Enter for @{username}")
            return DMResult.ERROR

    except Exception as e:
        logger.error(f"[DM] Unexpected error sending DM to @{username}: {e}")
        return DMResult.ERROR


def _find_message_input(driver, timeout_seconds: float = 8):
    """Find the DM text input/textarea element."""
    input_selectors = [
        "//div[@aria-label='Message' and @role='textbox']",
        "//div[@role='textbox' and @contenteditable='true']",
        "//textarea[contains(@placeholder, 'Message')]",
        "//textarea[contains(@placeholder, 'message')]",
        "//div[@role='dialog']//textarea",
        "//div[contains(@class, 'x1i10hfl')]//p",
        "//textarea",
    ]

    for xpath in input_selectors:
        try:
            elem = WebDriverWait(driver, timeout_seconds).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            if elem:
                return elem
        except Exception:
            continue

    return None


def _normalize_message_text(text: str) -> str:
    return " ".join(str(text or "").split()).strip().lower()


def _extract_input_text(driver, element):
    """Read current draft text from textarea/contenteditable input."""
    if element is None:
        return None

    try:
        value = element.get_attribute("value")
        if value is not None:
            return str(value)
    except Exception:
        pass

    try:
        raw_text = driver.execute_script(
            """
            const el = arguments[0];
            if (!el) return null;
            if (typeof el.value === 'string') return el.value;
            return (el.innerText || el.textContent || '');
            """,
            element,
        )
        if raw_text is not None:
            return str(raw_text)
    except Exception:
        pass

    try:
        return str(element.text or "")
    except Exception:
        return None


def _read_message_draft(driver, text_area):
    """Best-effort read of current message draft text; None means unreadable."""
    draft = _extract_input_text(driver, text_area)
    if draft is not None:
        return str(draft).strip()

    refreshed_input = _find_message_input(driver, timeout_seconds=2)
    if refreshed_input:
        refreshed_draft = _extract_input_text(driver, refreshed_input)
        if refreshed_draft is not None:
            return str(refreshed_draft).strip()

    return None


def _is_submit_confirmed(driver, text_area, baseline_text: str = "") -> bool:
    """Treat submit as successful only when draft text is cleared."""
    current_draft = _read_message_draft(driver, text_area)
    if current_draft is None:
        return False

    if not current_draft.strip():
        return True

    baseline_norm = _normalize_message_text(baseline_text)
    current_norm = _normalize_message_text(current_draft)
    return bool(baseline_norm) and current_norm != baseline_norm


def _click_send_button(driver) -> bool:
    send_btn_xpaths = [
        "//div[@role='button'][.//span[normalize-space()='Send']]",
        "//button[normalize-space()='Send']",
        "//*[@aria-label='Send' and (@role='button' or self::button)]",
        "//div[@role='button' and normalize-space()='Send']",
    ]

    for xpath in send_btn_xpaths:
        try:
            button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", button)
            return True
        except Exception:
            continue

    return False


def _send_message(driver, text_area, expected_message: str = "") -> bool:
    """Submit message and only report success once submit is confirmed."""
    try:
        baseline_text = str(expected_message or _read_message_draft(driver, text_area) or "").strip()

        for attempt in range(2):
            # Ensure focus before sending Enter.
            try:
                text_area.click()
            except Exception:
                try:
                    driver.execute_script("arguments[0].focus();", text_area)
                except Exception:
                    pass

            human_delay(0.4, 0.9)

            submitted = False
            for key in (Keys.ENTER, Keys.RETURN):
                try:
                    text_area.send_keys(key)
                    submitted = True
                    break
                except Exception:
                    continue

            if not submitted:
                try:
                    active = driver.switch_to.active_element
                    active.send_keys(Keys.ENTER)
                    submitted = True
                except Exception:
                    submitted = False

            if submitted:
                human_delay(1.0, 1.8)
                if _is_submit_confirmed(driver, text_area, baseline_text=baseline_text):
                    return True

            # Enter can fail silently in some sessions; try explicit Send button.
            if _click_send_button(driver):
                human_delay(1.0, 1.8)
                if _is_submit_confirmed(driver, text_area, baseline_text=baseline_text):
                    return True

            logger.warning(f"[DM] Submit attempt {attempt + 1} not confirmed")

        return False

    except Exception as e:
        logger.warning(f"[DM] Error in _send_message: {e}")
        return False


def _dismiss_popups(driver):
    """Dismiss common popups (Not Now, notifications, etc.)."""
    popup_xpaths = [
        "//button[contains(text(), 'Not Now')]",
        "//button[normalize-space()='Not Now']",
        "//div[contains(@class, '_a9-z')]//button[normalize-space()='Not Now']",
    ]
    for xpath in popup_xpaths:
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", btn)
            human_delay(0.5, 1)
        except Exception:
            continue


def wait_between_dms(stop_event=None):
    """Random delay between DMs to appear human-like."""
    dm_delay_min = _setting_float("DM_DELAY_MIN")
    dm_delay_max = _setting_float("DM_DELAY_MAX")
    if dm_delay_max < dm_delay_min:
        dm_delay_min, dm_delay_max = dm_delay_max, dm_delay_min

    delay = random.uniform(dm_delay_min, dm_delay_max)
    logger.info(f"[DM] Waiting {delay:.0f}s before next DM...")

    end_time = time.time() + delay
    while time.time() < end_time:
        if stop_event and stop_event.is_set():
            return
        time.sleep(min(0.5, max(0.0, end_time - time.time())))

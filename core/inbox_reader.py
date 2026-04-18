"""
Inbox reader module — scrapes unread DM threads from the Instagram inbox
using Selenium. Returns structured data for the dashboard to display.
"""
import time
import re
import logging
import os
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config.settings import INSTAGRAM_BASE_URL
from core.auth import human_delay

logger = logging.getLogger("model_dm_bot")

# ── Constants ──
INBOX_URL = f"{INSTAGRAM_BASE_URL}/direct/inbox/"
MAX_SCROLL_ATTEMPTS = 260
THREAD_LOAD_WAIT = 8
SCROLL_END_STABLE_HITS = 3
MESSAGE_SCROLL_ATTEMPTS = 220
MESSAGE_SCROLL_STABLE_HITS = 3
NEW_MESSAGE_COUNT_RE = re.compile(r"\b(\d+)\s+new\s+messages?\b", re.IGNORECASE)
NEW_MESSAGE_MARKER_RE = re.compile(r"\bnew\s+messages?\b", re.IGNORECASE)


def _parse_new_message_count(raw_text: str) -> int:
    """Parse inbox badge text like '2 new messages' into a numeric count."""
    clean_text = " ".join(str(raw_text or "").split())
    if not clean_text:
        return 0

    count_match = NEW_MESSAGE_COUNT_RE.search(clean_text)
    if count_match:
        try:
            return max(0, int(count_match.group(1)))
        except Exception:
            return 0

    if NEW_MESSAGE_MARKER_RE.search(clean_text):
        return 1

    return 0


def _dismiss_inbox_popups(driver):
    """Dismiss common Instagram popups (notifications, etc.)."""
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


def _parse_relative_time(time_text: str) -> str:
    """Convert Instagram relative time (1d, 5d, 1w, etc.) to a readable string."""
    clean = str(time_text or "").strip().lower()
    if not clean:
        return ""
    return clean


def _parse_instagram_timestamp(raw_text: str, now_dt: datetime = None):
    """Best-effort parse of Instagram time labels into local naive datetimes."""
    clean_text = " ".join(str(raw_text or "").split())
    if not clean_text:
        return None

    reference_now = now_dt or datetime.now()

    normalized_iso = clean_text.replace("Z", "+00:00")
    try:
        parsed_iso = datetime.fromisoformat(normalized_iso)
        if parsed_iso.tzinfo is not None:
            try:
                return parsed_iso.astimezone().replace(tzinfo=None)
            except Exception:
                return parsed_iso.replace(tzinfo=None)
        return parsed_iso
    except Exception:
        pass

    lowered = clean_text.strip().lower().replace("ago", "").strip()
    if lowered in ("now", "just now"):
        return reference_now

    if lowered in ("today",):
        return reference_now
    if lowered in ("yesterday",):
        return reference_now - timedelta(days=1)

    relative_match = re.match(
        r"^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|wk|wks|week|weeks)$",
        lowered,
    )
    if relative_match:
        amount = max(0, int(relative_match.group(1)))
        unit = relative_match.group(2)

        if unit.startswith("s"):
            return reference_now - timedelta(seconds=amount)
        if unit.startswith("m"):
            return reference_now - timedelta(minutes=amount)
        if unit.startswith("h"):
            return reference_now - timedelta(hours=amount)
        if unit.startswith("d"):
            return reference_now - timedelta(days=amount)
        return reference_now - timedelta(weeks=amount)

    def _parse_clock_time(text_value: str, base_day: datetime):
        raw = str(text_value or "").strip().upper()
        for pattern in ("%I:%M %p", "%I %p", "%H:%M"):
            try:
                parsed = datetime.strptime(raw, pattern)
                return base_day.replace(
                    hour=parsed.hour,
                    minute=parsed.minute,
                    second=0,
                    microsecond=0,
                )
            except Exception:
                continue
        return None

    if lowered.startswith("today at "):
        return _parse_clock_time(clean_text[9:].strip(), reference_now)

    if lowered.startswith("yesterday at "):
        base = reference_now - timedelta(days=1)
        return _parse_clock_time(clean_text[13:].strip(), base)

    parsed_clock = _parse_clock_time(clean_text, reference_now)
    if parsed_clock is not None:
        if parsed_clock > (reference_now + timedelta(minutes=2)):
            return parsed_clock - timedelta(days=1)
        return parsed_clock

    return None


def _scroll_inbox_threads_container(driver):
    """Scroll the DM thread list once and report movement/end state."""
    try:
        result = driver.execute_script(
            """
            function findThreadContainer() {
                var candidates = Array.from(document.querySelectorAll('div')).filter(function (el) {
                    if (!el) return false;
                    if ((el.scrollHeight || 0) <= (el.clientHeight || 0) + 2) return false;
                    if (!el.querySelector('img[alt="user-profile-picture"]')) return false;
                    return true;
                });
                candidates.sort(function (a, b) {
                    return (b.scrollHeight || 0) - (a.scrollHeight || 0);
                });
                return candidates.length ? candidates[0] : null;
            }

            var container = findThreadContainer();
            if (!container) {
                var main = document.querySelector('[role="main"]');
                if (!main) return { moved: false, atEnd: true };

                var beforeTopMain = main.scrollTop || 0;
                var maxTopMain = Math.max(0, (main.scrollHeight || 0) - (main.clientHeight || 0));
                var stepMain = Math.max(220, Math.floor((main.clientHeight || 0) * 0.9));
                main.scrollTop = Math.min(maxTopMain, beforeTopMain + stepMain);

                return {
                    moved: Math.abs((main.scrollTop || 0) - beforeTopMain) > 2,
                    atEnd: (main.scrollTop || 0) >= (maxTopMain - 2)
                };
            }

            var beforeTop = container.scrollTop || 0;
            var maxTop = Math.max(0, (container.scrollHeight || 0) - (container.clientHeight || 0));
            var step = Math.max(280, Math.floor((container.clientHeight || 0) * 0.9));
            container.scrollTop = Math.min(maxTop, beforeTop + step);

            return {
                moved: Math.abs((container.scrollTop || 0) - beforeTop) > 2,
                atEnd: (container.scrollTop || 0) >= (maxTop - 2)
            };
            """
        )

        if isinstance(result, dict):
            return bool(result.get("moved", False)), bool(result.get("atEnd", False))
    except Exception:
        pass

    return False, True


def _scroll_thread_messages_container_up(driver):
    """Scroll the current conversation container up once to load older rows."""
    try:
        result = driver.execute_script(
            """
            function resolveChatContainer() {
                var anchor = document.querySelector('div[role="row"]') || document.querySelector('div[role="listbox"] > div');
                if (!anchor) return null;

                var node = anchor.parentElement;
                while (node && node !== document.body) {
                    if ((node.scrollHeight || 0) > ((node.clientHeight || 0) + 2)) {
                        return node;
                    }
                    node = node.parentElement;
                }
                return null;
            }

            var rowCount = document.querySelectorAll('div[role="row"]').length;
            if (!rowCount) {
                rowCount = document.querySelectorAll('div[role="listbox"] > div').length;
            }

            var container = resolveChatContainer();
            if (!container) {
                return { moved: false, atTop: true, rowCount: rowCount };
            }

            var beforeTop = container.scrollTop || 0;
            var step = Math.max(240, Math.floor((container.clientHeight || 0) * 0.92));
            container.scrollTop = Math.max(0, beforeTop - step);

            var newRowCount = document.querySelectorAll('div[role="row"]').length;
            if (!newRowCount) {
                newRowCount = document.querySelectorAll('div[role="listbox"] > div').length;
            }

            return {
                moved: Math.abs((container.scrollTop || 0) - beforeTop) > 2,
                atTop: (container.scrollTop || 0) <= 2,
                rowCount: newRowCount
            };
            """
        )

        if isinstance(result, dict):
            moved = bool(result.get("moved", False))
            at_top = bool(result.get("atTop", False))
            row_count = 0
            try:
                row_count = int(result.get("rowCount") or 0)
            except Exception:
                row_count = 0
            return moved, at_top, row_count
    except Exception:
        pass

    return False, True, 0


def _get_oldest_visible_thread_timestamp(driver):
    """Return timestamp metadata from the last visible thread row in inbox list."""
    try:
        result = driver.execute_script(
            """
            var buttons = Array.from(document.querySelectorAll('div[role="button"]')).filter(function (el) {
                return !!el.querySelector('img[alt="user-profile-picture"]');
            });
            if (!buttons.length) {
                return { timestamp: '', label: '' };
            }

            var candidate = buttons[buttons.length - 1];
            var timeEl = candidate.querySelector('abbr[aria-label], time[datetime], time');
            if (!timeEl) {
                return { timestamp: '', label: '' };
            }

            return {
                timestamp: (timeEl.getAttribute('datetime') || (timeEl.textContent || '').trim() || ''),
                label: (timeEl.getAttribute('aria-label') || '')
            };
            """
        )
        if isinstance(result, dict):
            return str(result.get("timestamp", "") or "").strip(), str(result.get("label", "") or "").strip()
    except Exception:
        pass

    return "", ""


def _get_oldest_visible_message_timestamp(driver) -> str:
    """Return timestamp text from the oldest currently visible message row."""
    try:
        result = driver.execute_script(
            """
            var rows = Array.from(document.querySelectorAll('div[role="row"]'));
            if (!rows.length) {
                rows = Array.from(document.querySelectorAll('div[role="listbox"] > div'));
            }

            for (var i = 0; i < rows.length; i++) {
                var row = rows[i];
                if (!row) continue;
                var timeEl = row.querySelector('time[datetime], time, abbr[aria-label]');
                if (!timeEl) continue;

                var value = timeEl.getAttribute('datetime') || timeEl.getAttribute('aria-label') || (timeEl.textContent || '').trim() || '';
                if (value) return value;
            }

            return '';
            """
        )
        return str(result or "").strip()
    except Exception:
        return ""


def _extract_thread_data_from_button(button_el, driver) -> dict:
    """Extract thread data from a single thread button element.
    
    Based on the actual Instagram DM inbox DOM structure:
    - Display name: span[title] inside the button
    - Message preview: second text line after the display name
    - Timestamp: abbr[aria-label] element
    - Profile picture: img[alt='user-profile-picture']
    - Unread indicator: div containing 'Unread' text
    """
    thread = {
        "display_name": "",
        "username": "",
        "message_preview": "",
        "timestamp": "",
        "timestamp_label": "",
        "profile_pic_url": "",
        "is_unread": False,
        "new_message_count": 0,
    }

    try:
        # 1. Extract display name from span[title]
        try:
            name_el = button_el.find_element(By.CSS_SELECTOR, "span[title]")
            thread["display_name"] = name_el.get_attribute("title") or ""
        except NoSuchElementException:
            pass

        # 2. Extract profile picture URL
        try:
            img_el = button_el.find_element(
                By.CSS_SELECTOR, "img[alt='user-profile-picture']"
            )
            thread["profile_pic_url"] = img_el.get_attribute("src") or ""
        except NoSuchElementException:
            pass

        # 3. Extract timestamp from abbr[aria-label]
        try:
            abbr_el = button_el.find_element(By.CSS_SELECTOR, "abbr[aria-label]")
            thread["timestamp_label"] = abbr_el.get_attribute("aria-label") or ""
            # Also get the short text (1d, 5d, 1w, etc.)
            thread["timestamp"] = abbr_el.text.strip() if abbr_el.text else ""
        except NoSuchElementException:
            pass

        # 4. Check if thread is unread
        try:
            unread_els = button_el.find_elements(
                By.XPATH,
                ".//*[contains(translate(normalize-space(.), 'UNREAD', 'unread'), 'unread')]",
            )
            thread["is_unread"] = len(unread_els) > 0
        except Exception:
            pass

        # 4b. Parse "2 new messages" / "3 new message" badges from row text.
        try:
            row_text = " ".join(str(button_el.text or "").split())
        except Exception:
            row_text = ""

        badge_count = _parse_new_message_count(row_text)
        if badge_count > 0:
            thread["new_message_count"] = badge_count
            thread["is_unread"] = True

        # 5. Extract message preview — it's the text content after the display name
        #    but before the timestamp. We use JS to get all visible text spans.
        try:
            preview_text = driver.execute_script("""
                var button = arguments[0];
                // Find the display name span
                var nameSpan = button.querySelector('span[title]');
                if (!nameSpan) return '';
                
                // Navigate up to find the container with message preview
                // The preview is in a sibling/cousin div after the name
                var container = nameSpan.closest('[class]');
                while (container && container !== button) {
                    var nextSibling = container.nextElementSibling;
                    if (nextSibling) {
                        // Look for text content spans, skip timestamp abbr
                        var spans = nextSibling.querySelectorAll('span');
                        for (var i = 0; i < spans.length; i++) {
                            var span = spans[i];
                            // Skip if it contains abbr (timestamp) or 'Unread'
                            if (span.querySelector('abbr')) continue;
                            if (span.textContent.trim() === 'Unread') continue;
                            if (span.textContent.trim() === '·') continue;
                            if (span.textContent.trim() === '') continue;
                            // Skip the display name itself
                            if (span.getAttribute('title')) continue;
                            if (span.closest('span[title]')) continue;
                            
                            var text = span.textContent.trim();
                            if (text && text.length > 1 && !text.match(/^\\d+[dwhmsy]$/)) {
                                return text;
                            }
                        }
                    }
                    container = container.parentElement;
                }
                return '';
            """, button_el)
            thread["message_preview"] = str(preview_text or "").strip()
        except Exception:
            pass

        preview_count = _parse_new_message_count(thread["message_preview"])
        if preview_count > thread["new_message_count"]:
            thread["new_message_count"] = preview_count
            thread["is_unread"] = True

        if thread["new_message_count"] > 0 and not thread["message_preview"]:
            count = int(thread["new_message_count"])
            noun = "message" if count == 1 else "messages"
            thread["message_preview"] = f"{count} new {noun}"

        # 6. Try to extract username from the thread link or profile info
        #    Instagram doesn't always show usernames in the inbox list,
        #    but we can try to extract from the display name or other attributes
        try:
            # Try to get username from any link inside the thread button
            links = button_el.find_elements(By.CSS_SELECTOR, "a[href*='instagram.com/']")
            for link in links:
                href = link.get_attribute("href") or ""
                if "/direct/" not in href and "/p/" not in href:
                    parts = href.rstrip("/").split("/")
                    if parts:
                        candidate = parts[-1].strip().lower()
                        if candidate and candidate not in ("", "direct", "inbox", "explore"):
                            thread["username"] = candidate
                            break
        except Exception:
            pass

        # If no username found, use display name as fallback identifier
        if not thread["username"]:
            thread["username"] = thread["display_name"]

    except Exception as e:
        logger.debug(f"[InboxReader] Error extracting thread data: {e}")

    return thread


def scrape_inbox_threads(driver, max_threads: int = 30, unread_only: bool = True, recent_hours: float = 0) -> list:
    """
    Navigate to Instagram DM inbox and scrape thread list data.
    
    Args:
        driver: WebDriver instance (must be logged in)
        max_threads: Maximum number of threads to scrape
        unread_only: If True, only return unread threads
    
    Returns:
        List of thread dicts with keys:
        - display_name: str
        - username: str  
        - message_preview: str
        - timestamp: str (short form like "1d", "5d", "1w")
        - timestamp_label: str (long form like "a day ago")
        - profile_pic_url: str
        - is_unread: bool
    """
    logger.info(f"[InboxReader] Navigating to DM inbox: {INBOX_URL}")

    try:
        driver.get(INBOX_URL)
        human_delay(3, 5)
        _dismiss_inbox_popups(driver)
    except Exception as e:
        logger.error(f"[InboxReader] Failed to navigate to inbox: {e}")
        return []

    # Wait for thread list to load
    # Thread items are div[role='button'] elements inside the inbox container
    try:
        WebDriverWait(driver, THREAD_LOAD_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='button'] img[alt='user-profile-picture']"))
        )
        human_delay(1, 2)
    except TimeoutException:
        logger.warning("[InboxReader] Inbox thread list did not load in time")
        return []

    thread_limit = None
    try:
        parsed_limit = int(max_threads)
        if parsed_limit > 0:
            thread_limit = parsed_limit
    except Exception:
        thread_limit = None

    # Scroll through inbox and collect thread rows until we reach the end.
    threads_data = []
    seen_names = set()
    scroll_passes = 0
    end_hits = 0
    recent_cutoff = None
    reference_now = datetime.now()

    try:
        parsed_recent_hours = float(recent_hours)
    except Exception:
        parsed_recent_hours = 0.0

    if parsed_recent_hours > 0:
        recent_cutoff = reference_now - timedelta(hours=parsed_recent_hours)

    while scroll_passes < MAX_SCROLL_ATTEMPTS:
        # Find all thread button elements
        try:
            # Each thread in the inbox is a div[role='button'] that contains
            # an img[alt='user-profile-picture'] and thread info
            thread_buttons = driver.find_elements(
                By.XPATH,
                "//div[@role='button'][.//img[@alt='user-profile-picture']]"
            )
        except Exception as e:
            logger.debug(f"[InboxReader] Error finding thread buttons: {e}")
            break

        new_found = 0
        reached_recent_cutoff = False
        for button in thread_buttons:
            if thread_limit is not None and len(threads_data) >= thread_limit:
                break

            try:
                thread = _extract_thread_data_from_button(button, driver)

                # Skip if no display name found
                if not thread["display_name"]:
                    continue

                # Dedup by display name
                dedup_key = thread["display_name"].lower().strip()
                if dedup_key in seen_names:
                    continue

                if recent_cutoff is not None:
                    parsed_thread_dt = _parse_instagram_timestamp(thread.get("timestamp", ""), reference_now)
                    if parsed_thread_dt is None:
                        parsed_thread_dt = _parse_instagram_timestamp(thread.get("timestamp_label", ""), reference_now)
                    if parsed_thread_dt is not None and parsed_thread_dt < recent_cutoff:
                        reached_recent_cutoff = True
                        break

                seen_names.add(dedup_key)

                # Filter for unread only if requested
                if unread_only and not thread["is_unread"]:
                    continue

                threads_data.append(thread)
                new_found += 1
            except Exception as e:
                logger.debug(f"[InboxReader] Error processing thread button: {e}")
                continue

        if reached_recent_cutoff:
            break

        if thread_limit is not None and len(threads_data) >= thread_limit:
            break

        moved, at_end = _scroll_inbox_threads_container(driver)
        if at_end and not moved:
            end_hits += 1
        else:
            end_hits = 0

        if end_hits >= SCROLL_END_STABLE_HITS:
            break

        if recent_cutoff is not None:
            oldest_short, oldest_label = _get_oldest_visible_thread_timestamp(driver)
            oldest_dt = _parse_instagram_timestamp(oldest_short, reference_now)
            if oldest_dt is None:
                oldest_dt = _parse_instagram_timestamp(oldest_label, reference_now)
            if oldest_dt is not None and oldest_dt < recent_cutoff:
                break

        scroll_passes += 1
        human_delay(0.8, 1.4)

    status = "unread" if unread_only else "all"
    logger.info(f"[InboxReader] Scraped {len(threads_data)} {status} threads from inbox")
    return threads_data


def _xpath_literal(raw_text: str) -> str:
    """Build a safe XPath string literal for arbitrary text."""
    value = str(raw_text or "")
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'

    parts = value.split("'")
    joined = ", \"'\", ".join(f"'{part}'" for part in parts)
    return f"concat({joined})"


def _find_thread_message_input(driver):
    """Find DM message textbox/textarea in an open thread."""
    selectors = [
        (By.XPATH, "//div[@aria-label='Message' and @role='textbox']"),
        (By.XPATH, "//div[@role='textbox' and @contenteditable='true']"),
        (By.XPATH, "//textarea[contains(@placeholder, 'Message') or contains(@placeholder, 'message')]"),
        (By.XPATH, "//div[@role='dialog']//textarea"),
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        (By.CSS_SELECTOR, "textarea"),
    ]

    for by, selector in selectors:
        try:
            return WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((by, selector))
            )
        except Exception:
            continue
    return None


def _find_thread_attachment_input(driver):
    """Find file input for image/video attachments in an open thread."""
    selectors = [
        "input[type='file'][accept*='image']",
        "input[type='file'][accept*='video']",
        "input[type='file'][accept*='image/*']",
        "input[type='file']",
    ]

    for selector in selectors:
        try:
            return WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
        except Exception:
            continue
    return None


def _click_send_button(driver) -> bool:
    """Click the visible Send button when Enter-key submit is unreliable."""
    send_xpaths = [
        "//div[@role='button'][.//span[normalize-space()='Send']]",
        "//button[normalize-space()='Send']",
        "//*[@aria-label='Send' and (@role='button' or self::button)]",
    ]

    for xpath in send_xpaths:
        try:
            button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", button)
            human_delay(1.5, 2.5)
            return True
        except Exception:
            continue

    return False


def _submit_thread_message(driver, text_input=None) -> bool:
    """Submit current thread message draft using Enter then Send-button fallback."""
    if text_input is not None:
        for key in (Keys.ENTER, Keys.RETURN):
            try:
                text_input.send_keys(key)
                human_delay(1.5, 2.5)
                return True
            except Exception:
                continue

        try:
            active_el = driver.switch_to.active_element
            active_el.send_keys(Keys.ENTER)
            human_delay(1.5, 2.5)
            return True
        except Exception:
            pass

    return _click_send_button(driver)


def _open_thread_by_display_name(driver, thread_display_name: str) -> bool:
    """Open one inbox thread by display name."""
    target_name = str(thread_display_name or "").strip()
    if not target_name:
        return False

    quoted = _xpath_literal(target_name)
    xpaths = [
        f"//div[@role='button'][.//span[@title={quoted}]]",
        f"//div[@role='button'][.//*[contains(normalize-space(), {quoted})]]",
    ]

    for xpath in xpaths:
        try:
            thread_button = WebDriverWait(driver, 6).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].click();", thread_button)
            human_delay(2, 3)
            return True
        except Exception:
            continue

    return False


def scrape_thread_messages(
    driver,
    thread_display_name: str,
    max_messages: int = 20,
    thread_id: int = None,
    recent_hours: float = 0,
) -> list:
    """
    Click into a specific DM thread and scrape recent messages.
    
    Args:
        driver: WebDriver instance (must be on inbox page)
        thread_display_name: The display name of the thread to open
        max_messages: Maximum number of messages to scrape
    
    Returns:
        List of message dicts with keys:
        - sender: str (display name or 'You')
        - text: str
        - timestamp: str
        - is_self: bool  
    """
    logger.info(f"[InboxReader] Opening thread: {thread_display_name}")

    if not _open_thread_by_display_name(driver, thread_display_name):
        logger.warning(f"[InboxReader] Could not find thread: {thread_display_name}")
        return []

    # Wait for message area to load
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='row'], div[role='listbox']"))
        )
        human_delay(1, 2)
    except TimeoutException:
        logger.warning("[InboxReader] Message area did not load")
        return []

    # max_messages <= 0 means "unlimited" for full-history scraping.
    try:
        parsed_limit = int(max_messages)
    except Exception:
        parsed_limit = 0

    limit = parsed_limit if parsed_limit > 0 else None

    recent_cutoff = None
    reference_now = datetime.now()
    try:
        parsed_recent_hours = float(recent_hours)
    except Exception:
        parsed_recent_hours = 0.0
    if parsed_recent_hours > 0:
        recent_cutoff = reference_now - timedelta(hours=parsed_recent_hours)

    # Load older lines by scrolling up before extracting visible rows.
    last_row_count = -1
    stable_hits = 0
    for _ in range(MESSAGE_SCROLL_ATTEMPTS):
        moved, at_top, row_count = _scroll_thread_messages_container_up(driver)

        if row_count > 0 and row_count == last_row_count:
            stable_hits += 1
        else:
            stable_hits = 0
            last_row_count = row_count

        if limit is not None and row_count >= limit and stable_hits >= 1:
            break

        if at_top and stable_hits >= MESSAGE_SCROLL_STABLE_HITS:
            break

        if not moved and at_top:
            stable_hits += 1
            if stable_hits >= MESSAGE_SCROLL_STABLE_HITS:
                break

        if recent_cutoff is not None:
            oldest_visible_ts = _get_oldest_visible_message_timestamp(driver)
            oldest_visible_dt = _parse_instagram_timestamp(oldest_visible_ts, reference_now)
            if oldest_visible_dt is not None and oldest_visible_dt < recent_cutoff:
                break

        human_delay(0.28, 0.55)

    # Extract messages from the conversation view using JS.
    messages = []
    try:
        raw_messages = driver.execute_script("""
            var parsedLimit = Number(arguments[0]);
            var limit = Number.isFinite(parsedLimit) ? parsedLimit : 0;
            var rows = Array.from(document.querySelectorAll('div[role="row"]'));
            if (!rows.length) {
                rows = Array.from(document.querySelectorAll('div[role="listbox"] > div'));
            }

            var messages = [];
            for (var i = 0; i < rows.length; i++) {
                var row = rows[i];
                if (!row) continue;

                var textNodes = row.querySelectorAll('div[dir="auto"], span[dir="auto"]');
                var msgText = '';
                for (var j = 0; j < textNodes.length; j++) {
                    var candidate = (textNodes[j].textContent || '').trim();
                    if (!candidate) continue;
                    if (/^\\d{1,2}:\\d{2}(\\s?(AM|PM))?$/i.test(candidate)) continue;
                    if (/^seen$/i.test(candidate)) continue;
                    msgText = candidate;
                    break;
                }

                var hasAttachment = !!row.querySelector('img[src], video[src], a[href*="cdninstagram"], a[href*="fbcdn"], a[href*="instagram"]');
                if (!msgText && !hasAttachment) {
                    continue;
                }

                var timeText = '';
                var timeEl = row.querySelector('time[datetime], time, abbr[aria-label]');
                if (timeEl) {
                    timeText = timeEl.getAttribute('datetime') || timeEl.getAttribute('aria-label') || (timeEl.textContent || '').trim() || '';
                }

                var bubble = row.querySelector('div[dir="auto"], span[dir="auto"], img[src], video[src]');
                var rect = bubble ? bubble.getBoundingClientRect() : row.getBoundingClientRect();
                var bubbleCenterX = rect.left + (rect.width / 2);
                var rightAligned = bubbleCenterX > (window.innerWidth * 0.58);
                var selfHint = /\\b(You sent|You replied)\\b/i.test((row.textContent || ''));

                messages.push({
                    text: msgText,
                    timestamp: timeText,
                    has_attachment: !!hasAttachment,
                    is_self: !!(selfHint || rightAligned)
                });
            }

            if (limit > 0 && messages.length > limit) {
                messages = messages.slice(messages.length - limit);
            }
            return messages;
        """, (limit if limit is not None else 0))

        if raw_messages:
            for msg in raw_messages:
                is_self = bool(msg.get("is_self", False))
                has_attachment = bool(msg.get("has_attachment", False))
                message_text = str(msg.get("text", "")).strip()
                if not message_text and has_attachment:
                    message_text = "[Attachment]"

                timestamp_text = str(msg.get("timestamp", "")).strip()
                if recent_cutoff is not None:
                    parsed_message_dt = _parse_instagram_timestamp(timestamp_text, reference_now)
                    if parsed_message_dt is not None and parsed_message_dt < recent_cutoff:
                        continue

                messages.append({
                    "sender": "You" if is_self else str(thread_display_name or "").strip(),
                    "text": message_text,
                    "timestamp": timestamp_text,
                    "has_attachment": has_attachment,
                    "is_self": is_self,
                })

        if thread_id is not None:
            try:
                from config import database

                database.save_thread_messages(thread_id, messages)
            except Exception as db_error:
                logger.debug(f"[InboxReader] Could not persist thread lines for thread_id={thread_id}: {db_error}")

    except Exception as e:
        logger.debug(f"[InboxReader] Error extracting messages: {e}")

    logger.info(f"[InboxReader] Extracted {len(messages)} messages from thread: {thread_display_name}")
    return messages


def reply_to_thread(driver, thread_display_name: str, text: str = "", image_path: str = "") -> dict:
    """Open a DM thread and send a text and/or image reply."""
    clean_name = str(thread_display_name or "").strip()
    clean_text = str(text or "").strip()
    clean_image_path = str(image_path or "").strip()

    if not clean_name:
        return {"success": False, "error": "Missing thread display name"}

    if not clean_text and not clean_image_path:
        return {"success": False, "error": "Reply must include text or an image"}

    if clean_image_path:
        clean_image_path = os.path.abspath(clean_image_path)
        if not os.path.isfile(clean_image_path):
            return {"success": False, "error": "Attachment file does not exist"}

    logger.info(f"[InboxReader] Sending reply to thread: {clean_name}")

    try:
        driver.get(INBOX_URL)
        human_delay(2, 4)
        _dismiss_inbox_popups(driver)
    except Exception as nav_error:
        return {"success": False, "error": f"Failed to open inbox: {nav_error}"}

    if not _open_thread_by_display_name(driver, clean_name):
        return {"success": False, "error": f"Could not find thread '{clean_name}'"}

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='row'], div[role='listbox'], input[type='file'], div[role='textbox'][contenteditable='true']"))
        )
        human_delay(1, 2)
    except TimeoutException:
        return {"success": False, "error": "Thread conversation view did not load"}

    if clean_image_path:
        file_input = _find_thread_attachment_input(driver)
        if not file_input:
            return {"success": False, "error": "Could not find attachment input in this thread"}

        try:
            driver.execute_script(
                "arguments[0].style.display='block'; arguments[0].style.visibility='visible';",
                file_input,
            )
        except Exception:
            pass

        try:
            file_input.send_keys(clean_image_path)
            human_delay(2.5, 4.5)
        except Exception as upload_error:
            return {"success": False, "error": f"Failed to upload attachment: {upload_error}"}

    message_input = None
    if clean_text:
        message_input = _find_thread_message_input(driver)
        if not message_input:
            return {"success": False, "error": "Could not find message input"}

        try:
            message_input.click()
            human_delay(0.3, 0.6)
        except Exception:
            try:
                driver.execute_script("arguments[0].focus();", message_input)
            except Exception:
                pass

        try:
            driver.execute_script(
                "arguments[0].focus(); document.execCommand('insertText', false, arguments[1]);",
                message_input,
                clean_text,
            )
            human_delay(0.5, 1.1)
        except Exception:
            try:
                message_input.send_keys(clean_text)
                human_delay(0.5, 1.1)
            except Exception as type_error:
                return {"success": False, "error": f"Failed to type message: {type_error}"}

    if not _submit_thread_message(driver, text_input=message_input):
        return {"success": False, "error": "Could not submit message in thread"}

    logger.info(f"[InboxReader] Reply submitted in thread: {clean_name}")
    return {"success": True}

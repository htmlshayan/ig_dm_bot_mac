"""
Scraper module — navigates to model profiles, extracts recent posts,
determines post age, and scrapes likers/commenters from posts.
"""
import time
import random
import re
import logging
from datetime import datetime, timezone

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config.settings import (
    INSTAGRAM_BASE_URL,
)
from config.database import get_required_setting
from core.auth import human_delay

logger = logging.getLogger("model_dm_bot")


def _setting_int(key: str) -> int:
    value = get_required_setting(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer setting '{key}': {value}")


def get_recent_posts(driver, model_username: str) -> list:
    """
    Navigate to a model's profile and extract recent post URLs with metadata.
    
    Args:
        driver: WebDriver instance (must be logged in)
        model_username: Instagram username of the model
    
    Returns:
        List of dicts: [{"url": str, "age_hours": float, "element": WebElement}, ...]
        Sorted by age (newest first), posts < POST_AGE_PRIORITY_HOURS first.
    """
    profile_url = f"{INSTAGRAM_BASE_URL}/{model_username}/"
    logger.info(f"[Scraper] Navigating to model profile: {profile_url}")

    driver.get(profile_url)
    human_delay(3, 5)

    # Check if profile exists
    try:
        page_source = driver.page_source.lower()
        if "sorry, this page isn't available" in page_source:
            logger.warning(f"[Scraper] Profile @{model_username} not found")
            return []
    except Exception:
        pass

    # Wait for posts grid to load — match both /p/ and /reel/ links
    max_posts_to_check = _setting_int("MAX_POSTS_TO_CHECK")
    posts = []
    post_xpath = "//a[contains(@href, '/p/') or contains(@href, '/reel/')]"
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, post_xpath))
        )
    except TimeoutException:
        logger.warning(f"[Scraper] No posts found for @{model_username}")
        return []

    # Collect post links (both regular posts and reels)
    try:
        post_links = driver.find_elements(By.XPATH, post_xpath)
        seen_urls = set()
        for link in post_links:
            href = link.get_attribute("href")
            if href and ("/p/" in href or "/reel/" in href) and href not in seen_urls:
                seen_urls.add(href)
                posts.append({
                    "url": href,
                    "age_hours": None,  # Will be determined when visiting the post
                })
            if len(posts) >= max_posts_to_check:
                break
    except Exception as e:
        logger.error(f"[Scraper] Error collecting post links: {e}")

    logger.info(f"[Scraper] Found {len(posts)} posts/reels for @{model_username}")
    return posts


def get_post_age_hours(driver) -> float:
    """
    Get the age of the currently loaded post in hours.
    Parses the post's timestamp element.
    
    Returns:
        Age in hours, or 999.0 if unable to determine
    """
    try:
        # Look for the time element on the post page
        time_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//time[@datetime]"))
        )
        datetime_str = time_element.get_attribute("datetime")
        if datetime_str:
            post_time = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            age_hours = (now - post_time).total_seconds() / 3600
            return round(age_hours, 1)
    except Exception as e:
        logger.debug(f"[Scraper] Could not determine post age: {e}")

    return 999.0  # Unknown age — treat as old


def get_post_interactors(driver, post_url: str, already_dmd: set, model_username: str) -> list:
    """
    Open a post and scrape usernames of commenters and likers.
    
    Args:
        driver: WebDriver instance
        post_url: URL of the Instagram post
        already_dmd: Set of usernames already DM'd (to skip)
        model_username: The username of the model whose post we are scraping (to skip)
    
    Returns:
        List of unique usernames who interacted with the post
    """
    logger.info(f"[Scraper] Scraping interactors from: {post_url}")
    driver.get(post_url)
    human_delay(3, 5)

    usernames = set()

    # 1. Load more comments by clicking the "+" button repeatedly
    load_more_clicks = 0
    for _ in range(5):  # Up to 5 clicks to load more comment batches
        try:
            load_more_btn = driver.find_element(
                By.XPATH,
                "//button[contains(@class, '_abl-')] | "
                "//button[.//*[contains(@aria-label, 'Load more')]] | "
                "//svg[@aria-label='Load more comments']/ancestor::button"
            )
            driver.execute_script("arguments[0].click();", load_more_btn)
            load_more_clicks += 1
            human_delay(1.5, 2.5)
        except Exception:
            break

    if load_more_clicks:
        logger.info(f"[Scraper] Loaded {load_more_clicks} more comment batches")

    # Also click "View replies" buttons to expand reply threads
    try:
        reply_buttons = driver.find_elements(
            By.XPATH, "//button[contains(., 'View replies')]"
        )
        for btn in reply_buttons[:5]:  # Expand up to 5 reply threads
            try:
                driver.execute_script("arguments[0].click();", btn)
                human_delay(1, 1.5)
            except Exception:
                continue
    except Exception:
        pass

    # 2. Extract commenter usernames using JavaScript (most reliable)
    #    This scans all <a> tags on the page and filters to profile links
    #    that appear inside comment list items
    try:
        # Wait a little longer for comments to fully render
        human_delay(4, 6)
        
        js_usernames = driver.execute_script("""
            var usernames = [];
            // Get all <a> tags with an href attribute on the entire page
            var links = document.querySelectorAll('a[href]');
            for (var i = 0; i < links.length; i++) {
                try {
                    // Use pathname to cleanly get the path without query params or domain
                    var path = links[i].pathname;
                    if (!path) continue;
                    
                    // Split and remove empty segments
                    var parts = path.split('/').filter(function(p) { return p.trim().length > 0; });
                    if (parts.length !== 1) continue;
                    
                    var username = parts[0].toLowerCase();
                    // Skip non-username paths
                    if (['explore', 'p', 'reel', 'stories', 'accounts', 'direct', 'c', 'reels'].indexOf(username) >= 0) continue;
                    // Skip if it's just a number (comment ID)
                    if (/^\\d+$/.test(username)) continue;
                    if (!username) continue;
                    
                    // Only add if the link text essentially matches the username
                    var text = links[i].textContent.trim().toLowerCase();
                    if (text === username) {
                        usernames.push(parts[0]);
                    }
                } catch(e) {}
            }
            // Deduplicate
            return [...new Set(usernames)];
        """)
        if js_usernames:
            logger.info(f"[Scraper] JS extracted {len(js_usernames)} usernames")
            for uname in js_usernames:
                if uname and uname != model_username and uname not in already_dmd:
                    usernames.add(uname)
    except Exception as e:
        logger.debug(f"[Scraper] JS extraction error: {e}")

    # 3. Fallback: XPath-based extraction from h3 tags in comment containers
    if not usernames:
        logger.info("[Scraper] JS found nothing, trying XPath fallback...")
        try:
            # Try multiple selectors
            xpath_selectors = [
                "//h3//a[@role='link' and @href]",
                "//ul//li//a[@role='link' and @href]",
            ]
            for xpath in xpath_selectors:
                elements = driver.find_elements(By.XPATH, xpath)
                for elem in elements:
                    href = elem.get_attribute("href")
                    text = elem.text.strip()
                    if href and text:
                        username = _extract_username_from_href(href)
                        # Only count it if the link text matches the username
                        if username and username == text and username not in already_dmd:
                            usernames.add(username)
                if usernames:
                    logger.info(f"[Scraper] XPath fallback found {len(usernames)} usernames")
                    break
        except Exception as e:
            logger.debug(f"[Scraper] XPath fallback error: {e}")

    commenter_count = len(usernames)

    # 4. Scrape likers by clicking "likes" count
    try:
        max_likers = _setting_int("MAX_LIKERS_PER_POST")
        likers = _scrape_likers(driver, already_dmd, max_likers)
        usernames.update(likers)
    except Exception as e:
        logger.debug(f"[Scraper] Error scraping likers: {e}")

    # Remove the model's own username if present
    usernames.discard(model_username)
    final_list = list(usernames)
    logger.info(f"[Scraper] Found {len(final_list)} unique interactors ({commenter_count} commenters, {len(final_list) - commenter_count} likers)")
    return final_list


def _scrape_likers(driver, already_dmd: set, max_count: int) -> set:
    """
    Click on the likes count to open the likers dialog and scrape usernames.
    """
    likers = set()

    # Try to find and click the likes button
    likes_selectors = [
        "//a[contains(@href, '/liked_by/')]",
        "//button[contains(., 'like')]",
        "//span[contains(., 'like')]/ancestor::a",
        "//section//a[contains(@href, 'liked_by')]",
    ]

    likes_button = None
    for xpath in likes_selectors:
        try:
            likes_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            if likes_button:
                break
        except Exception:
            continue

    if not likes_button:
        logger.debug("[Scraper] Could not find likes button")
        return likers

    try:
        driver.execute_script("arguments[0].click();", likes_button)
        human_delay(2, 3)

        # Wait for the likers dialog to appear
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@role='dialog']//a[contains(@href, '/')]")
            )
        )

        # Scroll the dialog and collect usernames
        scroll_attempts = 0
        while len(likers) < max_count and scroll_attempts < 5:
            username_elements = driver.find_elements(
                By.XPATH, "//div[@role='dialog']//a[contains(@href, '/') and @role='link']"
            )

            for elem in username_elements:
                href = elem.get_attribute("href")
                username = _extract_username_from_href(href)
                if username and username not in already_dmd:
                    likers.add(username)
                if len(likers) >= max_count:
                    break

            # Scroll the dialog
            try:
                dialog = driver.find_element(By.XPATH, "//div[@role='dialog']//div[contains(@style, 'overflow')]")
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", dialog)
            except Exception:
                # Try scrolling the last element into view
                if username_elements:
                    driver.execute_script("arguments[0].scrollIntoView(true);", username_elements[-1])

            human_delay(1, 2)
            scroll_attempts += 1

        # Close the dialog
        try:
            close_btn = driver.find_element(By.XPATH, "//div[@role='dialog']//button[contains(@class, 'close') or @aria-label='Close']")
            close_btn.click()
        except Exception:
            driver.execute_script("document.querySelector('[role=\"dialog\"]')?.closest('[role=\"presentation\"]')?.querySelector('button')?.click()")

        human_delay(1, 2)

    except Exception as e:
        logger.debug(f"[Scraper] Error in likers dialog: {e}")

    return likers


def sort_posts_by_priority(posts: list, driver) -> list:
    """
    Visit each post to determine age, then sort:
    1. Posts < POST_AGE_PRIORITY_HOURS first (newest first)
    2. Then older posts (newest first)
    
    Returns sorted list with age_hours populated.
    """
    for post in posts:
        driver.get(post["url"])
        human_delay(2, 3)
        post["age_hours"] = get_post_age_hours(driver)
        logger.info(f"[Scraper] Post {post['url'][-15:]} age: {post['age_hours']}h")

    priority_hours = _setting_int("POST_AGE_PRIORITY_HOURS")

    # Sort: priority posts first, then by age ascending
    priority = [p for p in posts if p["age_hours"] < priority_hours]
    non_priority = [p for p in posts if p["age_hours"] >= priority_hours]

    priority.sort(key=lambda x: x["age_hours"])
    non_priority.sort(key=lambda x: x["age_hours"])

    result = priority + non_priority
    if priority:
        logger.info(f"[Scraper] 🔥 {len(priority)} priority posts (< {priority_hours}h old)")

    return result


def _extract_username_from_href(href: str) -> str:
    """Extract username from an Instagram profile URL."""
    if not href:
        return ""
    # Remove trailing slash and get last path segment
    href = href.rstrip("/")
    parts = href.split("/")
    username = parts[-1] if parts else ""
    # Filter out non-username paths
    if username in ["", "explore", "p", "reel", "stories", "accounts", "direct", "c", "liked_by", "comments"]:
        return ""
    # Filter out numeric-only strings (comment IDs)
    if username.isdigit():
        return ""
    # Filter out usernames with query params
    if "?" in username:
        username = username.split("?")[0]
    return username

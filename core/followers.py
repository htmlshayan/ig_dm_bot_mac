"""
Followers module — scrapes a model's followers list.
Used as secondary DM targets after post interactors.
"""
import time
import random
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config.settings import (
    INSTAGRAM_BASE_URL,
)
from config.database import get_required_setting
from core.auth import human_delay

logger = logging.getLogger("model_dm_bot")


def get_followers(driver, model_username: str, already_dmd: set, max_count: int = None) -> list:
    """
    Open a model's followers dialog and scrape usernames.
    
    Args:
        driver: WebDriver instance (must be logged in)
        model_username: Instagram username of the model
        already_dmd: Set of usernames already DM'd
        max_count: Maximum followers to scrape (default: MAX_FOLLOWERS_TO_SCRAPE)
    
    Returns:
        List of follower usernames (deduplicated against already_dmd)
    """
    max_count = max_count or int(get_required_setting("MAX_FOLLOWERS_TO_SCRAPE"))
    profile_url = f"{INSTAGRAM_BASE_URL}/{model_username}/"

    logger.info(f"[Followers] Opening @{model_username}'s followers list")

    # Always navigate to profile page to ensure clean state
    driver.get(profile_url)
    human_delay(3, 5)

    followers = []

    # Click the followers count link
    followers_link = None
    selectors = [
        f"//a[contains(@href, '/{model_username}/followers')]",
        "//a[contains(@href, '/followers')]",
        "//header//ul//li[2]//a",
        "//a[contains(., 'follower')]",
        "//span[contains(., 'follower')]/ancestor::a",
    ]

    for xpath in selectors:
        try:
            followers_link = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            if followers_link:
                break
        except Exception:
            continue

    if not followers_link:
        logger.warning(f"[Followers] Could not find followers link for @{model_username}")
        return followers

    try:
        driver.execute_script("arguments[0].click();", followers_link)
        human_delay(2, 4)

        # Wait for the followers dialog
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@role='dialog']//a[contains(@href, '/')]")
            )
        )

        # Scroll and collect followers
        scroll_attempts = 0
        max_scroll_attempts = 10
        seen = set()

        while len(followers) < max_count and scroll_attempts < max_scroll_attempts:
            # Collect visible follower links
            follower_elements = driver.find_elements(
                By.XPATH, "//div[@role='dialog']//a[contains(@href, '/') and @role='link']"
            )

            new_found = 0
            for elem in follower_elements:
                try:
                    href = elem.get_attribute("href")
                    if not href:
                        continue
                    username = href.rstrip("/").split("/")[-1]
                    if not username or username in ["explore", "p", "reel", "accounts"]:
                        continue
                    if "?" in username:
                        username = username.split("?")[0]

                    if username not in seen:
                        seen.add(username)
                        if username not in already_dmd and username != model_username:
                            followers.append(username)
                            new_found += 1

                    if len(followers) >= max_count:
                        break
                except Exception:
                    continue

            if new_found == 0:
                scroll_attempts += 1
            else:
                scroll_attempts = 0  # Reset on successful new finds

            # Scroll the dialog down
            try:
                scrollable = driver.find_element(
                    By.XPATH, "//div[@role='dialog']//div[contains(@class, '_aano')]"
                )
                driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", scrollable
                )
            except Exception:
                # Fallback: scroll last visible element into view
                if follower_elements:
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", follower_elements[-1]
                    )

            human_delay(1.5, 3)

        # Close the dialog
        try:
            close_btn = driver.find_element(
                By.XPATH, "//div[@role='dialog']//button[@aria-label='Close' or contains(@class, 'close')]"
            )
            close_btn.click()
        except Exception:
            try:
                # Press Escape to close
                from selenium.webdriver.common.keys import Keys
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            except Exception:
                pass

        human_delay(1, 2)

    except TimeoutException:
        logger.warning(f"[Followers] Followers dialog did not load for @{model_username}")
    except Exception as e:
        logger.error(f"[Followers] Error scraping followers: {e}")

    logger.info(f"[Followers] Scraped {len(followers)} followers for @{model_username}")
    return followers

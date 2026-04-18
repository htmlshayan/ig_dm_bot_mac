"""
Global settings for the Instagram Model DM Bot.
Static paths and URLs only.
Runtime behavior settings are stored in the database.
"""
import os

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
COOKIES_DIR = os.path.join(BASE_DIR, "cookies")
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
for d in [COOKIES_DIR, DATA_DIR, LOGS_DIR]:
    os.makedirs(d, exist_ok=True)

# Config file paths
ACCOUNTS_FILE = os.path.join(CONFIG_DIR, "accounts.json")
MODELS_FILE = os.path.join(CONFIG_DIR, "models.json")
MESSAGES_FILE = os.path.join(CONFIG_DIR, "messages.json")
DM_LOG_FILE = os.path.join(DATA_DIR, "dm_log.json")
SETTINGS_FILE = os.path.join(CONFIG_DIR, "settings.json")

# ──────────────────────────────────────────────
# Static URLs
# ──────────────────────────────────────────────
INSTAGRAM_BASE_URL = "https://www.instagram.com"
INSTAGRAM_LOGIN_URL = "https://www.instagram.com/accounts/login/"

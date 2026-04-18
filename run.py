"""
CLI entry point for the Instagram Model DM Bot.
"""
import sys
import os
import json
import getpass

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--setup":
        run_setup()
    elif len(sys.argv) > 1 and sys.argv[1] == "--status":
        show_status()
    else:
        run_main()


def run_main():
    """Run the main DM bot."""
    print("=" * 60)
    print("  INSTAGRAM MODEL DM BOT")
    print("=" * 60)
    print()

    from config.settings import ACCOUNTS_FILE, MODELS_FILE, MESSAGES_FILE

    # Validate config files
    for f, name in [(ACCOUNTS_FILE, "accounts.json"), (MODELS_FILE, "models.json"), (MESSAGES_FILE, "messages.json")]:
        if not os.path.exists(f):
            print(f"❌ Missing config file: {name}")
            print(f"   Expected at: {f}")
            return

    # Check for placeholder values
    with open(ACCOUNTS_FILE, "r") as f:
        accounts = json.load(f)
    if accounts and accounts[0].get("username") == "your_ig_username_1":
        print("❌ Please configure your accounts in config/accounts.json first!")
        print("   Replace the placeholder usernames and passwords.")
        return

    with open(MODELS_FILE, "r") as f:
        models = json.load(f)
    if models and models[0] == "model_username_1":
        print("❌ Please configure target models in config/models.json first!")
        return

    print(f"📋 Accounts: {len(accounts)}")
    print(f"🎯 Models: {len(models)}")
    print()

    confirm = input("Start the bot? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    from bot import run_bot
    run_bot()


def run_setup():
    """Setup mode — login each account and save cookies."""
    print("=" * 60)
    print("  ACCOUNT SETUP — Save Login Cookies")
    print("=" * 60)
    print()

    from config.settings import ACCOUNTS_FILE
    from core.browser import create_driver, close_driver
    from core.auth import login_with_credentials, is_logged_in
    from core.cookie_manager import save_cookies

    if not os.path.exists(ACCOUNTS_FILE):
        print(f"❌ Missing {ACCOUNTS_FILE}")
        return

    with open(ACCOUNTS_FILE, "r") as f:
        accounts = json.load(f)

    if not accounts:
        print("❌ No accounts in accounts.json")
        return

    for i, account in enumerate(accounts):
        username = account["username"]
        if username.startswith("your_ig_"):
            print(f"⚠️ Skipping placeholder account: {username}")
            continue

        print(f"\n--- Account {i+1}/{len(accounts)}: @{username} ---")
        print("A browser will open. Please complete login (including 2FA if needed).")
        print("After you're logged in, press Enter here to save cookies.\n")

        driver = create_driver()

        try:
            # Try auto-login first
            if login_with_credentials(driver, account):
                print(f"✅ Auto-login successful for @{username}")
            else:
                print(f"⚠️ Auto-login didn't work. Please login manually in the browser.")
                print(f"   Navigate to: https://www.instagram.com/accounts/login/")
                input("Press Enter after you've logged in manually...")

            if is_logged_in(driver):
                save_cookies(driver, username)
                print(f"✅ Cookies saved for @{username}")
            else:
                print(f"❌ Could not verify login for @{username}")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            close_driver(driver)

    print("\n✅ Setup complete! You can now run: python run.py")


def show_status():
    """Show current DM log stats."""
    from config.settings import DM_LOG_FILE

    if not os.path.exists(DM_LOG_FILE):
        print("No DM log found yet.")
        return

    with open(DM_LOG_FILE, "r") as f:
        dm_log = json.load(f)

    total = len(dm_log)
    sent = sum(1 for v in dm_log.values() if isinstance(v, dict) and v.get("timestamp"))
    failed = sum(1 for v in dm_log.values() if isinstance(v, dict) and v.get("status") == "cant_message")

    print(f"📊 DM Log Stats:")
    print(f"   Total entries: {total}")
    print(f"   ✅ Sent: {sent}")
    print(f"   ⚠️ Can't message: {failed}")

    # Show by model
    models = {}
    for user, data in dm_log.items():
        if isinstance(data, dict) and "model" in data:
            m = data["model"]
            models[m] = models.get(m, 0) + 1

    if models:
        print(f"\n   By model:")
        for model, count in sorted(models.items(), key=lambda x: -x[1]):
            print(f"     @{model}: {count} DMs")


if __name__ == "__main__":
    main()

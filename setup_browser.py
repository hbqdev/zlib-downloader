"""
Run this once (or whenever the session expires) to log in and solve
the Cloudflare challenge in a visible Chrome window (displayed via WSLg).

The session is saved to browser_profile_chrome/ and reused headlessly
by zlibdownload.py on subsequent runs.

Usage:
    python setup_browser.py
"""

import asyncio
import os
from playwright.async_api import async_playwright

try:
    from playwright_stealth import Stealth
    _stealth = Stealth()
except ImportError:
    _stealth = None

CHROME_PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_profile_chrome")
CHROME_EXECUTABLE = "/usr/bin/google-chrome"

import json
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def load_config():
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


async def setup():
    config = load_config()
    domain = config.get("domain", "z-library.sk")

    print("=" * 60)
    print("  Z-Library Browser Setup (system Chrome via WSLg)")
    print("=" * 60)
    print(f"  Domain  : {domain}")
    print(f"  Profile : {CHROME_PROFILE_DIR}")
    print()
    print("  A Chrome window will open on your screen.")
    print("  1. Solve any Cloudflare challenge if prompted")
    print("  2. Log in to Z-Library")
    print("  3. Once the homepage loads, CLOSE the Chrome window")
    print("     — the session will be saved automatically.")
    print("=" * 60)
    input("\n  Press Enter to launch Chrome...")

    os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)
    playwright = await async_playwright().start()

    context = await playwright.chromium.launch_persistent_context(
        CHROME_PROFILE_DIR,
        executable_path=CHROME_EXECUTABLE,
        headless=False,
        accept_downloads=True,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
    )

    page = context.pages[0] if context.pages else await context.new_page()
    if _stealth:
        await _stealth.apply_stealth_async(page)

    print(f"\n  🌐 Navigating to https://{domain} ...")
    await page.goto(f"https://{domain}", wait_until="domcontentloaded", timeout=60000)

    print("\n  Chrome is open. Log in and wait for the page to fully load.")
    print("  Then CLOSE the Chrome window to save your session.")

    # Wait until the user closes the browser
    await context.wait_for_event("close", timeout=300000)

    await playwright.stop()
    print("\n  ✅ Session saved to", CHROME_PROFILE_DIR)
    print("  You can now run: python zlibdownload.py")


if __name__ == "__main__":
    asyncio.run(setup())

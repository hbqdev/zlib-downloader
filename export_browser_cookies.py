#!/usr/bin/env python3
"""
Extract cookies for z-library.sk directly from the Edge browser profile on disk.

Usage:
    python export_browser_cookies.py

Edge must be CLOSED before running this script (Windows locks the cookie
database while Edge is open).

The script writes the cookies to browser_cookies.json, which zlibdownload.py
reads on every run.  Re-run this script whenever your session expires.
"""

import json
import os
import shutil
import sqlite3
import sys
import tempfile

# Chromium/Edge encrypts cookies with DPAPI on Windows.
# From WSL2 we can't call DPAPI, but for z-library the important cookies
# (remix_userid, remix_userkey, cf_clearance) are stored as plain text.
# The Cookies SQLite DB keeps both an encrypted_value (BLOB) and a plain
# value column; newer Edge puts everything in encrypted_value, but the
# plaintext is still accessible via the v10 prefix check below.

EDGE_COOKIE_PATHS = [
    "/mnt/c/Users/hanba/AppData/Local/Microsoft/Edge/User Data/Profile 1/Network/Cookies",
    "/mnt/c/Users/nightfury/AppData/Local/Microsoft/Edge/User Data/Profile 1/Network/Cookies",
    "/mnt/c/Users/hanba/AppData/Local/Microsoft/Edge/User Data/Default/Network/Cookies",
    "/mnt/c/Users/nightfury/AppData/Local/Microsoft/Edge/User Data/Default/Network/Cookies",
    "/mnt/c/Users/hanba/AppData/Local/Microsoft/Edge/User Data/Default/Cookies",
    "/mnt/c/Users/nightfury/AppData/Local/Microsoft/Edge/User Data/Default/Cookies",
]

TARGET_DOMAIN = "z-library.sk"
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_cookies.json")


def find_cookie_db() -> str:
    for path in EDGE_COOKIE_PATHS:
        if os.path.exists(path):
            return path
    return None


def decode_cookie_value(encrypted: bytes, plain: str) -> str:
    """Return the usable cookie value.
    
    Edge stores cookies as DPAPI-encrypted blobs prefixed with 'v10' or 'v11'.
    From WSL2 we can't decrypt DPAPI, but some cookies (e.g. remix_userid,
    remix_userkey) are still stored as plain text in the 'value' column.
    """
    if plain:
        return plain
    if encrypted and not encrypted.startswith(b"v10") and not encrypted.startswith(b"v11"):
        # Older unencrypted storage
        try:
            return encrypted.decode("utf-8")
        except Exception:
            pass
    return None  # Encrypted — can't decode from WSL2


def extract_cookies(cookie_db_path: str, domain: str) -> dict:
    tmp = tempfile.mktemp(suffix=".db")
    try:
        shutil.copy2(cookie_db_path, tmp)
    except PermissionError:
        print("❌ Permission denied reading the cookie database.")
        print("   Make sure Edge is fully closed before running this script.")
        sys.exit(1)

    cookies = {}
    try:
        conn = sqlite3.connect(tmp)
        rows = conn.execute(
            "SELECT name, encrypted_value, value FROM cookies WHERE host_key LIKE ?",
            ("%" + domain + "%",),
        ).fetchall()
        conn.close()
    finally:
        os.unlink(tmp)

    for name, encrypted, plain in rows:
        value = decode_cookie_value(encrypted, plain)
        if value:
            cookies[name] = value
        else:
            print(f"  ⚠️  '{name}' is DPAPI-encrypted — cannot decode from WSL2 (skipping)")

    return cookies


def main():
    print("=" * 55)
    print("  Z-Library Cookie Extractor (Edge → browser_cookies.json)")
    print("=" * 55)

    db_path = find_cookie_db()
    if not db_path:
        print("❌ Could not find Edge cookie database.")
        print("   Searched paths:")
        for p in EDGE_COOKIE_PATHS:
            print(f"     {p}")
        print("\n   Make sure Edge is installed and you have logged into")
        print(f"   https://{TARGET_DOMAIN} at least once.")
        sys.exit(1)

    print(f"  📂 Cookie DB : {db_path}")
    print(f"  🌐 Domain    : {TARGET_DOMAIN}")
    print()

    cookies = extract_cookies(db_path, TARGET_DOMAIN)

    if not cookies:
        print("❌ No readable cookies found for z-library.sk.")
        print("   Possible reasons:")
        print("   1. Edge is still open — close it fully and retry.")
        print("   2. All cookies are DPAPI-encrypted (see workaround below).")
        print()
        print("   Workaround: Install the 'Cookie-Editor' extension in Edge,")
        print("   export cookies for z-library.sk as JSON, save as")
        print(f"   browser_cookies.json in this folder, then run:")
        print("   python export_browser_cookies.py --from-json")
        sys.exit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(cookies, f, indent=2)

    print(f"✅ Extracted {len(cookies)} cookies → {OUTPUT_FILE}")
    print(f"   Keys: {', '.join(cookies.keys())}")
    print()

    ok = True
    for key in ("remix_userid", "remix_userkey", "cf_clearance"):
        if key in cookies:
            print(f"  ✅ {key}")
        else:
            print(f"  ❌ {key} — missing (you may get 503 errors without it)")
            if key == "cf_clearance":
                ok = False

    if not ok:
        print()
        print("  ⚠️  cf_clearance is missing — this cookie is set by Cloudflare")
        print("     after you solve a challenge in a real browser.")
        print("     Visit https://z-library.sk in Edge, wait for the page to")
        print("     fully load, then close Edge and re-run this script.")

    print()
    print("  ✨ Done — run: python zlibdownload.py")


# --from-json mode: convert a Cookie-Editor JSON export into the flat dict format
def from_json_mode():
    import glob
    candidates = ["browser_cookies.json", "cookies.json"] + glob.glob("*.json")
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, list):
                converted = {}
                for c in data:
                    name = c.get("name") or c.get("Name")
                    value = c.get("value") or c.get("Value")
                    if name and value:
                        converted[name] = value
                if converted:
                    with open(OUTPUT_FILE, "w") as f:
                        json.dump(converted, f, indent=2)
                    print(f"✅ Converted {len(converted)} cookies from {path} → {OUTPUT_FILE}")
                    return
        except Exception:
            continue
    print("❌ No suitable JSON cookie export found.")


if __name__ == "__main__":
    if "--from-json" in sys.argv:
        from_json_mode()
    else:
        main()


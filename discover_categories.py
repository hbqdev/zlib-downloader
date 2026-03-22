"""
discover_categories.py — Scrape Z-Library's /categories page and merge all
subcategories into categories.json.

Usage:
    python discover_categories.py            # add new categories (enabled by default)
    python discover_categories.py --dry-run  # preview without writing
    python discover_categories.py --disabled # add as disabled (scrape_enabled: false)
"""

import asyncio
import json
import re
import sys
import os

CATEGORIES_FILE = "categories.json"
DOMAIN = "z-library.sk"


def load_json(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def fetch_all_categories(domain: str) -> list[dict]:
    """Navigate to /categories and extract all subcategory entries."""
    from browser_scraper import BrowserScraper

    scraper = BrowserScraper(domain=domain)
    await scraper.start(headless=True)

    print(f"🌐 Navigating to https://{domain}/categories ...")
    await scraper.page.goto(
        f"https://{domain}/categories",
        wait_until="domcontentloaded",
        timeout=90000,
    )

    # Wait for Cloudflare challenge to pass
    for _ in range(30):
        try:
            title = await scraper.page.title()
            if "Checking" not in title:
                break
        except Exception:
            pass
        await asyncio.sleep(1)

    title = await scraper.page.title()
    print(f"📄 Page title: {title}")
    if "Checking" in title:
        print("❌ Cloudflare challenge did not resolve. Run 'python setup_browser.py' to refresh session.")
        await scraper.close()
        return []

    raw = await scraper.page.evaluate("""() => {
        return [...document.querySelectorAll('a[href*="/category/"]')].map(a => ({
            href: a.href,
            text: a.textContent.trim()
        }));
    }""")

    await scraper.close()

    categories = []
    for item in raw:
        href = item["href"]
        text = item["text"]

        # Match subcategories only: /category/{id}/{slug}
        m = re.match(r".*/category/(\d+)/([^/?#]+)", href)
        if not m:
            continue  # skip main category links (no slug)

        cat_id = int(m.group(1))
        slug = m.group(2)

        # Parse name and book count from link text, e.g. "Architecture\xa0(21242)"
        name_match = re.match(r"^(.+?)(?:\s*\([\d,]+\))?$", text.replace("\xa0", " ").strip())
        name = name_match.group(1).strip() if name_match else text.strip()

        categories.append({"id": cat_id, "slug": slug, "name": name})

    return categories


def merge_into_categories_json(discovered: list[dict], enabled: bool, dry_run: bool):
    existing = load_json(CATEGORIES_FILE)

    # Build a set of already-tracked category ids
    existing_ids = {c["id"] for c in existing if "id" in c}

    # Find the current max order_to_download
    existing_orders = [c.get("order_to_download", 0) for c in existing if isinstance(c.get("order_to_download"), int)]
    next_order = max(existing_orders, default=0) + 1

    new_entries = []
    for cat in discovered:
        if cat["id"] in existing_ids:
            continue  # already tracked, preserve existing progress
        entry = {
            "name": cat["name"],
            "id": cat["id"],
            "slug": cat["slug"],
            "scrape_enabled": enabled,
            "max_pages_to_scrape": 9999,
            "next_page_to_scrape": 1,
            "books_processed_on_page": 0,
            "order_to_download": next_order,
        }
        new_entries.append(entry)
        next_order += 1

    print(f"\n📊 Results:")
    print(f"  Discovered subcategories : {len(discovered)}")
    print(f"  Already in categories.json: {len(existing_ids)}")
    print(f"  New to add               : {len(new_entries)}")
    print(f"  scrape_enabled default   : {enabled}")

    if dry_run:
        print("\n🔍 Dry run — nothing written. First 20 new entries:")
        for e in new_entries[:20]:
            print(f"  [{e['id']}] {e['name']} ({e['slug']})")
        if len(new_entries) > 20:
            print(f"  ... and {len(new_entries) - 20} more")
        return

    if not new_entries:
        print("✅ Nothing to add — categories.json is already up to date.")
        return

    merged = existing + new_entries
    save_json(CATEGORIES_FILE, merged)
    print(f"\n✅ Added {len(new_entries)} new categories to {CATEGORIES_FILE}")
    print("   Run 'python zlibdownload.py' to start downloading.")


async def main():
    dry_run = "--dry-run" in sys.argv
    enabled = "--disabled" not in sys.argv

    if dry_run:
        print("🔍 DRY RUN — no changes will be written\n")

    discovered = await fetch_all_categories(DOMAIN)
    if not discovered:
        print("❌ No categories found.")
        return

    merge_into_categories_json(discovered, enabled=enabled, dry_run=dry_run)


if __name__ == "__main__":
    asyncio.run(main())

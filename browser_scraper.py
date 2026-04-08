"""
Browser-based scraper for Z-Library using Playwright + system Chrome (WSLg).

The first time you run it a Chrome window will appear on your screen (via WSLg).
Log in to Z-Library and solve any Cloudflare challenge, then the session is saved
to browser_profile_chrome/ and reused headlessly on every subsequent run.
"""

import asyncio
import json
import re
import os
import subprocess
import time
from playwright.async_api import async_playwright, Browser, Page

# Try to import stealth
try:
    from playwright_stealth import Stealth
    HAS_STEALTH = True
    _stealth = Stealth()
except ImportError:
    HAS_STEALTH = False
    _stealth = None

CHROME_PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_profile_chrome")
CHROME_EXECUTABLE = "/usr/bin/google-chrome"

# Matches trailing "(z-library.sk, 1lib.sk, ...)" domain groups in filenames
_ZLIB_DOMAIN_RE = re.compile(
    r'\s*\([^)]*(?:z-library|z-lib|1lib|singlelogin|zlibrary)[^)]*\)',
    re.IGNORECASE,
)
# Matches the author block: last parenthesised group before extension
_AUTHOR_GROUP_RE = re.compile(r'\(([^()]+)\)\s*$')


def _clean_zlib_filename(filename: str) -> str:
    """Clean a Z-Library suggested filename into 'Authors - Title.ext' format.

    Input:  "Museums, Health and Well-Being (Helen J Chatterjee, Guy Noble) (z-library.sk, 1lib.sk, z-lib.sk).pdf"
    Output: "Helen J Chatterjee, Guy Noble - Museums, Health and Well-Being.pdf"
    """
    stem, ext = os.path.splitext(filename)

    # Strip all domain suffix groups
    cleaned = stem
    while True:
        new = _ZLIB_DOMAIN_RE.sub('', cleaned).rstrip()
        if new == cleaned:
            break
        cleaned = new

    # Extract author group (last parenthesised block)
    m = _AUTHOR_GROUP_RE.search(cleaned)
    if m:
        authors = m.group(1).strip()
        title = cleaned[:m.start()].strip().rstrip(',').strip()
        new_stem = f"{authors} - {title}" if authors else title
    else:
        new_stem = cleaned.strip()

    # Sanitize characters illegal on common filesystems
    new_stem = re.sub(r'[\\/:*?"<>|]', '', new_stem)
    new_stem = re.sub(r'\s+', ' ', new_stem).strip()

    return (new_stem[:180] + ext) if new_stem else filename


class BrowserScraper:
    def __init__(
        self,
        domain: str = "z-library.sk",
        cookies_file: str = "browser_cookies.json",
        **kwargs,  # absorb unused legacy kwargs (mode, windows_host, debug_port)
    ):
        self.domain = domain
        self.cookies_file = cookies_file
        self.context = None
        self.page: Page = None
        self._playwright = None
        self._last_nav_time: float = 0.0  # epoch seconds of last main-page navigation
        self._CF_KEEPALIVE_INTERVAL = 20 * 60  # re-navigate after 20 min of no activity

    async def start(self, headless: bool = True):
        """Launch Chrome with a persistent profile.

        If the profile directory is new (first run), headless is forced False so
        a visible window appears and you can log in / solve Cloudflare challenges.
        The session is saved automatically when close() is called.
        """
        self._playwright = await async_playwright().start()
        os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)

        # Force visible window on first run so the user can log in
        profile_is_new = not os.path.exists(os.path.join(CHROME_PROFILE_DIR, "Default"))
        effective_headless = headless and not profile_is_new
        if profile_is_new:
            print("🆕 First run — opening a visible Chrome window so you can log in.")
            print("   Navigate to https://{} and log in, then close the window.".format(self.domain))

        self.context = await self._playwright.chromium.launch_persistent_context(
            CHROME_PROFILE_DIR,
            executable_path=CHROME_EXECUTABLE,
            headless=effective_headless,
            accept_downloads=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Re-fulfill document responses so Playwright never aborts navigation
        # with "Download is starting" when the server sends unexpected headers.
        await self.context.route("**/*", self._route_refulfill_documents)

        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()

        if HAS_STEALTH and _stealth:
            await _stealth.apply_stealth_async(self.page)
            print("✅ Stealth mode enabled")

        if profile_is_new:
            # Navigate to the site and wait for the user to log in manually
            try:
                await self.page.goto(f"https://{self.domain}/", wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"⚠️  Auto-navigation failed ({e!s:.80}). Navigate manually in the Chrome window.")
            print("⏳ Waiting for you to log in... close the Chrome window when done.")
            await self.context.wait_for_event("close", timeout=300000)
            # Re-launch headlessly now that the session is saved
            self._playwright = await async_playwright().start()
            self.context = await self._playwright.chromium.launch_persistent_context(
                CHROME_PROFILE_DIR,
                executable_path=CHROME_EXECUTABLE,
                headless=True,
                accept_downloads=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
            )
            await self.context.route("**/*", self._route_refulfill_documents)
            self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
            if HAS_STEALTH and _stealth:
                await _stealth.apply_stealth_async(self.page)

        print(f"✅ Chrome started (headless={effective_headless}, profile: {CHROME_PROFILE_DIR})")

        # Warm up the CF session immediately so downloads work right after start/restart
        await self._keepalive_if_needed(force=True)

    async def _keepalive_if_needed(self, force: bool = False):
        """Navigate the main page to Z-Library to refresh Cloudflare cookies.
        Only runs if more than _CF_KEEPALIVE_INTERVAL seconds have passed (or force=True).
        CF clearance cookies expire after ~30-60 min; without this, download events
        stop firing on long-running sessions because /dl/ gets a challenge page instead.
        """
        if not force and (time.time() - self._last_nav_time) < self._CF_KEEPALIVE_INTERVAL:
            return
        print(f"      🔄 Refreshing CF session (keepalive)...")
        try:
            await self.page.goto(
                f"https://{self.domain}/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            # Wait for any Cloudflare JS challenge to fully resolve before returning.
            # Without this loop, domcontentloaded fires on the challenge page HTML
            # *before* the JS runs and redirects — leaving CF cookies un-refreshed.
            for _ in range(15):
                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=5000)
                    title = await self.page.title()
                except Exception:
                    await self.page.wait_for_timeout(2000)
                    continue
                if "checking" in title.lower() or "just a moment" in title.lower():
                    print(f"      ⏳ CF challenge detected during keepalive, waiting...")
                    await self.page.wait_for_timeout(2000)
                else:
                    break
            self._last_nav_time = time.time()
        except Exception as e:
            # Non-fatal — best effort keepalive
            print(f"      ⚠️  CF keepalive navigation failed ({e!s:.60}), continuing anyway.")


    async def close(self):
        """Close the browser — profile (including Cloudflare cookies) is saved automatically"""
        if self.context:
            await self.context.close()
        if self._playwright:
            await self._playwright.stop()
        print("🔌 Browser closed")

    @staticmethod
    async def _route_refulfill_documents(route):
        """Re-fulfill document responses to prevent Playwright aborting navigation
        with 'Download is starting' when the server sends unexpected headers.
        Download URLs (/dl/ paths) are passed through unchanged so Playwright's
        download event still fires correctly."""
        if route.request.resource_type != "document":
            await route.continue_()
            return
        # Let actual file-download URLs pass through untouched so expect_download works
        if "/dl/" in route.request.url:
            await route.continue_()
            return
        try:
            response = await route.fetch()
            headers = {k: v for k, v in response.headers.items()
                       if k.lower() != "content-disposition"}
            await route.fulfill(
                status=response.status,
                headers=headers,
                body=await response.body(),
            )
        except Exception:
            await route.continue_()
    
    async def scrape_category(
        self, 
        category_id: int, 
        category_slug: str, 
        page: int = 1,
        query_params: str = None
    ) -> dict:
        """Scrape a category page"""
        if query_params:
            # Clean up query params
            cleaned_params = re.sub(r'[&?]page=\d+', '', query_params)
            cleaned_params = re.sub(r'^page=\d+&?', '', cleaned_params)
            cleaned_params = re.sub(r'[&?]order=[^&]*', '', cleaned_params)
            cleaned_params = re.sub(r'^order=[^&]*&?', '', cleaned_params)
            url = f"https://{self.domain}/category/{category_id}/{category_slug}/s/?{cleaned_params}&order=popular&page={page}" if cleaned_params else f"https://{self.domain}/category/{category_id}/{category_slug}/s/?order=popular&page={page}"
        else:
            url = f"https://{self.domain}/category/{category_id}/{category_slug}/s/?languages%5B%5D=english&selected_content_types%5B%5D=book&order=popular&page={page}"
        
        return await self._scrape_page(url, f"category {category_id}/{category_slug}")
    
    async def scrape_search(
        self,
        search_term: str,
        page: int = 1,
        query_params: str = None
    ) -> dict:
        """Scrape a search results page"""
        from urllib.parse import quote
        encoded_term = quote(search_term)
        
        if query_params:
            cleaned_params = re.sub(r'[&?]page=\d+', '', query_params)
            cleaned_params = re.sub(r'^page=\d+&?', '', cleaned_params)
            cleaned_params = re.sub(r'[&?]order=[^&]*', '', cleaned_params)
            cleaned_params = re.sub(r'^order=[^&]*&?', '', cleaned_params)
            url = f"https://{self.domain}/s/{encoded_term}/?{cleaned_params}&order=popular&page={page}" if cleaned_params else f"https://{self.domain}/s/{encoded_term}/?order=popular&page={page}"
        else:
            url = f"https://{self.domain}/s/{encoded_term}/?languages%5B%5D=english&selected_content_types%5B%5D=book&order=popular&page={page}"
        
        return await self._scrape_page(url, f"search '{search_term}'")
    
    async def _scrape_page(self, url: str, scrape_type: str) -> dict:
        """Internal method to scrape a page and extract book data"""
        print(f"Scraping {scrape_type}, URL: {url}")
        
        # Retry loop — handles ERR_NETWORK_CHANGED on Chrome/WSL2 startup
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = await self.page.goto(url, wait_until="domcontentloaded", timeout=90000)

                # The site may issue a JS challenge that triggers an automatic redirect.
                # Wait for the page to settle, then check if we're past it.
                for _ in range(10):
                    try:
                        await self.page.wait_for_load_state("domcontentloaded", timeout=5000)
                        title = await self.page.title()
                    except Exception:
                        # Context destroyed = page is still navigating; wait and retry
                        await self.page.wait_for_timeout(2000)
                        continue
                    if "checking" in title.lower() or "just a moment" in title.lower():
                        print("⏳ Waiting for Cloudflare challenge to resolve...")
                        await self.page.wait_for_timeout(2000)
                    else:
                        break

                try:
                    status = response.status if response else 200
                except Exception:
                    status = 200  # response object stale after redirect — assume OK

                # If status is non-2xx, check whether the page actually loaded after
                # a Cloudflare challenge (the initial 503 stays on `response` but the
                # page may have navigated to the real content).
                if status not in (200, 0):
                    try:
                        current_title = await self.page.title()
                        challenge = "checking" in current_title.lower() or "just a moment" in current_title.lower()
                        if not challenge:
                            status = 200  # challenge resolved; real page is loaded
                    except Exception:
                        pass

                if status == 503:
                    print(f"❌ Got 503 error - service unavailable")
                    return {"success": False, "books_found": 0, "error": "503 Service Unavailable", "books_data": []}

                if status not in (200, 0):
                    print(f"❌ Got HTTP {status}")
                    return {"success": False, "books_found": 0, "error": f"HTTP {status}", "books_data": []}

                self._last_nav_time = time.time()
                break  # Success — exit retry loop
            except Exception as e:
                err_str = str(e)
                retryable = (
                    "ERR_NETWORK_CHANGED" in err_str
                    or "ERR_NETWORK" in err_str
                    or "ERR_ABORTED" in err_str
                    or "Download is starting" in err_str
                )
                if attempt < max_attempts - 1 and retryable:
                    wait = (attempt + 1) * 3
                    print(f"⚠️  Transient error on attempt {attempt + 1}/{max_attempts}, retrying in {wait}s: {err_str[:80]}")
                    await self.page.wait_for_timeout(wait * 1000)
                    continue
                print(f"❌ Error scraping page: {e}")
                return {"success": False, "books_found": 0, "error": str(e), "books_data": []}

        # Wait for book cards to render (z-bookcard is a web component)
        try:
            await self.page.wait_for_selector("z-bookcard", timeout=15000)
        except Exception:
            pass  # No book cards on this page (end of results)

        # Extract book data using JavaScript
        books_data = await self.page.evaluate("""
                () => {
                    const books = [];
                    document.querySelectorAll('z-bookcard').forEach(card => {
                        const id = card.getAttribute('id');
                        const hash = card.getAttribute('termshash');
                        const dl = card.getAttribute('download');   // e.g. /dl/31Bvk4WPwv
                        const title = card.querySelector('[slot="title"]')?.textContent?.trim() || 'Unknown Title';
                        const author = card.querySelector('[slot="author"]')?.textContent?.trim() || 'Unknown Author';
                        if (id && hash) {
                            books.push({ id, hash, title, authors: author, dl: dl || '' });
                        }
                    });
                    return books;
                }
            """)
        
        print(f"✅ Found {len(books_data)} books on page")
        return {
            "success": True,
            "books_found": len(books_data),
            "books_data": books_data
        }
    
    async def download_book_direct(self, dl_path: str, output_dir: str, save_timeout: int = 600) -> dict:
        """Download a book by opening the /dl/ URL in a new tab — mimics clicking download."""
        dl_url = f"https://{self.domain}{dl_path}"
        os.makedirs(output_dir, exist_ok=True)

        # Refresh CF session if it's been idle too long — prevents download timeouts
        # caused by expired cf_clearance cookies on long-running sessions.
        await self._keepalive_if_needed()

        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            dl_page = None
            try:
                dl_page = await self.context.new_page()
                # Bypass the context-level route handler on this page so that
                # download redirects (which may not contain '/dl/') are not
                # intercepted and stripped of Content-Disposition.
                # Must be a proper async handler — a sync lambda returning a
                # coroutine would leave it unawaited, hanging the request.
                async def _passthrough(route):
                    await route.continue_()
                await dl_page.route("**/*", _passthrough)
                async with dl_page.expect_download(timeout=30000) as dl_info:
                    try:
                        await dl_page.goto(dl_url, wait_until="commit", timeout=15000)
                    except Exception:
                        pass  # "Download is starting" is expected

                download = await dl_info.value
                raw_filename = download.suggested_filename
                filename = _clean_zlib_filename(raw_filename)
                filepath = os.path.join(output_dir, filename)

                # Show live progress by polling Playwright's temp file size
                async def save_with_progress():
                    save_task = asyncio.create_task(download.save_as(filepath))
                    print(f"      ⬇️  {filename[:55]}", end="", flush=True)
                    last_size = 0
                    while not save_task.done():
                        await asyncio.sleep(3)
                        try:
                            tmp = await download.path()
                            if tmp and os.path.exists(tmp):
                                size = os.path.getsize(tmp) / (1024 * 1024)
                                if size != last_size:
                                    print(f"\r      ⬇️  {filename[:45]} {size:.1f} MB...", end="", flush=True)
                                    last_size = size
                        except Exception:
                            pass
                    await save_task  # re-raise any exception
                    print()  # newline after progress

                await asyncio.wait_for(save_with_progress(), timeout=save_timeout)

                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"      ✅ {filename[:55]} ({size_mb:.1f} MB)")
                return {"success": True, "filepath": filepath, "filename": filename}

            except asyncio.TimeoutError:
                print(f"\n      ⚠️  Transfer exceeded {save_timeout}s — skipping.")
                try:
                    if 'filepath' in dir() and os.path.exists(filepath):
                        os.remove(filepath)
                except Exception:
                    pass
                return {"success": False, "error": f"save_as timeout after {save_timeout}s"}
            except Exception as e:
                err = str(e)
                if attempt < max_attempts:
                    print(f"\n      ⚠️  Attempt {attempt}/{max_attempts} failed ({err[:60]}). Retrying in 5s...")
                    await asyncio.sleep(5)
                else:
                    return {"success": False, "error": err}
            finally:
                if dl_page:
                    try:
                        await dl_page.close()
                    except Exception:
                        pass


# Synchronous wrapper for easier use
class BrowserScraperSync:
    """Synchronous wrapper for BrowserScraper"""
    
    def __init__(
        self,
        domain: str = "z-library.sk",
        cookies_file: str = "browser_cookies.json",
        mode: str = "local",
        windows_host: str = None,
        debug_port: int = 9222,
    ):
        self._scraper = BrowserScraper(domain, cookies_file)
        self._loop = None
    
    def _get_loop(self):
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop
    
    def start(self, headless: bool = True):
        return self._get_loop().run_until_complete(self._scraper.start(headless))
    
    def close(self):
        if self._loop:
            try:
                self._loop.run_until_complete(self._scraper.close())
            except Exception:
                pass
            finally:
                # Explicitly close and discard the loop so the next start() gets a
                # completely fresh event loop. Without this, hundreds of
                # run_until_complete() calls accumulate stale internal asyncio state
                # (pending callbacks, I/O watchers, cancelled tasks) which eventually
                # causes Playwright operations to silently fail — matching the
                # "works after manual restart but not auto-restart" symptom.
                try:
                    self._loop.close()
                except Exception:
                    pass
                self._loop = None
        # Recreate the inner scraper so all its state (context, page, timers) is fresh.
        domain = self._scraper.domain
        cookies_file = self._scraper.cookies_file
        self._scraper = BrowserScraper(domain, cookies_file)
    
    def scrape_category(self, category_id: int, category_slug: str, page: int = 1, query_params: str = None) -> dict:
        return self._get_loop().run_until_complete(
            self._scraper.scrape_category(category_id, category_slug, page, query_params)
        )
    
    def scrape_search(self, search_term: str, page: int = 1, query_params: str = None) -> dict:
        return self._get_loop().run_until_complete(
            self._scraper.scrape_search(search_term, page, query_params)
        )

    def download_book_direct(self, dl_path: str, output_dir: str) -> dict:
        return self._get_loop().run_until_complete(
            self._scraper.download_book_direct(dl_path, output_dir)
        )


# Test function
async def test_scraper():
    """Test the browser scraper"""
    scraper = BrowserScraper()
    
    try:
        await scraper.start(headless=True)
        
        # Test category scrape
        result = await scraper.scrape_category(311, "Cookbooks-Food--Wine", page=1)
        
        print(f"\n📊 Results:")
        print(f"   Success: {result['success']}")
        print(f"   Books found: {result['books_found']}")
        
        if result['books_data']:
            print(f"\n📚 First 3 books:")
            for book in result['books_data'][:3]:
                print(f"   - {book['title'][:50]}... by {book['authors'][:30]}...")
                print(f"     ID: {book['id']}, Hash: {book['hash']}")
        
    finally:
        await scraper.close()


async def test_with_persistent_profile():
    """Test using a persistent browser profile that remembers login"""
    from playwright.async_api import async_playwright
    
    print("🚀 Testing with persistent browser profile...")
    print("   This will save browser state between runs")
    
    playwright = await async_playwright().start()
    
    # Use persistent context - saves cookies, localStorage, etc.
    user_data_dir = os.path.join(os.path.dirname(__file__), "browser_profile")
    
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir,
        headless=False,
        args=[
            '--disable-blink-features=AutomationControlled',
        ],
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
    )
    
    page = context.pages[0] if context.pages else await context.new_page()
    
    if HAS_STEALTH and _stealth:
        await _stealth.apply_stealth_async(page)
        print("✅ Stealth mode enabled")
    
    try:
        # First, go to main page to establish session
        print("📍 Navigating to Z-Library main page first...")
        await page.goto(f"https://z-library.sk/", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)
        
        # Check if we need to login
        login_check = await page.query_selector('a[href*="login"], a:has-text("Sign in")')
        if login_check:
            print("⚠️ Not logged in! Please log in manually in the browser window...")
            print("   After logging in, the session will be saved for future runs.")
            print("   Press Enter in terminal when done logging in...")
            input()
        
        # Now try the category page
        url = "https://z-library.sk/category/311/Cookbooks-Food--Wine/s/?languages%5B0%5D=english&order=popular&page=1"
        print(f"📖 Navigating to category page: {url}")
        
        response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        print(f"   Response status: {response.status}")
        
        if response.status == 200:
            await page.wait_for_timeout(2000)
            
            # Extract books
            books_data = await page.evaluate("""
                () => {
                    const books = [];
                    const bookCards = document.querySelectorAll('z-bookcard');
                    
                    bookCards.forEach(card => {
                        const id = card.getAttribute('id');
                        const href = card.querySelector('a[href*="/book/"]')?.getAttribute('href');
                        const hash = href ? href.match(/\\/book\\/\\d+\\/([a-z0-9]+)/)?.[1] : null;
                        const title = card.querySelector('[slot="title"]')?.textContent?.trim() || 'Unknown Title';
                        const author = card.querySelector('[slot="author"]')?.textContent?.trim() || 'Unknown Author';
                        
                        if (id && hash) {
                            books.push({ id, hash, title, authors: author });
                        }
                    });
                    
                    return books;
                }
            """)
            
            print(f"\n✅ SUCCESS! Found {len(books_data)} books")
            
            if books_data:
                print(f"\n📚 First 5 books:")
                for book in books_data[:5]:
                    print(f"   - {book['title'][:60]}...")
                    print(f"     ID: {book['id']}, Hash: {book['hash']}")
        else:
            print(f"❌ Got HTTP {response.status}")
        
        print("\n⏳ Keeping browser open for 10 seconds...")
        await page.wait_for_timeout(10000)
        
    finally:
        await context.close()
        await playwright.stop()
        print("🔌 Browser closed")


if __name__ == "__main__":
    asyncio.run(test_with_persistent_profile())


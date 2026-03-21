"""
Windows Browser Scraper for Z-Library
Connects to Firefox running on Windows from WSL2
"""

import asyncio
import json
import re
import os
import subprocess
from playwright.async_api import async_playwright


async def scrape_with_windows_firefox():
    """
    Connect to Firefox on Windows and scrape Z-Library
    
    SETUP: Run this command in Windows PowerShell first:
    
    & "C:\Program Files\Mozilla Firefox\firefox.exe" -start-debugger-server 9222 -profile "%TEMP%\firefox-debug"
    
    Or for Firefox in AppData:
    & "$env:LOCALAPPDATA\Mozilla Firefox\firefox.exe" -start-debugger-server 9222 -profile "$env:TEMP\firefox-debug"
    """
    
    print("=" * 60)
    print("Windows Firefox Browser Scraper")
    print("=" * 60)
    print()
    print("BEFORE RUNNING THIS SCRIPT:")
    print()
    print("1. Open PowerShell on Windows (not WSL)")
    print("2. Run one of these commands to start Firefox with debugging:")
    print()
    print('   Standard install:')
    print('   & "C:\\Program Files\\Mozilla Firefox\\firefox.exe" --start-debugger-server 9222')
    print()
    print("3. In Firefox, navigate to https://z-library.sk and LOG IN")
    print("4. After logging in, come back here and press Enter")
    print()
    input("Press Enter when Firefox is running and you're logged into Z-Library...")
    
    playwright = await async_playwright().start()
    
    try:
        # Connect to Firefox running on Windows
        # WSL2 can access Windows localhost via the host IP
        
        # Try to get Windows host IP
        try:
            result = subprocess.run(
                ["cat", "/etc/resolv.conf"],
                capture_output=True,
                text=True
            )
            windows_host = None
            for line in result.stdout.split('\n'):
                if 'nameserver' in line:
                    windows_host = line.split()[1]
                    break
            
            if not windows_host:
                windows_host = "localhost"
        except:
            windows_host = "localhost"
        
        print(f"Connecting to Firefox at {windows_host}:9222...")
        
        # Connect to the browser
        browser = await playwright.firefox.connect_over_cdp(f"http://{windows_host}:9222")
        
        print("✅ Connected to Firefox!")
        
        # Get existing context and pages
        contexts = browser.contexts
        if not contexts:
            print("❌ No browser contexts found")
            return
        
        context = contexts[0]
        pages = context.pages
        
        if not pages:
            page = await context.new_page()
        else:
            # Find the Z-Library page or use the first one
            page = None
            for p in pages:
                if 'z-library' in p.url or 'zlibrary' in p.url:
                    page = p
                    break
            if not page:
                page = pages[0]
        
        print(f"Using page: {page.url}")
        
        # Navigate to category page
        url = "https://z-library.sk/category/311/Cookbooks-Food--Wine/s/?languages%5B0%5D=english&order=popular&page=1"
        print(f"\n📖 Navigating to: {url}")
        
        response = await page.goto(url, wait_until="networkidle", timeout=60000)
        
        print(f"Response status: {response.status}")
        
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
                
                # Save to file
                with open('scraped_books.json', 'w') as f:
                    json.dump(books_data, f, indent=2)
                print(f"\n💾 Saved all {len(books_data)} books to scraped_books.json")
        else:
            print(f"❌ Got HTTP {response.status}")
            content = await page.content()
            print(f"Page content preview: {content[:500]}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        print("Troubleshooting:")
        print("1. Make sure Firefox is running with: --start-debugger-server 9222")
        print("2. Make sure you're logged into Z-Library in Firefox")
        print("3. Try connecting to 'localhost' instead if using WSLg")
        
    finally:
        await playwright.stop()


async def scrape_with_chromium_cdp():
    """
    Alternative: Connect to Chrome/Edge on Windows
    
    Start Chrome with:
    "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
    
    Or Edge:
    "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=9222
    """
    
    print("=" * 60)
    print("Windows Chrome/Edge Browser Scraper")
    print("=" * 60)
    print()
    print("BEFORE RUNNING THIS SCRIPT:")
    print()
    print("1. Close ALL Chrome/Edge windows first")
    print("2. Open PowerShell on Windows (not WSL)")
    print("3. Run one of these commands:")
    print()
    print('   For Chrome:')
    print('   & "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\\temp\\chrome-debug"')
    print()
    print('   For Edge:')
    print('   & "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222 --user-data-dir="C:\\temp\\edge-debug"')
    print()
    print("4. In the browser, navigate to https://z-library.sk and LOG IN")
    print("5. After logging in, come back here and press Enter")
    print()
    input("Press Enter when browser is running and you're logged into Z-Library...")
    
    playwright = await async_playwright().start()
    
    try:
        # Get Windows host IP for WSL2
        try:
            result = subprocess.run(
                ["cat", "/etc/resolv.conf"],
                capture_output=True,
                text=True
            )
            windows_host = None
            for line in result.stdout.split('\n'):
                if 'nameserver' in line:
                    windows_host = line.split()[1]
                    break
            if not windows_host:
                windows_host = "localhost"
        except:
            windows_host = "localhost"
        
        print(f"Connecting to Chrome/Edge at {windows_host}:9222...")
        
        browser = await playwright.chromium.connect_over_cdp(f"http://{windows_host}:9222")
        
        print("✅ Connected to browser!")
        
        # Get the default context
        context = browser.contexts[0]
        pages = context.pages
        
        # Find Z-Library page or use first
        page = None
        for p in pages:
            if 'z-library' in p.url or 'zlibrary' in p.url:
                page = p
                print(f"Found Z-Library tab: {p.url}")
                break
        
        if not page:
            page = pages[0] if pages else await context.new_page()
        
        print(f"Using page: {page.url}")
        
        # Navigate to category
        url = "https://z-library.sk/category/311/Cookbooks-Food--Wine/s/?languages%5B0%5D=english&order=popular&page=1"
        print(f"\n📖 Navigating to: {url}")
        
        response = await page.goto(url, wait_until="networkidle", timeout=60000)
        
        print(f"Response status: {response.status}")
        
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
                
                # Save to file
                with open('scraped_books.json', 'w') as f:
                    json.dump(books_data, f, indent=2)
                print(f"\n💾 Saved all {len(books_data)} books to scraped_books.json")
                
        else:
            print(f"❌ Got HTTP {response.status}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await playwright.stop()


if __name__ == "__main__":
    print("Which browser do you want to use?")
    print("1. Firefox")
    print("2. Chrome/Edge")
    print()
    choice = input("Enter 1 or 2: ").strip()
    
    if choice == "1":
        asyncio.run(scrape_with_windows_firefox())
    else:
        asyncio.run(scrape_with_chromium_cdp())


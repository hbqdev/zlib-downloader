# How to Export Browser Cookies to Bypass 503 Errors

Z-Library blocks automated requests but your browser works fine. Solution: Use your browser's cookies!

## Quick Start (Firefox on WSL2)

### Method 1: Using Browser DevTools (Easiest)

1. **Open Firefox** and go to `https://z-library.sk`
2. **Log in** to your Z-Library account
3. **Press F12** to open Developer Tools
4. **Go to "Storage" tab** (or "Application" in Chrome)
5. **Click "Cookies"** → `https://z-library.sk`
6. **Copy these specific cookies** (right-click → Copy Value):
   - `remix_userid`
   - `remix_userkey`
   - Any other cookies you see

7. **Create `browser_cookies.json`** in the zlib-downloader directory:

```json
{
  "remix_userid": "paste_value_here",
  "remix_userkey": "paste_value_here",
  "siteLanguageV2": "en"
}
```

8. **Run the converter**:
```bash
python export_browser_cookies.py
```

9. **Run the downloader** - it will now use your browser cookies:
```bash
python zlibdownload.py
```

### Method 2: Using Browser Extension (More Complete)

1. **Install Cookie Extension**:
   - Firefox: [Cookie-Editor](https://addons.mozilla.org/en-US/firefox/addon/cookie-editor/)
   - Chrome: [EditThisCookie](https://chrome.google.com/webstore/detail/editthiscookie/)

2. **Export Cookies**:
   - Click the extension icon
   - Click "Export" (get JSON format)
   - Save as `browser_cookies.json`

3. **Convert cookies**:
```bash
python export_browser_cookies.py
```

4. **Run the downloader**:
```bash
python zlibdownload.py
```

## How It Works

- Your browser has a valid session that Z-Library trusts
- We export those cookies and use them in Python
- The script now looks identical to your browser
- Bypasses bot detection completely!

## Troubleshooting

**Still getting 503?**
- Make sure you're logged into Z-Library in the browser first
- Cookies expire - re-export them if they stop working
- Check that `converted_cookies.json` was created successfully

**Script says "Loaded X browser cookies"?**
- ✅ Good! Cookies are being used
- If still getting 503, cookies may have expired - re-export them

**No cookie files found?**
- The script will work normally with API authentication
- But may still hit 503 errors on browsing pages


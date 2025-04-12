"""
Copyright (c) 2023-2024 Bipinkrish
This file is part of the Zlibrary-API by Bipinkrish
Zlibrary-API / Zlibrary.py

For more information, see: 
https://github.com/bipinkrish/Zlibrary-API/
"""

"""
This was modified by Tin Tran to scrape the Z-Library website for book information.
"""

import requests
import re
import html
import os # Needed for file operations
import json # Import json module here

class Zlibrary:
    def __init__(
        self,
        email: str = None,
        password: str = None,
        domain: str = "z-library.sk",
        remix_userid: [int, str] = None,
        remix_userkey: str = None,
    ):
        self.__email: str
        self.__name: str
        self.__kindle_email: str
        self.__remix_userid: [int, str]
        self.__remix_userkey: str
        self.__domain = domain

        self.__loggedin = False
        self.__headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        }
        self.__cookies = {
            "siteLanguageV2": "en",
        }

        if email is not None and password is not None:
            self.login(email, password)
        elif remix_userid is not None and remix_userkey is not None:
            self.loginWithToken(remix_userid, remix_userkey)

    def __setValues(self, response, domain_override=None) -> dict[str, str]:
        if not response["success"]:
            return response
        self.__email = response["user"]["email"]
        self.__name = response["user"]["name"]
        self.__kindle_email = response["user"]["kindle_email"]
        self.__remix_userid = str(response["user"]["id"])
        self.__remix_userkey = response["user"]["remix_userkey"]
        self.__cookies["remix_userid"] = self.__remix_userid
        self.__cookies["remix_userkey"] = self.__remix_userkey
        if domain_override:
            self.__domain = domain_override
        self.__loggedin = True
        return response

    def __login(self, email, password, domain_to_try=None) -> dict[str, str]:
        target_domain = domain_to_try or self.__domain
        return self.__setValues(
            self.__makePostRequest(
                "/eapi/user/login",
                data={
                    "email": email,
                    "password": password,
                },
                domain_override=target_domain,
                override=True,
            ),
            domain_override=target_domain
        )

    def __checkIDandKey(self, remix_userid, remix_userkey) -> dict[str, str]:
        return self.__setValues(
            self.__makeGetRequest(
                "/eapi/user/profile",
                cookies=self.__cookies,
            )
        )

    def login(self, email: str, password: str) -> dict[str, str]:
        result = self.__login(email, password)
        return result

    def loginWithToken(
        self, remix_userid: [int, str], remix_userkey: str
    ) -> dict[str, str]:
        return self.__checkIDandKey(remix_userid, remix_userkey)

    def __makePostRequest(
        self, url: str, data: dict = {}, override=False, domain_override: str = None
    ) -> dict[str, str]:
        if not self.isLoggedIn() and override is False:
            print("Not logged in")
            return None

        target_domain = domain_override or self.__domain

        try:
            response = requests.post(
                "https://" + target_domain + url,
                data=data,
                cookies=self.__cookies,
                headers=self.__headers,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error in POST request to {url}: {e}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            print(f"Error decoding JSON from POST {url}: {e} - Response: {response.text[:200]}")
            return None

    def __makeGetRequest(
        self, url: str, params: dict = {}, cookies=None, domain_override: str = None
    ) -> dict[str, str]:
        if not self.isLoggedIn() and cookies is None:
            print("Not logged in")
            return None

        target_domain = domain_override or self.__domain
        target_cookies = self.__cookies if cookies is None else cookies

        try:
            response = requests.get(
                "https://" + target_domain + url,
                params=params,
                cookies=target_cookies,
                headers=self.__headers,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error in GET request to {url}: {e}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            print(f"Error decoding JSON from GET {url}: {e} - Response: {response.text[:200]}")
            return None

    def getProfile(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/user/profile")

    def getMostPopular(self, switch_language: str = None) -> dict[str, str]:
        if switch_language is not None:
            return self.__makeGetRequest(
                "/eapi/book/most-popular", {"switch-language": switch_language}
            )
        return self.__makeGetRequest("/eapi/book/most-popular")

    def getRecently(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/book/recently")

    def getUserRecommended(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/user/book/recommended")

    def deleteUserBook(self, bookid: [int, str]) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/user/book/{bookid}/delete")

    def unsaveUserBook(self, bookid: [int, str]) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/user/book/{bookid}/unsave")

    def getBookForamt(self, bookid: [int, str], hashid: str) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/book/{bookid}/{hashid}/formats")

    def getDonations(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/user/donations")

    def getUserDownloaded(
        self, order: str = None, page: int = None, limit: int = None
    ) -> dict[str, str]:
        """
        order takes one of the values\n
        ["year",...]
        """
        params = {
            k: v
            for k, v in {"order": order, "page": page, "limit": limit}.items()
            if v is not None
        }
        return self.__makeGetRequest("/eapi/user/book/downloaded", params)

    def getExtensions(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/info/extensions")

    def getDomains(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/info/domains")

    def getLanguages(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/info/languages")

    def getPlans(self, switch_language: str = None) -> dict[str, str]:
        if switch_language is not None:
            return self.__makeGetRequest(
                "/eapi/info/plans", {"switch-language": switch_language}
            )
        return self.__makeGetRequest("/eapi/info/plans")

    def getUserSaved(
        self, order: str = None, page: int = None, limit: int = None
    ) -> dict[str, str]:
        """
        order takes one of the values\n
        ["year",...]
        """
        params = {
            k: v
            for k, v in {"order": order, "page": page, "limit": limit}.items()
            if v is not None
        }
        return self.__makeGetRequest("/eapi/user/book/saved", params)

    def getInfo(self, switch_language: str = None) -> dict[str, str]:
        if switch_language is not None:
            return self.__makeGetRequest(
                "/eapi/info", {"switch-language": switch_language}
            )
        return self.__makeGetRequest("/eapi/info")

    def hideBanner(self) -> dict[str, str]:
        return self.__makeGetRequest("/eapi/user/hide-banner")

    def recoverPassword(self, email: str) -> dict[str, str]:
        return self.__makePostRequest(
            "/eapi/user/password-recovery", {"email": email}, override=True
        )

    def makeRegistration(self, email: str, password: str, name: str) -> dict[str, str]:
        return self.__makePostRequest(
            "/eapi/user/registration",
            {"email": email, "password": password, "name": name},
            override=True,
        )

    def resendConfirmation(self) -> dict[str, str]:
        return self.__makePostRequest("/eapi/user/email/confirmation/resend")

    def saveBook(self, bookid: [int, str]) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/user/book/{bookid}/save")

    def sendTo(self, bookid: [int, str], hashid: str, totype: str) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/book/{bookid}/{hashid}/send-to-{totype}")

    def getBookInfo(
        self, bookid: [int, str], hashid: str, switch_language: str = None
    ) -> dict[str, str]:
        if switch_language is not None:
            return self.__makeGetRequest(
                f"/eapi/book/{bookid}/{hashid}", {"switch-language": switch_language}
            )
        return self.__makeGetRequest(f"/eapi/book/{bookid}/{hashid}")

    def getSimilar(self, bookid: [int, str], hashid: str) -> dict[str, str]:
        return self.__makeGetRequest(f"/eapi/book/{bookid}/{hashid}/similar")

    def makeTokenSigin(self, name: str, id_token: str) -> dict[str, str]:
        return self.__makePostRequest(
            "/eapi/user/token-sign-in",
            {"name": name, "id_token": id_token},
            override=True,
        )

    def updateInfo(
        self,
        email: str = None,
        password: str = None,
        name: str = None,
        kindle_email: str = None,
    ) -> dict[str, str]:
        return self.__makePostRequest(
            "/eapi/user/update",
            {
                k: v
                for k, v in {
                    "email": email,
                    "password": password,
                    "name": name,
                    "kindle_email": kindle_email,
                }.items()
                if v is not None
            },
        )

    def search(
        self,
        message: str = None,
        yearFrom: int = None,
        yearTo: int = None,
        languages: str = None,
        extensions: [str] = None,
        order: str = None,
        page: int = None,
        limit: int = None,
    ) -> dict[str, str]:
        return self.__makePostRequest(
            "/eapi/book/search",
            {
                k: v
                for k, v in {
                    "message": message,
                    "yearFrom": yearFrom,
                    "yearTo": yearTo,
                    "languages": languages,
                    "extensions[]": extensions,
                    "order": order,
                    "page": page,
                    "limit": limit,
                }.items()
                if v is not None
            },
        )

    def __getImageData(self, url: str) -> bytes | None:
        try:
            res = requests.get(url, headers=self.__headers)
            res.raise_for_status()
            if res.status_code == 200:
                return res.content
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error getting image data from {url}: {e}")
            return None

    def getImage(self, book: dict[str, str]) -> bytes | None:
        cover_url = book.get("cover")
        if cover_url:
            return self.__getImageData(cover_url)
        return None

    def __getBookFile(self, bookid: [int, str], hashid: str) -> tuple[str, bytes] | None:
        response = self.__makeGetRequest(f"/eapi/book/{bookid}/{hashid}/file")
        
        if not response or "file" not in response:
            print(f"  Error: API response for book {bookid}/{hashid} did not contain 'file' key.")
            print(f"  API Response: {response}")
            return None

        file_info = response["file"]
        filename = file_info.get("description", f"book_{bookid}")
        author_info = file_info.get("author")
        extension = file_info.get("extension", "bin")

        try:
            if author_info:
                filename += f" ({author_info})"
        except Exception:
            pass
        finally:
            filename += f".{extension}"

        ddl = file_info.get("downloadLink")
        if not ddl:
            print(f"Error: No downloadLink found in API response for book {bookid}")
            return None

        download_headers = self.__headers.copy()
        try:
            authority = ddl.split("/")[2]
            download_headers["authority"] = authority 
        except IndexError:
            print("Warning: Could not parse authority from download link.")

        try:
            res = requests.get(ddl, headers=download_headers, stream=True, timeout=60) 
            res.raise_for_status()
            if res.status_code == 200:
                content = res.content 
                return filename, content
            else:
                print(f"Error: Download request for book {bookid} returned status {res.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error downloading book file from {ddl}: {e}")
            return None

    def downloadBook(self, book: dict[str, str]) -> tuple[str, bytes] | None:
        book_id = book.get("id")
        book_hash = book.get("hash")
        if not book_id or not book_hash:
            print("Error: Book dictionary missing id or hash for download.")
            return None
        return self.__getBookFile(book_id, book_hash)

    def isLoggedIn(self) -> bool:
        return self.__loggedin

    def sendCode(self, email: str, password: str, name: str) -> dict[str, str]:
        usr_data = {
            "email": email,
            "password": password,
            "name": name,
            "rx": 215,
            "action": "registration",
            "site_mode": "books",
            "isSinglelogin": 1,
        }
        response = self.__makePostRequest(
            "/papi/user/verification/send-code", data=usr_data, override=True
        )
        if response["success"]:
            response["msg"] = (
                "Verification code is sent to mail, use verify_code to complete registration"
            )
        return response

    def verifyCode(
        self, email: str, password: str, name: str, code: str
    ) -> dict[str, str]:
        usr_data = {
            "email": email,
            "password": password,
            "name": name,
            "verifyCode": code,
            "rx": 215,
            "action": "registration",
            "redirectUrl": "",
            "isModa": True,
            "gg_json_mode": 1,
        }
        return self.__makePostRequest("/rpc.php", data=usr_data, override=True)

    def getDownloadsLeft(self) -> int:
        user_profile: dict = self.getProfile()["user"]
        return user_profile.get("downloads_limit", 10) - user_profile.get(
            "downloads_today", 0
        )

    def search_scrape(
        self,
        config: dict # Pass the whole config dictionary
     ) -> dict:
        """Searches for books by scraping a specific URL or constructing one from config.
           Prioritizes 'scrape_url' in config if present.
           Uses Regex to extract ID, Hash, Title, Author from raw HTML.
           Saves results to to_download.txt without checking download history.
        """
        if not self.isLoggedIn():
            print("Not logged in")
            return {"success": False, "error": "Not logged in", "books": []}

        target_url = config.get("scrape_url")
        params = None # Params are usually part of scrape_url

        if not target_url:
            print("Error: scrape_url must be provided in config for search_scrape method.")
            return {"success": False, "error": "scrape_url not provided", "books": []}
        else:
            print(f"Scraping specific URL from config: {target_url}")
            # Ensure the URL uses the configured domain if it's relative (optional)
            if not target_url.startswith("http"):
                target_url = f"https://{self.__domain}{target_url}"

        # --- Step 1: Fetch the HTML ---
        try:
            response = requests.get(
                target_url,
                params=params, # Usually None when using specific scrape_url
                cookies=self.__cookies,
                headers=self.__headers,
                timeout=30
            )
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Error during request to {target_url}: {e}")
            return {"success": False, "error": str(e), "books": []}

        html_content = response.text

        # --- Step 2: Extract only <z-bookcard> blocks and save ---
        print("Extracting book card blocks from HTML...")
        card_pattern = re.compile(r'<z-bookcard.*?/z-bookcard>', re.DOTALL | re.IGNORECASE)
        card_matches = card_pattern.findall(html_content)
        filtered_html_content = "\n".join(card_matches) # Join blocks with newline

        # --- Save the filtered HTML for debugging/inspection (Optional but helpful) ---
        output_filename = "raw_html_output.txt"
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write(filtered_html_content)
            print(f"Successfully saved {len(card_matches)} book card blocks to {output_filename}")
        except IOError as e:
            print(f"Warning: Error saving filtered HTML to file {output_filename}: {e}")
            # Continue even if saving fails, as parsing uses the variable directly

        # --- Step 3: Parse file content with Regex and Construct List ---
        books_data = []

        # Regex patterns (same as before)
        title_inner_pattern = re.compile(r'<div\s+slot=\"title\">(.*?)</div>', re.IGNORECASE)
        author_inner_pattern = re.compile(r'<div\s+slot=\"author\">(.*?)</div>', re.IGNORECASE)

        print(f"Attempting regex extraction from scraped HTML...")

        matches_found = 0
        for card_match in card_pattern.finditer(filtered_html_content): # Iterate directly on filtered content
            card_text = card_match.group(0) # Get the full text of the block

            # Reset for each card
            book_id = None
            book_hash = None
            title = None
            authors = None

            try:
                # Find ID within the block
                id_match = re.search(r'id=\"(\d+)\"', card_text)
                if id_match: book_id = id_match.group(1)

                # Find Hash within the block
                hash_match = re.search(r'href=\"/book/\d+/([a-z0-9]+)/.*?\"', card_text)
                if hash_match: book_hash = hash_match.group(1)

                # Extract title from inner content
                title_match = title_inner_pattern.search(card_text)
                if title_match: title = html.unescape(title_match.group(1).strip())

                # Extract author from inner content
                author_match = author_inner_pattern.search(card_text)
                if author_match: authors = html.unescape(author_match.group(1).strip())

                # Basic validation
                if book_id and book_hash:
                    matches_found += 1 # Count successful extractions

                    # Use extracted title/author or placeholders if extraction failed
                    title_print = title if title else "Title Not Found"
                    authors_print = authors if authors else "Author Not Found"
                    # print(f"  Regex Extracted: ID={book_id}, Hash={book_hash}, Title='{title_print}', Authors='{authors_print}'") # Verbose logging

                    # Append data to be written to file (without download flag)
                    books_data.append({
                        "id": book_id,
                        "hash": book_hash,
                        "title": title_print, # Use extracted or placeholder
                        "authors": authors_print, # Use extracted or placeholder
                    })
                else:
                    # Print details only if it looked like a card but failed ID/Hash
                    print(f"  Regex Warning: Skipped block due to missing ID or Hash. Card start: {card_text[:100]}...")

            except Exception as e:
                import traceback
                print(f"Error processing card block: {e}\n{traceback.format_exc()} - Card start: {card_text[:100]}...")

        print(f"Regex successfully extracted info for {matches_found} books.")
        if not books_data:
             print("Warning: Regex did not successfully extract any valid book data (ID/Hash/Title/Author) from the scraped HTML.")
             # Return success=False if nothing useful extracted
             return {"success": False, "books": []}

        # Apply limit if specified
        limit = config.get("limit") # Get limit from config
        if limit is not None and len(books_data) > limit:
            print(f"Applying limit: Using first {limit} of {len(books_data)} extracted books.")
            books_data = books_data[:limit]

        # --- Step 4: Save extracted data to to_download.txt ---
        download_filename = "to_download.txt"
        try:
            with open(download_filename, "w", encoding="utf-8") as f:
                for book in books_data:
                    # Simple pipe-separated format: ID|HASH|TITLE|AUTHOR
                    line = f"{book['id']}|{book['hash']}|{book['title'].replace('|', ' ')}|{book['authors'].replace('|', ' ')}\n"
                    f.write(line) # Write the line including the newline character
            print(f"Successfully saved data for {len(books_data)} books to {download_filename}")
            return {"success": True} # Indicate success, data is in the file
        except IOError as e:
            print(f"Error saving data to file {download_filename}: {e}")
            return {"success": False, "error": f"Failed to save download data: {e}"}

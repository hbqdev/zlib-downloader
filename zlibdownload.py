from Zlibrary import Zlibrary
import os
import time
import json
import cbconnect
import sys
import re
import logging
from datetime import datetime
from tqdm import tqdm

def load_json(file_path):
    """Loads data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"❌ Error: File '{file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from '{file_path}'. Check format.")
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred loading '{file_path}': {e}")
        return None

def save_json(data, file_path):
    """Saves data to a JSON file with indentation."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2) 
        print(f"✅ Successfully saved updated data to '{file_path}'")
        return True
    except IOError as e:
        print(f"❌ Error saving data to '{file_path}': {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred saving to '{file_path}': {e}")
        return False

def fetch_and_save_user_history(z_instance):
    """Fetches all pages of user download history and saves raw responses."""
    print("\n📚 Fetching Full User Download History...")
    raw_history_responses = []
    current_page = 1
    page_limit = 200
    total_ids_found = 0

    while True:
        print(f"  📄 Fetching history page {current_page} (limit {page_limit})...")
        try:
            history_response = z_instance.getUserDownloaded(limit=page_limit, page=current_page)
            
            if not history_response:
                print(f"  ⚠️ API request failed for history page {current_page}. Stopping.")
                break
            
            raw_history_responses.append(json.dumps(history_response, indent=2))

            if history_response.get('success') and history_response.get('history'):
                num_items_on_page = len(history_response['history'])
                total_ids_found += num_items_on_page
                print(f"    📊 Found {num_items_on_page} items. Total: {total_ids_found}")
                
                if num_items_on_page < page_limit:
                    print("  🏁 Reached the last page of history.")
                    break
                
                current_page += 1
                time.sleep(0.5)
            else:
                if not history_response.get('success'):
                    print(f"  ❌ API reported failure: {history_response.get('error', 'N/A')}")
                elif not history_response.get('history'):
                     print(f"  ❌ API response missing 'history' key or it's empty.")
                print("  🛑 Stopping history fetch.")
                break
                
        except Exception as e:
            import traceback
            print(f"  ❌ Error during history fetch: {e}")
            print(traceback.format_exc())
            print("  🛑 Stopping history fetch due to error.")
            break

    if raw_history_responses:
        raw_history_filename = "raw_api_history.txt"
        print(f"\n  💾 Saving {len(raw_history_responses)} API responses to {raw_history_filename}...")
        try:
            with open(raw_history_filename, "w", encoding="utf-8") as f_raw:
                separator = "\n\n---\n\n" 
                f_raw.write(separator.join(raw_history_responses)) 
            print(f"  ✅ Successfully saved raw API history responses.")
        except IOError as e:
            print(f"  ❌ Error saving raw API history: {e}")
    else:
         print("\n  ℹ️ No raw history responses collected to save.")
    print("✅ Finished Fetching Full User Download History")

def run_download_process(config_file="config.json", categories_file="categories.json"):
    """
    Main process: Loads config & categories, iterates through enabled categories
    and pages, scrapes, checks Couchbase, downloads, and marks in Couchbase.
    """
    # Load Configuration
    print(f"⚙️ Loading configuration from '{config_file}'...")
    config = load_json(config_file)
    if not config:
        sys.exit(1)
    
    print(f"📋 Loading categories from '{categories_file}'...")
    categories = load_json(categories_file)
    if not categories:
        sys.exit(1)

    # Extract Core Config
    email = config.get("email")
    password = config.get("password")
    domain = config.get("domain", "z-library.sk")
    should_download = config.get("download_books", True)
    output_dir = config.get("output_dir")
    download_filename = "to_download.txt"
    fetch_full = config.get("fetch_full_history", False)

    if not email or not password:
        print("❌ Error: 'email' and 'password' must be specified in config.")
        sys.exit(1)
    if should_download and not output_dir:
        print("❌ Error: 'output_dir' must be specified when 'download_books' is true.")
        sys.exit(1)

    # Initialize Connections
    print("🔌 Initializing Couchbase connection...")
    cluster, collection = cbconnect.connect_db()
    if not cluster or not collection:
        print("❌ Fatal Error: Could not connect to Couchbase.")
        sys.exit(1)
    
    # Ensure DB connection is closed on exit
    db_closed = False
    def cleanup_db():
        nonlocal db_closed
        if not db_closed:
            print("\n🔌 Closing Couchbase connection...")
            cbconnect.close_db(cluster)
            db_closed = True

    print(f"🔑 Initializing Zlibrary for domain: {domain}...")
    z = Zlibrary(email=email, password=password, domain=domain)
    if not z.isLoggedIn():
        print("❌ Fatal Error: Failed to login to Z-Library. Check credentials.")
        cleanup_db()
        sys.exit(1)

    # Get Initial Download Count
    downloads_left_today = 0
    if should_download:
        try:
            downloads_left_today = z.getDownloadsLeft()
            print(f"✅ Successfully logged in. Downloads left today: {downloads_left_today}")
            if downloads_left_today <= 0:
                 print("⚠️ API reports 0 downloads left initially.")
        except Exception as e:
            print(f"⚠️ Could not get initial downloads left: {e}")
            print("⚠️ Proceeding, but download count messages might be inaccurate.")
    else:
        print("✅ Logged in. Download flag is false, will list/check books only.")

    # Call history fetch if requested
    if fetch_full:
        fetch_and_save_user_history(z)

    # Main Loop Variables
    total_books_processed_all_categories = 0
    total_downloads_attempted_this_run = 0
    initial_download_count_for_summary = downloads_left_today
    halt_run_due_to_limit = False

    # Initialize Dry Run Report File
    report_file_handle = None
    dry_run_report_filename = None
    dry_run_book_count = 0
    if not should_download:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dry_run_report_filename = f"dry_run_report_{timestamp}.txt"
        try:
            report_file_handle = open(dry_run_report_filename, 'a', encoding='utf-8')
            report_file_handle.write(f"[DRY RUN] Dry run started at {timestamp}. No downloads or state updates will occur.\n")
            report_file_handle.write("Scraped Book Details (ID|Hash|Title|Author):\n")
            report_file_handle.write("---\n")
            print(f"📝 [DRY RUN] Report file created: {dry_run_report_filename}")
        except IOError as e:
            print(f"❌ [DRY RUN] Error creating report file: {e}. Reporting disabled.")
            report_file_handle = None

    try:
        # Outer Loop: Categories
        for category in tqdm(categories, desc="Processing categories", unit="category"):
            cat_id = category.get("id")
            cat_slug = category.get("slug")
            cat_name = category.get("name", f"ID {cat_id}")
            scrape_enabled = category.get("scrape_enabled", False)
            max_pages = category.get("max_pages_to_scrape", 1)
            start_page = category.get("start_page_next_run", 1)

            if not scrape_enabled:
                print(f"\n⏭️ Skipping disabled category: {cat_name}")
                continue
            
            if not all([cat_id, cat_slug]):
                print(f"\n⚠️ Skipping category with missing id/slug: {category}")
                continue
            
            end_page = start_page + max_pages - 1
            print(f"\n📚 Processing Category: {cat_name} (Pages {start_page} to {end_page})")
            
            books_processed_this_category = 0
            limit_hit_for_this_category = False
            last_successfully_scraped_page = start_page - 1

            # Inner Loop: Pagination
            for current_page in range(start_page, end_page + 1):
                if limit_hit_for_this_category and should_download:
                    print(f"  ⛔ Download limit hit, skipping further pages for: {cat_name}")
                    break

                print(f"\n📄 Scraping Page {current_page}/{end_page} for Category: {cat_name}")
                
                if should_download and os.path.exists(download_filename):
                    try: os.remove(download_filename)
                    except OSError as e: print(f"⚠️ Could not remove old {download_filename}: {e}")

                scrape_result = z.search_scrape(cat_id, cat_slug, current_page, enable_file_output=should_download)
                books_found_on_page = scrape_result.get("books_found", 0)

                if not scrape_result.get("success", False):
                    print(f"  ❌ Error scraping page {current_page}: {scrape_result.get('error', 'Unknown error')}")
                    print(f"  🔄 Will retry from page {current_page} next run.")
                    break
                
                last_successfully_scraped_page = current_page

                if books_found_on_page == 0:
                    print(f"  📭 No books found on page {current_page}. Assuming end of category.")
                    break

                print(f"  ✅ Found {books_found_on_page} potential books on page {current_page}.")
                
                # Process Books from the Scraped Page
                books_to_process_page = []
                if should_download:
                    try:
                        with open(download_filename, "r", encoding="utf-8") as f:
                            for line in f:
                                parts = line.strip().split('|')
                                if len(parts) == 4:
                                    books_to_process_page.append({
                                        "id": parts[0],
                                        "hash": parts[1],
                                        "title": parts[2],
                                        "authors": parts[3]
                                    })
                                else:
                                    print(f"  ⚠️ Skipping malformed line: {line.strip()}")
                    except FileNotFoundError:
                        print(f"  ❌ {download_filename} not found after successful scrape. Skipping.")
                        continue
                    except IOError as e:
                        print(f"  ❌ Error reading {download_filename}: {e}. Skipping.")
                        continue
                
                if should_download and not books_to_process_page and books_found_on_page > 0:
                    print(f"  ⚠️ Scrape found {books_found_on_page} books, but failed to load details. Skipping.")
                    continue

                # Download/Check/DryRun Logic
                page_book_count = books_found_on_page
                
                # Determine data source based on mode
                page_book_data_iterable = []
                if should_download:
                    page_book_data_iterable = books_to_process_page
                else:
                    page_book_data_iterable = scrape_result.get("books_data", [])
                    if not page_book_data_iterable and books_found_on_page > 0:
                         print(f"    ⚠️ [DRY RUN] Scrape reported {books_found_on_page} books, but no data received.")
                         
                # Loop through books for this page
                for idx, book_data in enumerate(page_book_data_iterable):
                    book_id = book_data.get("id")
                    book_hash = book_data.get("hash")
                    title = book_data.get("title", "Unknown Title")
                    authors = book_data.get("authors", "Unknown Author")

                    print(f"    📖 ({idx+1}/{page_book_count}) Processing: {book_id} ('{title}')")

                    # Check Couchbase
                    is_already_downloaded_in_db = cbconnect.check_if_downloaded(collection, book_id)
                    if is_already_downloaded_in_db:
                        print("      ✓ Already in Couchbase. Skipping.")
                        if should_download: books_processed_this_category += 1 
                        continue

                    # Handle Dry Run Mode OR Download Mode
                    if not should_download:
                        print(f"      🔍 [DRY RUN] Found New: ID={book_id}, Hash={book_hash}, Title='{title[:60]}...', Authors='{authors[:50]}...'")
                        if report_file_handle:
                            try:
                                line = f"{book_id}|{book_hash}|{title.replace('|',' ')}|{authors.replace('|',' ')}\n"
                                report_file_handle.write(line)
                                dry_run_book_count += 1
                            except IOError as e:
                                print(f"      ❌ [DRY RUN] Error writing to report file: {e}.")
                                report_file_handle.close()
                                report_file_handle = None
                        books_processed_this_category += 1
                        continue
                    
                    # Download Mode Specific Logic
                    if halt_run_due_to_limit:
                        print("      ⛔ Download limit hit earlier. Skipping.")
                        continue

                    # Attempt Download
                    print(f"      ⬇️ Attempting download... ({downloads_left_today} left reported)")
                    try:
                        download_result = z.downloadBook({"id": book_id, "hash": book_hash})

                        if download_result:
                            # --- Successful Download Initiation ---
                            # `downloadBook` now returns (extension_string, response_object)
                            file_extension, response = download_result 
                            
                            # --- Construct Filename from Full Data (from to_download.txt) ---
                            full_title = title    # Already loaded from to_download via book_data
                            full_authors = authors # Already loaded from to_download via book_data
                            
                            # Clean the author string (replace separators with space, collapse spaces)
                            if full_authors:
                                authors_with_spaces = re.sub(r'[;|]+', ' ', full_authors)
                                clean_authors = re.sub(r'\s+', ' ', authors_with_spaces).strip()
                            else:
                                clean_authors = "Unknown Author"
                            
                            # Construct base name with spaces and hyphen separator
                            base_filename = f"{full_title} - {clean_authors}"

                            # --- Sanitize Constructed Base Filename ---
                            # 1. Replace invalid filesystem chars with a SPACE
                            invalid_chars_pattern = r'[\\/?:*"<>|]' 
                            clean_base_filename = re.sub(invalid_chars_pattern, ' ', base_filename)

                            # 2. Replace multiple consecutive spaces with a single space
                            clean_base_filename = re.sub(r'\s+', ' ', clean_base_filename).strip()

                            # --- Combine with Extension and Save ---
                            final_filename = f"{clean_base_filename}{file_extension}" # Use extension from downloadBook
                            filepath = os.path.join(output_dir, final_filename)
                            
                            # --- Save File with Progress Bar ---
                            try:
                                total_size = int(response.headers.get('content-length', 0))
                                block_size = 1024 
                                
                                progress_bar = tqdm(
                                    total=total_size, 
                                    unit='iB', 
                                    unit_scale=True,
                                    desc=f"      Downloading {final_filename[:40]}...", 
                                    leave=False 
                                )
                                
                                if not os.path.exists(output_dir):
                                    try: os.makedirs(output_dir)
                                    except OSError as e:
                                        print(f"\n      ❌ Error creating directory '{output_dir}': {e}")
                                        progress_bar.close()
                                        continue 

                                with open(filepath, "wb") as f:
                                    for data in response.iter_content(block_size):
                                        progress_bar.update(len(data))
                                        f.write(data)
                                        
                                progress_bar.close() 

                                if total_size != 0 and progress_bar.n != total_size:
                                    print(f"\n      ⚠️ WARNING: Download incomplete for {final_filename}. Expected {total_size}, got {progress_bar.n}.")
                                else:
                                    # Print the *final* filename used for saving
                                    print(f"      ✅ Downloaded: {final_filename}") 
                                
                                print(f"      📝 Marking book ID {book_id} in Couchbase...")
                                mark_success = cbconnect.mark_as_downloaded(collection, book_id, title, authors)
                                if not mark_success:
                                    print(f"      ⚠️ Failed to mark book {book_id} in Couchbase.")
                                
                                books_processed_this_category += 1
                                total_downloads_attempted_this_run += 1
                                downloads_left_today -= 1 
                                time.sleep(0.5) # Short delay after successful download

                            except IOError as e:
                                print(f"\n      ❌ Error saving file '{filepath}': {e}")
                                if progress_bar: progress_bar.close()
                            except Exception as e:
                                print(f"\n      ❌ Unexpected error during download/saving for {final_filename}: {e}")
                                import traceback
                                print(traceback.format_exc())
                                if progress_bar: progress_bar.close()
                                # Consider if this should trigger halt_run_due_to_limit = True

                        else:
                            # Failed Download
                            print(f"      ❌ Download failed for book ID {book_id}")
                            print("      ⛔ Assuming download limit reached. Stopping further attempts.")
                            halt_run_due_to_limit = True

                    except Exception as e:
                        # Unexpected Download Error
                        import traceback
                        print(f"      ❌ Unexpected error downloading book ID {book_id}: {e}")
                        print(traceback.format_exc())
                        halt_run_due_to_limit = True
                
                # End of Book Processing Loop for Page
                print(f"  ✅ Finished processing books for page {current_page}.")
                if halt_run_due_to_limit:
                    print(f"  ⛔ Halting further page processing due to download limit/error.")
                    break 
                
                if books_found_on_page > 0: time.sleep(1) 
            
            # End of Pagination Loop for Category
            print(f"✅ Finished category: {cat_name}. Processed {books_processed_this_category} new books/listings.")
            total_books_processed_all_categories += books_processed_this_category
            
            # Update start page state only if downloads enabled
            if should_download:
                category["start_page_next_run"] = last_successfully_scraped_page + 1
                print(f"    🔄 Next run for '{cat_name}' will start at page {category['start_page_next_run']}")
            else:
                print(f"    ℹ️ [DRY RUN] State for '{cat_name}' not updated.")
                
            if halt_run_due_to_limit:
                print(f"\n⛔ Halting further category processing due to download limit/error.")
                break
        
        # End of Category Loop

    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user.")
    except Exception as e:
        import traceback
        print(f"\n❌ An unexpected error occurred: {e}")
        print(traceback.format_exc())
    finally:
        # Final Cleanup & Summary
        cleanup_db()
        if should_download and categories: 
            print("\n💾 Saving updated category start pages...")
            save_json(categories, categories_file)
        elif not should_download:
            print("\nℹ️ [DRY RUN] Skipping save of category start pages.")
            
            # Finalize and close Dry Run Report
            if report_file_handle:
                try:
                    report_file_handle.write("---\n")
                    report_file_handle.write(f"Total books listed: {dry_run_book_count}\n")
                    report_file_handle.close()
                    print(f"\n✅ [DRY RUN] Finished writing report: {dry_run_report_filename}")
                except IOError as e:
                     print(f"\n❌ [DRY RUN] Error finalizing report file: {e}")
            elif dry_run_report_filename:
                 print(f"\n⚠️ [DRY RUN] Report file '{dry_run_report_filename}' could not be written to.")
            else:
                 print("\n⚠️ [DRY RUN] No book details collected as report file setup failed.")

        print("\n📊 Run Summary 📊")
        print(f"Total new books/listings processed: {total_books_processed_all_categories}")
        if should_download:
             print(f"Total download attempts made: {total_downloads_attempted_this_run}")
             print(f"Initial downloads left: {initial_download_count_for_summary}, Final count: {downloads_left_today}") 
        print("✨ End of Run ✨")

# Main Execution
if __name__ == "__main__":
    run_download_process()

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
            # books_processed_on_page is read/updated directly within the category dict
            current_page_to_scrape = category.get("next_page_to_scrape", 1)

            if not scrape_enabled:
                print(f"\n⏭️ Skipping disabled category: {cat_name}")
                continue
            
            if not all([cat_id, cat_slug]):
                print(f"\n⚠️ Skipping category with missing id/slug: {cat_name}") # Use name for clarity
                continue
            
            # Calculate the range of pages to process in THIS run
            # Start from the saved page, go up to max_pages relative to that start
            start_page_this_run = current_page_to_scrape
            end_page_this_run = start_page_this_run + max_pages - 1 
            print(f"\n📚 Processing Category: {cat_name} (Targeting Pages {start_page_this_run} to {end_page_this_run})")
            
            books_processed_this_category = 0
            limit_hit_for_this_category = False
            last_successfully_scraped_page = start_page_this_run - 1

            # Inner Loop: Pagination
            new_pages_scraped_this_run = 0
            while not halt_run_due_to_limit and new_pages_scraped_this_run < max_pages:
                current_page = category.get("next_page_to_scrape", 1)
                # Get the number processed from the last run *before* starting the page
                # books_processed_before_page_start = category.get("books_processed_on_page", 0) # No longer needed
                
                print(f"\n📄 Processing Page {current_page} for Category: {cat_name} (Target: {max_pages} new pages this run)")
                # Reset the processed count for this specific page run in memory
                category["books_processed_on_page"] = 0
                print(f"   ℹ️ Resetting in-memory processed count to 0 for page {current_page} check.")
                # if books_processed_before_page_start > 0:
                #      print(f"   ℹ️ Resuming page - {books_processed_before_page_start} books already processed.")

                # --- Scrape the current page --- 
                if should_download and os.path.exists(download_filename):
                    try: os.remove(download_filename)
                    except OSError as e: print(f"⚠️ Could not remove old {download_filename}: {e}")
                
                scrape_result = z.search_scrape(cat_id, cat_slug, current_page, enable_file_output=should_download)
                books_found_on_page = scrape_result.get("books_found", 0)

                if not scrape_result.get("success", False):
                    print(f"  ❌ Error scraping page {current_page}: {scrape_result.get('error', 'Unknown error')}")
                    print(f"  Skipping rest of category '{cat_name}' for this run.")
                    break # Break the 'while' loop for this category
                
                if books_found_on_page == 0:
                    print(f"  📭 No books found on page {current_page}. Assuming end of category '{cat_name}'.")
                    # If no books found, consider the category done for this run.
                    # Don't increment next_page_to_scrape, let it retry next time if needed.
                    break # Break the 'while' loop for this category

                print(f"  ✅ Found {books_found_on_page} potential books on page {current_page}.")

                # --- Get book data --- 
                page_book_data_iterable = []
                if should_download:
                    try:
                        with open(download_filename, "r", encoding="utf-8") as f:
                            for line in f:
                                parts = line.strip().split('|')
                                if len(parts) == 4:
                                    page_book_data_iterable.append({
                                        "id": parts[0],
                                        "hash": parts[1],
                                        "title": parts[2],
                                        "authors": parts[3]
                                    })
                                else:
                                    print(f"  ⚠️ Skipping malformed line: {line.strip()}")
                    except FileNotFoundError:
                        print(f"  ❌ {download_filename} not found after successful scrape. Skipping page.")
                        break # Skip to next category if file missing
                    except IOError as e:
                        print(f"  ❌ Error reading {download_filename}: {e}. Skipping page.")
                        break # Skip to next category on read error
                else: # Dry run uses data directly from scrape result
                     page_book_data_iterable = scrape_result.get("books_data", [])
                     if not page_book_data_iterable and books_found_on_page > 0:
                         print(f"    ⚠️ [DRY RUN] Scrape reported {books_found_on_page} books, but no data received.")
                
                page_book_count = len(page_book_data_iterable) # Use actual length of loaded data

                # List to store books found missing from Couchbase on this page
                books_to_download_this_page = []

                # --- First Pass: Check all books against Couchbase --- 
                print(f"  Checking {page_book_count} books from page {current_page} against Couchbase...")

                for idx, book_data in enumerate(page_book_data_iterable):
                    # Check skip logic using the value directly from the category dictionary
                    if idx < category.get("books_processed_on_page", 0):
                        continue

                    book_id = book_data.get("id")
                    book_hash = book_data.get("hash") # Needed for download and dry run
                    title = book_data.get("title", "Unknown Title")
                    authors = book_data.get("authors", "Unknown Author")

                    if not book_id:
                        print(f"    ⚠️ Skipping book at index {idx} due to missing ID.")
                        continue

                    # print(f"    📖 ({idx+1}/{page_book_count}) Checking: {book_id} ('{title}')") # Verbose Check

                    # Check Couchbase 
                    try:
                        is_already_downloaded_in_db = cbconnect.check_if_downloaded(collection, book_id)
                    except Exception as cb_err:
                        print(f"    ❌ Error checking Couchbase for {book_id}: {cb_err}. Assuming not downloaded.")
                        is_already_downloaded_in_db = False # Treat check error as not downloaded

                    if is_already_downloaded_in_db:
                        print(f"      ✓ Already in Couchbase: {book_id} ('{title}')")
                        # **Increment the counter in the dictionary directly**
                        category["books_processed_on_page"] = category.get("books_processed_on_page", 0) + 1
                        # We don't save state here, only after a successful download *attempted in this run*.
                        continue # Move to the next book in the check loop
                    else:
                        # Book is NOT in Couchbase
                        # Handle Dry Run or Add to Download List
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
                            books_processed_this_category += 1 # Counter for dry run summary
                            continue # Go to next book
                        else:
                            # In download mode and book is missing from CB
                            print(f"      ➕ Not in Couchbase: {book_id} ('{title}'). Will download later.")
                            # Store the original index along with the book data
                            books_to_download_this_page.append((idx, book_data))
                            # **DO NOT increment category["books_processed_on_page"] here.**
                            # It will be incremented only after successful download below.
                            # Continue checking the rest of the books on the page.

                # --- End of First Pass Check Loop --- 
                print(f"  Check complete. Found {len(books_to_download_this_page)} book(s) to download for page {current_page}.")
                # Note: category["books_processed_on_page"] now reflects books found in CB in *this check* + those skipped.

                # --- Second Pass: Attempt Downloads for Missing Books ---
                if should_download and books_to_download_this_page:
                    print(f"  Attempting downloads...")
                    # Iterate through the list of (index, book_data) tuples
                    for original_index, book_data_to_download in books_to_download_this_page:
                        # Check global limit flag BEFORE attempting download
                        if halt_run_due_to_limit:
                            print("      ⛔ Download limit hit or error occurred previously. Skipping remaining downloads for this page.")
                            break # Break download loop

                        # --- Get book details --- 
                        book_id = book_data_to_download.get("id")
                        book_hash = book_data_to_download.get("hash")
                        title = book_data_to_download.get("title", "Unknown Title")
                        authors = book_data_to_download.get("authors", "Unknown Author")
                        # current_processed_count = category.get("books_processed_on_page", 0) # No longer needed for msg

                        # Use the original index for the message
                        print(f"    📖 ({original_index + 1}/{page_book_count}) Downloading: {book_id} ('{title}')")
                        print(f"      ⬇️ ({downloads_left_today} left reported)")
                        
                        # --- Download Attempt Block --- 
                        try:
                            download_result = z.downloadBook({"id": book_id, "hash": book_hash})

                            if download_result:
                                file_extension, response = download_result
                                # Construct Filename 
                                full_title = title
                                full_authors = authors
                                if full_authors:
                                    authors_with_spaces = re.sub(r'[;|]+', ' ', full_authors)
                                    clean_authors = re.sub(r'\s+', ' ', authors_with_spaces).strip()
                                else:
                                    clean_authors = "Unknown Author"
                                base_filename = f"{full_title} - {clean_authors}"
                                invalid_chars_pattern = r'[\\/?:*"<>|]'
                                clean_base_filename = re.sub(invalid_chars_pattern, ' ', base_filename)
                                clean_base_filename = re.sub(r'\s+', ' ', clean_base_filename).strip()
                                final_filename = f"{clean_base_filename}{file_extension}"
                                filepath = os.path.join(output_dir, final_filename)

                                # Save File with Progress Bar
                                try:
                                    total_size = int(response.headers.get('content-length', 0))
                                    block_size = 1024

                                    progress_bar = tqdm(
                                        total=total_size,
                                        unit='iB',
                                        unit_scale=True,
                                        desc=f"      Saving {final_filename[:40]}...",
                                        leave=False
                                    )

                                    if not os.path.exists(output_dir):
                                        try: os.makedirs(output_dir)
                                        except OSError as e:
                                            print(f"\n      ❌ Error creating directory '{output_dir}': {e}")
                                            progress_bar.close()
                                            continue # Skip this book if dir fails

                                    with open(filepath, "wb") as f:
                                        for data in response.iter_content(block_size):
                                            progress_bar.update(len(data))
                                            f.write(data)
                                    progress_bar.close()

                                    if total_size != 0 and progress_bar.n != total_size:
                                        print(f"\n      ⚠️ WARNING: Download size mismatch for {final_filename}...")
                                    else:
                                        print(f"      ✅ Saved: {final_filename}")

                                    # Mark and Save State ONLY AFTER successful save
                                    print(f"      📝 Marking book ID {book_id} in Couchbase...")
                                    mark_success = cbconnect.mark_as_downloaded(collection, book_id, title, authors)
                                    if not mark_success:
                                        print(f"      ⚠️ Failed to mark book {book_id} in Couchbase. State may be inconsistent.")
                                        # Continue processing other downloads unless limit hit
                                    else:
                                        # --- State Update Block --- 
                                        # Increment counter *after* successful download/mark
                                        # Use the dictionary value directly
                                        category["books_processed_on_page"] = category.get("books_processed_on_page", 0) + 1
                                        # Save the updated state immediately
                                        if not save_json(categories, categories_file):
                                            print("      ❌ CRITICAL ERROR: Failed to save state after marking book! Halting.")
                                            cleanup_db()
                                            sys.exit(1)
                                        print(f"      💾 State saved. Processed count for page {current_page}: {category['books_processed_on_page']}")

                                        # Update overall counters for *this run*
                                        books_processed_this_category += 1 # Count newly downloaded book
                                        total_downloads_attempted_this_run += 1
                                        downloads_left_today -= 1
                                        # --- End State Update Block ---

                                    time.sleep(0.5) # Rate limiting

                                except IOError as e:
                                    print(f"\n      ❌ Error saving file '{filepath}': {e}")
                                    if 'progress_bar' in locals() and progress_bar: progress_bar.close()
                                    # If save fails, don't mark in CB or update state
                                except Exception as e:
                                    print(f"\n      ❌ Unexpected error during file save/progress for {final_filename}: {e}")
                                    import traceback
                                    print(traceback.format_exc())
                                    if 'progress_bar' in locals() and progress_bar: progress_bar.close()
                                    # If save fails, don't mark in CB or update state

                            else: # Download failed (likely limit hit or API error)
                                print(f"      ❌ Download failed for book ID {book_id}")
                                if downloads_left_today <= 0:
                                    print("      ⛔ Download limit likely reached. Halting subsequent downloads.")
                                else:
                                    print("      ⛔ Download attempt failed for other reason. Halting subsequent downloads for safety.")
                                halt_run_due_to_limit = True
                                # Break download loop; state saved reflects downloads *before* this failure
                                break 

                        except Exception as e: # Error *initiating* download
                            print(f"      ❌ Unexpected error initiating download for book ID {book_id}: {e}")
                            import traceback
                            print(traceback.format_exc())
                            halt_run_due_to_limit = True
                            # Break download loop; state saved reflects downloads *before* this failure
                            break 

                    # --- End of Download Loop for Missing Books ---
                    if not halt_run_due_to_limit:
                        print(f"  ✅ Finished download attempts for page {current_page}.")
                
                # --- Page Completion Logic --- 
                # This runs *after* the check loop AND the download loop (if applicable)
                
                # Determine if the page is fully processed *now* using the dictionary value
                page_fully_processed = category.get("books_processed_on_page", 0) == page_book_count

                if halt_run_due_to_limit:
                    print(f"  ⛔ Halting page {current_page} processing due to download limit/error.")
                    # State was saved after the *last successful* download. No further save needed here.
                    break # Break the WHILE loop for pages

                # --- If no halt occurred ---
                if page_fully_processed:
                    print(f"  ✅ Page {current_page} confirmed fully processed ({category['books_processed_on_page']}/{page_book_count}).")
                    new_pages_scraped_this_run += 1

                    # Update state to move to the next page
                    next_page_to_start = current_page + 1
                    category["next_page_to_scrape"] = next_page_to_start
                    category["books_processed_on_page"] = 0 # Reset for the new page
                    last_successfully_scraped_page = current_page # Track for summary msg

                    print(f"    Updating state: Next page for '{cat_name}' is {next_page_to_start}, processed count reset.")
                    if not save_json(categories, categories_file):
                        print(f"      ❌ CRITICAL ERROR: Failed to save state after completing page {current_page}! Halting.")
                        cleanup_db()
                        sys.exit(1)
                else:
                    # Page not fully processed, but limit was NOT hit. 
                    # This implies potential non-limit download errors, CB errors during marking, or file save errors.
                    # The state reflects the last successful download/mark.
                    print(f"  ⚠️ Page {current_page} not fully processed ({category['books_processed_on_page']}/{page_book_count}), but download limit not hit.")
                    print(f"     Next run will resume page {current_page} attempting remaining downloads.")
                    # State should already be saved reflecting the last successful operation.
                    # Break the page loop for this category to avoid potential infinite loops on persistent errors.
                    break # Break the WHILE loop for pages
                
                # Check if we should continue to the next page in THIS RUN
                if new_pages_scraped_this_run >= max_pages:
                    print(f"  🏁 Reached max_pages_to_scrape ({max_pages}) for '{cat_name}' this run.")
                    break # Break the WHILE loop for pages
                # Otherwise, the WHILE loop continues to the next page if page was fully processed

            # --- End of While Loop for Pages ---

            # Update category summary message (uses last_successfully_scraped_page)
            print(f"\n✅ Finished processing pages for category: {cat_name}. Processed {books_processed_this_category} new books/listings in this run.")
            total_books_processed_all_categories += books_processed_this_category
            # The next page state ('next_page_to_scrape' and 'books_processed_on_page')
            # should already be correctly set and saved within the page loop.

            # Break category loop if global halt flag is set during page processing
            if halt_run_due_to_limit:
                print(f"\n⛔ Halting further category processing due to download limit/error reached in '{cat_name}'.")
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
            print("\n💾 Performing final state save...")
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

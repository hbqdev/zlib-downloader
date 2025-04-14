from Zlibrary import Zlibrary
import os
import time
import json
import cbconnect
import sys # <<< Added for sys.exit
from datetime import datetime # <<< Added for timestamp

def load_json(file_path):
    """Loads data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{file_path}'. Check format.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred loading '{file_path}': {e}")
        return None

def save_json(data, file_path):
    """Saves data to a JSON file with indentation."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2) 
        print(f"Successfully saved updated data to '{file_path}'")
        return True
    except IOError as e:
        print(f"Error saving data to '{file_path}': {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred saving to '{file_path}': {e}")
        return False

# <<< New function to fetch and save full download history >>>
def fetch_and_save_user_history(z_instance):
    """Fetches all pages of user download history and saves raw responses."""
    print("\n--- Fetching Full User Download History (as requested) ---")
    raw_history_responses = []
    current_page = 1
    page_limit = 200 # Items per API call page
    total_ids_found = 0

    while True:
        print(f"  Fetching history page {current_page} (limit {page_limit})...")
        try:
            history_response = z_instance.getUserDownloaded(limit=page_limit, page=current_page)
            
            if not history_response:
                print(f"  API request failed or returned None for history page {current_page}. Stopping history fetch.")
                break
            
            # Store raw response as string regardless of content for archival
            raw_history_responses.append(json.dumps(history_response, indent=2)) # Store formatted JSON string

            if history_response.get('success') and history_response.get('history'):
                num_items_on_page = len(history_response['history'])
                total_ids_found += num_items_on_page # Count items on page
                print(f"    Found {num_items_on_page} items on page {current_page}. Total items so far: {total_ids_found}")
                
                # Check if we received fewer items than the limit, indicating the last page
                if num_items_on_page < page_limit:
                    print("  Reached the last page of history.")
                    break # Exit loop if last page reached
                
                current_page += 1 # Go to the next page
                time.sleep(0.5) # Small delay between history API calls
            else:
                # Handle cases where success is false or history key is missing/empty
                if not history_response.get('success'):
                    print(f"  API reported failure for history page {current_page}. Response: {history_response.get('error', 'N/A')}")
                elif not history_response.get('history'):
                     print(f"  API response for page {current_page} missing 'history' key or it's empty.")
                print("  Stopping history fetch.")
                break # Exit loop
                
        except Exception as e:
            import traceback
            print(f"  An error occurred during history fetch for page {current_page}: {e}")
            print(traceback.format_exc())
            print("  Stopping history fetch due to error.")
            break

    # --- Save Raw History Responses --- 
    if raw_history_responses:
        raw_history_filename = "raw_api_history.txt"
        print(f"\n  Saving {len(raw_history_responses)} raw API history page responses to {raw_history_filename}...")
        try:
            with open(raw_history_filename, "w", encoding="utf-8") as f_raw:
                # Write each JSON response string separated by a newline for readability
                separator = "\n\n---\n\n" 
                f_raw.write(separator.join(raw_history_responses)) 
            print(f"  Successfully saved raw API history responses.")
        except IOError as e:
            print(f"  Error saving raw API history to file {raw_history_filename}: {e}")
    else:
         print("\n  No raw history responses collected to save.")
    print("--- Finished Fetching Full User Download History ---")

def run_download_process(config_file="config.json", categories_file="categories.json"):
    """
    Main process: Loads config & categories, iterates through enabled categories
    and pages, scrapes, checks Couchbase, downloads, and marks in Couchbase.
    """
    # --- Load Configuration --- 
    print(f"Loading configuration from '{config_file}'...")
    config = load_json(config_file)
    if not config:
        sys.exit(1) # Exit if config fails to load
    
    print(f"Loading categories from '{categories_file}'...")
    categories = load_json(categories_file)
    if not categories:
        sys.exit(1) # Exit if categories fail to load

    # --- Extract Core Config --- 
    email = config.get("email")
    password = config.get("password")
    domain = config.get("domain", "z-library.sk")
    should_download = config.get("download_books", True)
    output_dir = config.get("output_dir")
    # force_scrape = config.get("force_scrape", False) # 'force_scrape' is implicitly handled by running the script
    download_filename = "to_download.txt" # File used temporarily for each page scrape
    # <<< Get the fetch_full_history flag >>>
    fetch_full = config.get("fetch_full_history", False)

    if not email or not password:
        print("Error: 'email' and 'password' must be specified in config.")
        sys.exit(1)
    if should_download and not output_dir:
        print("Error: 'output_dir' must be specified in config when 'download_books' is true.")
        sys.exit(1)

    # --- Initialize Connections --- 
    print("Initializing Couchbase connection...")
    cluster, collection = cbconnect.connect_db()
    if not cluster or not collection:
        print("Fatal Error: Could not connect to Couchbase.")
        sys.exit(1)
    
    # Ensure DB connection is closed on exit
    db_closed = False
    def cleanup_db():
        nonlocal db_closed
        if not db_closed:
            print("\nClosing Couchbase connection...")
            cbconnect.close_db(cluster)
            db_closed = True

    print(f"Initializing Zlibrary for domain: {domain}...")
    z = Zlibrary(email=email, password=password, domain=domain)
    if not z.isLoggedIn():
        print("Fatal Error: Failed to login to Z-Library. Check credentials.")
        cleanup_db()
        sys.exit(1)

    # --- Get Initial Download Count (for info only) --- 
    downloads_left_today = 0 # Initialize
    if should_download:
        try:
            downloads_left_today = z.getDownloadsLeft()
            print(f"Successfully logged in. Initial downloads reported left today: {downloads_left_today}")
            if downloads_left_today <= 0:
                 print("API reports 0 downloads left initially. Will still attempt scraping, but downloads might fail immediately.")
        except Exception as e:
            print(f"Warning: Could not get initial downloads left from profile: {e}")
            print("Proceeding, but download count messages might be inaccurate.")
    else:
        print("Logged in. Download flag is false, will list/check books only.")

    # <<< Call history fetch function if flag is set >>>
    if fetch_full:
        fetch_and_save_user_history(z) # Pass the initialized Zlibrary instance
    # <<< End of history fetch call >>>

    # --- Main Loop Variables --- 
    total_books_processed_all_categories = 0
    total_downloads_attempted_this_run = 0
    initial_download_count_for_summary = downloads_left_today # Store initial count for summary
    halt_run_due_to_limit = False # <<< Global flag to stop run

    # <<< Initialize Dry Run Report File (if applicable) >>>
    report_file_handle = None
    dry_run_report_filename = None
    dry_run_book_count = 0
    if not should_download:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dry_run_report_filename = f"dry_run_report_{timestamp}.txt"
        try:
            # Open in append mode 'a'
            report_file_handle = open(dry_run_report_filename, 'a', encoding='utf-8')
            report_file_handle.write(f"[DRY RUN] Dry run started at {timestamp}. No downloads or state updates will occur.\n")
            report_file_handle.write("Scraped Book Details (ID|Hash|Title|Author):\n")
            report_file_handle.write("---\n")
            print(f"[DRY RUN] Report file created: {dry_run_report_filename}")
        except IOError as e:
            print(f"[DRY RUN] Error creating report file '{dry_run_report_filename}': {e}. Reporting disabled.")
            report_file_handle = None # Ensure handle is None if creation failed

    try:
        # --- Outer Loop: Categories --- 
        for category in categories:
            cat_id = category.get("id")
            cat_slug = category.get("slug")
            cat_name = category.get("name", f"ID {cat_id}")
            scrape_enabled = category.get("scrape_enabled", False)
            max_pages = category.get("max_pages_to_scrape", 1) # Default to 1 page if not specified
            start_page = category.get("start_page_next_run", 1)

            if not scrape_enabled:
                print(f"\n--- Skipping disabled category: {cat_name} ---")
                continue
            
            if not all([cat_id, cat_slug]):
                print(f"\n--- Skipping category with missing id/slug: {category} ---")
                continue
            
            end_page = start_page + max_pages - 1
            print(f"\n=== Processing Category: {cat_name} (Pages {start_page} to {end_page}) ===")
            
            books_processed_this_category = 0
            limit_hit_for_this_category = False # Flag to stop downloads if limit hit
            last_successfully_scraped_page = start_page - 1

            # --- Inner Loop: Pagination --- 
            for current_page in range(start_page, end_page + 1):
                if limit_hit_for_this_category and should_download: # Only stop if downloads active & limit hit
                    print(f"  Download limit previously hit, skipping further pages for category: {cat_name}")
                    break # Stop processing pages for this category

                print(f"\n-- Scraping Page {current_page}/{end_page} for Category: {cat_name} --")
                
                # Delete previous page's temp file if it exists and downloads are enabled
                if should_download and os.path.exists(download_filename):
                    try: os.remove(download_filename)
                    except OSError as e: print(f"Warning: Could not remove old {download_filename}: {e}")

                scrape_result = z.search_scrape(cat_id, cat_slug, current_page, enable_file_output=should_download)
                books_found_on_page = scrape_result.get("books_found", 0)

                if not scrape_result.get("success", False):
                    print(f"  Error scraping page {current_page} for {cat_name}: {scrape_result.get('error', 'Unknown error')}")
                    print(f"  Stopping processing for category '{cat_name}' due to scrape error. Will retry from page {current_page} next run.")
                    break # Break inner loop on error, state not updated
                
                last_successfully_scraped_page = current_page # Update on successful scrape

                if books_found_on_page == 0:
                    print(f"  No books found on page {current_page}. Assuming end of category {cat_name}.")
                    break # Stop pagination for this category

                print(f"  Scrape successful. Found {books_found_on_page} potential books on page {current_page}. Processing downloads...")
                
                # --- Process Books from the Scraped Page --- 
                books_to_process_page = []
                if should_download: # Read from file only if downloads enabled (it was written)
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
                                    print(f"  Warning: Skipping malformed line in {download_filename}: {line.strip()}")
                    except FileNotFoundError:
                        print(f"  Error: {download_filename} not found after successful scrape reported for page {current_page}? Skipping download processing.")
                        continue # To next page
                    except IOError as e:
                        print(f"  Error reading {download_filename} for page {current_page}: {e}. Skipping download processing.")
                        continue # To next page
                
                if should_download and not books_to_process_page and books_found_on_page > 0:
                    # Check if file read failed but scrape *did* find books
                    print(f"  Warning: Scrape found {books_found_on_page} books, but failed to load details from {download_filename}. Skipping page processing.")
                    continue # To next page

                # --- Download/Check/DryRun Logic --- 
                page_book_count = books_found_on_page # Use count from scrape result
                
                # Determine data source based on mode
                page_book_data_iterable = []
                if should_download:
                    page_book_data_iterable = books_to_process_page # Use data read from file
                else:
                    page_book_data_iterable = scrape_result.get("books_data", []) # Use data from scrape result
                    if not page_book_data_iterable and books_found_on_page > 0:
                         print(f"    [DRY RUN] Warning: Scrape reported {books_found_on_page} books, but no data received for processing/report.")
                         
                # Loop through books for this page
                for idx, book_data in enumerate(page_book_data_iterable):
                    book_id = book_data.get("id")
                    book_hash = book_data.get("hash")
                    title = book_data.get("title", "Unknown Title")
                    authors = book_data.get("authors", "Unknown Author")

                    # --- Processing Logic --- 
                    print(f"    ({idx+1}/{page_book_count}) Processing Book ID: {book_id} ('{title[:50]}...')")

                    # 1. Check Couchbase (Always do this)
                    is_already_downloaded_in_db = cbconnect.check_if_downloaded(collection, book_id)
                    if is_already_downloaded_in_db:
                        print("      Already in Couchbase. Skipping.")
                        # Count as processed only if we didn't already count it in dry run collection
                        if should_download: books_processed_this_category += 1 
                        continue # To next book

                    # 2. Handle Dry Run Mode OR Download Mode 
                    if not should_download:
                        # Dry Run: Just print and write to report
                        print(f"      [DRY RUN] Found New: ID={book_id}, Hash={book_hash}, Title='{title[:60]}...', Authors='{authors[:50]}...'")
                        if report_file_handle:
                            try:
                                line = f"{book_id}|{book_hash}|{title.replace('|',' ')}|{authors.replace('|',' ')}\n"
                                report_file_handle.write(line)
                                dry_run_book_count += 1
                            except IOError as e:
                                print(f"      [DRY RUN] Error writing to report file: {e}. Further reporting disabled.")
                                report_file_handle.close()
                                report_file_handle = None
                        books_processed_this_category += 1 # Count as processed in dry run
                        continue # To next book in dry run
                    
                    # --- Download Mode Specific Logic --- 
                    # 3. Check if limit was already hit GLOBALLY
                    if halt_run_due_to_limit:
                        print("      Download limit hit earlier in run. Skipping download attempt.")
                        continue # To next book

                    # 4. Attempt Download (if not skipped by checks above)
                    print(f"      Attempting download... ({downloads_left_today} left reported initially)")
                    try:
                        download_result = z.downloadBook({"id": book_id, "hash": book_hash})

                        if download_result:
                            # --- Successful Download --- 
                            filename, content = download_result
                            clean_filename = "".join(c if c.isalnum() or c in [' ', '.', '-', '_'] else '_' for c in filename)
                            
                            if not os.path.exists(output_dir):
                                try:
                                    os.makedirs(output_dir) 
                                except OSError as e: 
                                    print(f"      Error creating output directory '{output_dir}': {e}. Cannot save file.")
                                    continue # Skip saving/marking this book
                                     
                            filepath = os.path.join(output_dir, clean_filename)
                            
                            try:
                                with open(filepath, "wb") as f:
                                    f.write(content)
                                print(f"      Downloaded: {filepath}")
                                
                                print(f"      Marking book ID {book_id} as downloaded in Couchbase...")
                                mark_success = cbconnect.mark_as_downloaded(collection, book_id, title, authors)
                                if not mark_success:
                                    print(f"      Warning: Failed to mark book {book_id} in Couchbase.")
                                
                                books_processed_this_category += 1
                                total_downloads_attempted_this_run += 1
                                downloads_left_today -= 1 

                                time.sleep(2) # Delay
                                
                            except IOError as e:
                                print(f"      Error saving file '{filepath}': {e}")
                                # Not counting as processed if save fails

                        else:
                            # --- Failed Download --- 
                            print(f"      Download failed for book ID {book_id} (API returned None or error).")
                            print("      Assuming download limit reached or Z-Library API error. Stopping further download attempts for this run.")
                            halt_run_due_to_limit = True # <<< Set global stop flag
                            # Continue processing remaining books on this page (will skip download attempts)

                    except Exception as e:
                        # --- Unexpected Download Error --- 
                        import traceback
                        print(f"      Unexpected error downloading book ID {book_id}: {e}")
                        print(traceback.format_exc())
                        # Optionally set limit flags here too?
                        # page_limit_hit = True 
                        halt_run_due_to_limit = True # <<< Set global stop on unexpected error too?
                        # Continue processing remaining books on this page
                
                # --- End of Book Processing Loop for Page --- 
                print(f"  Finished processing books for page {current_page}.")
                # Break pagination loop *after* processing page if global halt flag is set
                if halt_run_due_to_limit:
                    print(f"  Halting further page processing due to download limit/error encountered on page {current_page}.")
                    break 
                
                # Delay between page scrapes if no halt occurred and books were found
                if books_found_on_page > 0: time.sleep(1) 
            
            # --- End of Pagination Loop for Category --- 
            print(f"=== Finished category: {cat_name}. Processed {books_processed_this_category} new books/listings in this category run. ===")
            total_books_processed_all_categories += books_processed_this_category
            
            # Update start page state only if downloads enabled
            if should_download:
                category["start_page_next_run"] = last_successfully_scraped_page + 1
                print(f"    Next run for '{cat_name}' will start at page {category['start_page_next_run']} (State updated)")
            else:
                print(f"    [DRY RUN] State for '{cat_name}' not updated.")
                
            # <<< Break category loop if global halt flag is set >>>
            if halt_run_due_to_limit:
                print(f"\nHalting further category processing due to download limit/error.")
                break
        
        # --- End of Category Loop --- 

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        import traceback
        print(f"\nAn unexpected error occurred during the main process: {e}")
        print(traceback.format_exc())
    finally:
        # --- Final Cleanup & Summary --- 
        cleanup_db()
        # <<< Only save the updated category data back IF downloads are enabled >>>
        if should_download and categories: 
            print("\nSaving updated category start pages...")
            save_json(categories, categories_file)
        elif not should_download:
            print("\n[DRY RUN] Skipping save of category start pages.")
            
            # <<< Finalize and close Dry Run Report >>>
            if report_file_handle:
                try:
                    report_file_handle.write("---\n")
                    report_file_handle.write(f"Total books listed: {dry_run_book_count}\n")
                    report_file_handle.close()
                    print(f"\n[DRY RUN] Finished writing report file: {dry_run_report_filename}")
                except IOError as e:
                     print(f"\n[DRY RUN] Error finalizing report file: {e}")
            elif dry_run_report_filename: # File creation failed but name exists
                 print(f"\n[DRY RUN] Report file '{dry_run_report_filename}' could not be written to.")
            else: # File creation wasn't even attempted
                 print("\n[DRY RUN] No book details collected as report file setup failed.")

        print("\n--- Run Summary ---")
        print(f"Total new books/listings processed across all categories: {total_books_processed_all_categories}")
        if should_download:
             print(f"Total download attempts made in this run: {total_downloads_attempted_this_run}")
             # Note: Final downloads_left_today is just informational based on initial check
             print(f"Initial downloads reported left by API: {initial_download_count_for_summary}, Local counter ended at: {downloads_left_today}") 
        print("--- End of Run ---")

# --- Main Execution --- 
if __name__ == "__main__":
    # Keep old function for reference maybe?
    # config = load_config() 
    # if config:
    #     download_books_by_category(config)
    
    # Call the new main process function
    run_download_process()


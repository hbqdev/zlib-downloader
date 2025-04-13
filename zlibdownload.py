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
                # In dry run, we'll handle data later
                
                if should_download and not books_to_process_page:
                    print(f"  No valid book data loaded from {download_filename} for page {current_page} despite scrape success? Skipping.")
                    continue # To next page

                # <<< Collect data for dry run report >>>
                if not should_download and scrape_result.get("success"): # Only collect if scrape ok
                    page_books_data = scrape_result.get("books_data", [])
                    if page_books_data:
                        # <<< Write incrementally to report file >>>
                        if report_file_handle:
                            page_books_written = 0
                            try:
                                for book in page_books_data:
                                    # <<< Also print book details to console in dry run >>>
                                    book_id = book.get('id','N/A')
                                    book_hash = book.get('hash','N/A')
                                    title = book.get('title','N/A')
                                    authors = book.get('authors','N/A')
                                    print(f"    [DRY RUN] Found: ID={book_id}, Hash={book_hash}, Title='{title[:60]}...', Authors='{authors[:50]}...'")
                                    
                                    # Write to file
                                    line = f"{book_id}|{book_hash}|{title.replace('|',' ')}|{authors.replace('|',' ')}\n"
                                    report_file_handle.write(line)
                                    page_books_written += 1
                                dry_run_book_count += page_books_written # Update total count
                                # Message now redundant as details are printed above
                                # print(f"    [DRY RUN] Wrote {page_books_written} book details to report.") 
                            except IOError as e:
                                print(f"    [DRY RUN] Error writing to report file: {e}. Further reporting disabled.")
                                report_file_handle.close() # Close on error
                                report_file_handle = None 
                        else:
                             print(f"    [DRY RUN] Report file handle not available, cannot write details.")
                            
                        # No longer extend in-memory list: dry_run_results.extend(page_books_data)
                        books_processed_this_category += len(page_books_data) # Still count books found
                else: # Dry Run logic 
                    # <<< Dry run logic using scrape_result['books_data'] >>>
                    page_books_data = scrape_result.get("books_data", [])
                    if page_books_data:
                        dry_run_book_count += len(page_books_data) # Count books found
                        books_processed_this_category += len(page_books_data) # Count books found
                    else:
                         if books_found_on_page > 0:
                              # Log if scrape said books found but data is missing
                              print(f"    [DRY RUN] Scrape reported {books_found_on_page} books found, but no data received for report.")
                         else: # books_found_on_page was 0
                              print(f"    [DRY RUN] Processed 0 books found on page {current_page}.")
                    
                # --- End of Download/Check/DryRun Logic for Page --- 
                print(f"  Finished processing books for page {current_page}.")
                if books_found_on_page > 0: time.sleep(1) # Delay between page scrapes if books were found
            
            # --- End of Pagination Loop for Category --- 
            print(f"=== Finished category: {cat_name}. Processed {books_processed_this_category} new books/listings in this category. ===")
            total_books_processed_all_categories += books_processed_this_category
            
            # <<< Only update the start page for the next run IF downloads are enabled >>>
            if should_download:
                category["start_page_next_run"] = last_successfully_scraped_page + 1
                print(f"    Next run for '{cat_name}' will start at page {category['start_page_next_run']} (State updated)")
            else:
                print(f"    [DRY RUN] State for '{cat_name}' not updated.")
        
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


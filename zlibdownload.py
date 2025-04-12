from Zlibrary import Zlibrary
import os
import time
import json
import cbconnect # <<< Import the new Couchbase module

def load_config(config_path="config.json"):
    """Loads configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        exit()
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Check the file format.")
        exit()

def save_config(config_data, config_path="config.json"):
    """Saves the configuration data back to the JSON file."""
    try:
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2) # Use indent for readability
        print(f"Configuration updated in '{config_path}'")
    except IOError as e:
        print(f"Error saving configuration to '{config_path}': {e}")

def download_books_by_category(config):
    """
    Downloads books from a specific Z-Library category based on config,
    checks Couchbase to avoid redownloading, and marks downloads in Couchbase.
    Potentially skips the scrape step if to_download.txt exists and
    force_scrape is false.

    Parameters:
    - config: A dictionary containing the configuration loaded from JSON
    """
    # --- Establish Couchbase Connection --- 
    print("Initializing Couchbase connection...")
    cluster, collection = cbconnect.connect_db() # <<< Connect to Couchbase
    if not cluster or not collection:
        print("Fatal Error: Could not connect to Couchbase. Please check settings and environment variables.")
        return # <<< Exit if DB connection fails
    
    # Ensure DB connection is closed on exit
    db_closed = False
    def cleanup_db():
        nonlocal db_closed
        if not db_closed:
            cbconnect.close_db(cluster)
            db_closed = True

    # Extract parameters from config
    email = config.get("email")
    password = config.get("password")
    category = config.get("category")
    limit = config.get("limit", 10) # Default limit if not specified
    output_dir = config.get("output_dir", "/mnt/g/MediaServer/EbooksDownload") # Default dir
    category_id = config.get("category_id") # Get category ID
    category_slug = config.get("category_slug") # Get category slug
    filters = config.get("filters", {}) # Default empty filters
    domain = config.get("domain", "z-library.sk") # Get domain, default if missing
    should_download = config.get("download_books", True) # Get download flag, default to True
    force_scrape = config.get("force_scrape", False) # Get force_scrape flag, default to False
    download_filename = "to_download.txt"

    # Validate essential config parameters
    if not email or not password or not category_id or not category_slug:
        print("Error: 'email', 'password', 'category_id', and 'category_slug' must be specified in the config file.")
        return

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Login to Z-Library
    print(f"Initializing Zlibrary for domain: {domain}...")
    z = Zlibrary(email=email, password=password, domain=domain)
    
    if not z.isLoggedIn():
        print("Failed to login. Please check your credentials.")
        return
    
    # Get user profile info once for download checks if needed
    user_info = z.getProfile() if should_download else None
    if should_download and (not user_info or not user_info['success']):
        print("Failed to get user profile for download check.")
        return

    if should_download:
        downloads_left = z.getDownloadsLeft() # Use the method now
        print(f"Successfully logged in. Downloads remaining today: {downloads_left}")
        if downloads_left < 1:
            print("No downloads remaining for today. Cannot download.")
            # Allow script to continue to list books even if downloads are zero
    else:
        print("Download flag is false. Will list books only.")
        downloads_left = 0 # Not relevant if not downloading

    # --- Decide whether to scrape ---
    perform_scrape = False
    if force_scrape:
        print("Config set to force scrape. Scraping will occur.")
        perform_scrape = True
    elif not os.path.exists(download_filename):
        print(f"'{download_filename}' not found. Scraping is required.")
        perform_scrape = True
    else:
        print(f"'{download_filename}' found and force_scrape is false. Skipping scrape.")
        
    if perform_scrape:
        print(f"Step 1: Scraping category page to get book titles/authors...")
        
        # Pass the whole config to the search function
        scrape_results = z.search_scrape(config)
        
        # --- Exit if scrape failed or produced no output file ---
        if not scrape_results or not scrape_results.get('success'):
            print("Scraping/File generation failed. Exiting.")
            return
        print(f"Scraping completed. Results saved to '{download_filename}'.")

    # --- Read data from to_download.txt --- 
    books_to_process = []
    try:
        with open(download_filename, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split('|')
                # <<< Expecting 4 parts now: ID|HASH|TITLE|AUTHOR >>>
                if len(parts) == 4:
                    books_to_process.append({
                        "id": parts[0],
                        "hash": parts[1],
                        "title": parts[2],
                        "authors": parts[3]
                        # <<< Removed downloaded_flag >>>
                    })
                else:
                    print(f"Warning: Skipping malformed line in {download_filename} (expected 4 parts): {line.strip()}")
    except FileNotFoundError:
        print(f"Error: {download_filename} not found. Scraping might have failed or was skipped.")
        cleanup_db() # <<< Close DB before exit
        return
    except IOError as e:
        print(f"Error reading {download_filename}: {e}")
        cleanup_db() # <<< Close DB before exit
        return

    if not books_to_process:
        print(f"No valid book data found in {download_filename}.")
        cleanup_db() # <<< Close DB before exit
        return

    count = len(books_to_process)

    print(f"Read data for {count} books from {download_filename}. Processing... Checks will be done against Couchbase.")
    
    # --- Download/List Loop --- 
    processed_count = 0
    downloads_attempted_today = 0
    try:
        for i, book_data in enumerate(books_to_process): # Iterate through data read from file
            book_id = book_data.get("id")
            book_hash = book_data.get("hash")
            title = book_data.get("title", "Unknown Title")
            authors = book_data.get("authors", "Unknown Author")

            # Print info read from file
            print(f"({i+1}/{count}) Processing: ID={book_id}, Hash={book_hash}, Title='{title}', Authors='{authors}'")

            # --- Check Couchbase instead of file flag --- 
            print("  Checking Couchbase...")
            is_already_downloaded_in_db = cbconnect.check_if_downloaded(collection, book_id)
            if is_already_downloaded_in_db:
                print("  Already marked as downloaded in Couchbase. Skipping.")
                processed_count += 1 # Still count as processed
                continue # Move to the next book
            else:
                print("  Not found in Couchbase. Proceeding...")

            if should_download:
                # --- Download limit check (REMOVED strict check here) --- 
                # We will now attempt the download and break only if it fails, 
                # assuming failure means the limit was hit or another API error occurred.
                # The initial check before the loop handles cases where the API reports 0 upfront.
                # if downloads_left <= 0: 
                #     print("  Download limit reached for today. Cannot download further.")
                #     break # Stop processing more books for download today

                try:
                    # <<< Still log the expected count based on initial fetch and local decrement >>>
                    print(f"  Attempting download... ({downloads_left} left based on initial check)")
                    # Pass ID/Hash read from file
                    download_result = z.downloadBook({"id": book_id, "hash": book_hash})

                    if download_result: # Check if download was successful
                        filename, content = download_result

                        # Clean filename (remains the same)
                        clean_filename = "".join(c if c.isalnum() or c in [' ', '.', '-', '_'] else '_' for c in filename)

                        # Save the book (remains the same)
                        filepath = os.path.join(output_dir, clean_filename)
                        with open(filepath, "wb") as f:
                            f.write(content)
                        print(f"  Downloaded: {filepath}")

                        # --- Mark as downloaded in Couchbase --- 
                        print(f"  Marking book ID {book_id} as downloaded in Couchbase...")
                        mark_success = cbconnect.mark_as_downloaded(collection, book_id, title, authors)
                        if not mark_success:
                            print(f"  Warning: Failed to mark book {book_id} as downloaded in Couchbase.")
                        # --- --- 

                        processed_count += 1
                        downloads_left -= 1 # Decrement remaining downloads for today
                        downloads_attempted_today += 1

                        # Delay (remains the same)
                        time.sleep(2)
                    else:
                        # <<< If download failed, assume limit hit or API error, and break >>>
                        print(f"  Download failed for book ID {book_id} (API returned None or error).")
                        print("  Assuming download limit reached or Z-Library API error. Stopping download attempts for this run.")
                        break # Exit the download loop for this run

                except Exception as e:
                    import traceback
                    print(f"  Error downloading book ID {book_id}: {e}")
                    print(traceback.format_exc()) # Print traceback for debugging
            else:
                # Just listing (remains the same)
                print("  Listing only (download_books is false).")
                processed_count += 1 # Count as processed (listed)
    
    finally:
        # --- Ensure DB connection is closed --- 
        cleanup_db()
        print("DB cleanup performed.")

    # --- Final Summary (remains the same) --- 
    final_action_verb = "processed"
    if should_download:
        final_action_verb = f"attempted downloads for {downloads_attempted_today}"

    print(f"\nProcessing complete! {final_action_verb.capitalize()} {processed_count} items based on data in {download_filename}.")
    if should_download:
        print(f"Downloads remaining today after this run: {downloads_left}")

    # No longer saving config as start_page is removed

# Example usage
if __name__ == "__main__":
    config_file_path = "config.json"
    config = load_config(config_file_path) # Load config from specified path
    if config:
        # Pass the whole config dictionary
        download_books_by_category(config)


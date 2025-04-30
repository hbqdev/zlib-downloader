#!/usr/bin/env python3

import json
import requests
import re
import argparse
import sys
import os
from urllib.parse import unquote_plus # Needed for decoding search terms

# --- JSON Helper Functions (similar to zlibdownload.py) ---
def load_json(file_path):
    """Loads data from a JSON file."""
    if not os.path.exists(file_path):
        print(f"ℹ️ File '{file_path}' not found. Starting with an empty list.")
        return [] # Return empty list if file doesn't exist
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Basic validation: Ensure it's a list
        if not isinstance(data, list):
             print(f"❌ Error: Expected a JSON list in '{file_path}', found {type(data)}.")
             return None
        return data
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
            json.dump(data, f, indent=2, ensure_ascii=False) # Use ensure_ascii=False for broader char support
        # print(f"✅ Successfully saved updated data to '{file_path}'") # Keep save message minimal
        return True
    except IOError as e:
        print(f"❌ Error saving data to '{file_path}': {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred saving to '{file_path}': {e}")
        return False

# --- Main Logic ---
def extract_url_info(url):
    """Extracts relevant info (ID/slug or search term) from a Z-Library URL."""
    # Try category format first
    category_match = re.search(r'/category/(\d+)/([^/?]+)', url) # Stop slug at / or ?
    if category_match:
        cat_id = int(category_match.group(1))
        cat_slug = category_match.group(2)
        return {"type": "category", "id": cat_id, "slug": cat_slug}

    # Try search format
    search_match = re.search(r'/s/([^/?]+)', url) # Stop search term at / or ?
    if search_match:
        encoded_term = search_match.group(1)
        try:
            decoded_term = unquote_plus(encoded_term)
            return {"type": "search", "search_term": decoded_term}
        except Exception as e:
            print(f"⚠️ Error decoding search term '{encoded_term}': {e}")
            return None # Indicate error

    # If neither matches
    return None

def fetch_category_name(url, slug):
    """Fetches the category page and extracts the full name from the H2 tag."""
    print(f"Fetching category name from {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        
        # Search for the main category heading (assuming it's in an H2 tag)
        name_match = re.search(r'<h2[^>]*>(.*?)</h2>', response.text, re.IGNORECASE | re.DOTALL)
        if name_match:
            # Clean up the extracted name (remove extra whitespace, potential HTML tags inside)
            raw_name = name_match.group(1).strip()
            clean_name = re.sub(r'<[^>]+>', '', raw_name).strip() # Remove inner tags if any
            print(f"Found category name: '{clean_name}'")
            return clean_name
        else:
            print(f"⚠️ Could not find category name in H2 tag. Using slug '{slug}' as fallback.")
            return slug # Fallback to slug if H2 not found/matched
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching URL {url}: {e}")
        print(f"Using slug '{slug}' as fallback name.")
        return slug
    except Exception as e:
        print(f"❌ Unexpected error processing URL {url}: {e}")
        print(f"Using slug '{slug}' as fallback name.")
        return slug

def main():
    parser = argparse.ArgumentParser(description="Add a new category or search term to categories.json from a Z-Library URL.")
    parser.add_argument("url", help="The full URL of the Z-Library category page OR a search results page (e.g., 'https://z-library.sk/category/52/Techniques/s/?...' or 'https://z-library.sk/s/your%20search/?...')")

    args = parser.parse_args()
    target_url = args.url
    categories_file = "categories.json"

    # 1. Extract info based on URL type
    url_info = extract_url_info(target_url)

    if url_info is None:
        print(f"❌ Error: Could not extract category OR search term from URL: {target_url}")
        print("  Expected format like: .../category/ID/SLUG/... or .../s/SEARCH_TERM/...")
        sys.exit(1)

    entry_type = url_info["type"]
    cat_id = url_info.get("id")
    cat_slug = url_info.get("slug")
    search_term = url_info.get("search_term")

    # 2. Determine Name
    entry_name = None
    if entry_type == "category":
        print(f"Extracted Category ID: {cat_id}, Slug: {cat_slug}")
        entry_name = fetch_category_name(target_url, cat_slug)
    elif entry_type == "search":
        print(f"Extracted Search Term: '{search_term}'")
        entry_name = f"Search: {search_term}" # Use search term directly for the name

    # 3. Load existing categories
    categories = load_json(categories_file)
    if categories is None: # Indicates a loading error
        sys.exit(1)

    # 4. Check for duplicates & Calculate next order
    max_order = 0
    duplicate_found = False
    for idx, existing_entry in enumerate(categories):
        # Check existing order, default to 0 if missing/invalid
        try:
            current_order = int(existing_entry.get('order_to_download', 0))
            if current_order > max_order:
                max_order = current_order
        except (ValueError, TypeError):
             print(f"⚠️ Warning: Invalid 'order_to_download' found at index {idx}. Treating as 0 for max order calculation.")

        # Check for duplicates
        if entry_type == "category" and existing_entry.get("id") == cat_id:
            print(f"ℹ️ Category ID {cat_id} ('{existing_entry.get('name', cat_slug)}') already exists. No changes made.")
            duplicate_found = True
            break
        elif entry_type == "search" and existing_entry.get("search_term") == search_term:
            print(f"ℹ️ Search term '{search_term}' ('{existing_entry.get('name', entry_name)}') already exists. No changes made.")
            duplicate_found = True
            break

    if duplicate_found:
        sys.exit(0)

    next_order = max_order + 1

    # 5. Create new entry
    new_entry = {
        "name": entry_name,
        "scrape_enabled": False,       # Default: disabled
        "max_pages_to_scrape": 10,     # Default: 10 pages
        "next_page_to_scrape": 1,      # Default: start at page 1
        "books_processed_on_page": 0,  # Default: 0 processed
        "order_to_download": next_order
    }

    # Add type-specific fields
    if entry_type == "category":
        new_entry["id"] = cat_id
        new_entry["slug"] = cat_slug
    elif entry_type == "search":
        new_entry["search_term"] = search_term

    print(f"\nAdding new {entry_type} entry (Order: {next_order}):")
    print(json.dumps(new_entry, indent=2))

    # 6. Append and Save
    categories.append(new_entry)
    if save_json(categories, categories_file):
        print(f"✅ Successfully added '{entry_name}' to {categories_file}.")
    else:
        print(f"❌ Failed to save updated categories to {categories_file}.")
        sys.exit(1)

if __name__ == "__main__":
    main() 
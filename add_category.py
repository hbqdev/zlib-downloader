#!/usr/bin/env python3

import json
import requests
import re
import argparse
import sys
import os

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
def extract_category_info(url):
    """Extracts ID and slug from the Z-Library category URL."""
    match = re.search(r'/category/(\d+)/([^/]+)', url)
    if match:
        cat_id = int(match.group(1))
        cat_slug = match.group(2)
        return cat_id, cat_slug
    else:
        return None, None

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
    parser = argparse.ArgumentParser(description="Add a new category to categories.json from a Z-Library URL.")
    parser.add_argument("url", help="The full URL of the Z-Library category page (e.g., 'https://z-library.sk/category/52/Techniques/s/?...')")
    
    args = parser.parse_args()
    category_url = args.url
    categories_file = "categories.json"

    # 1. Extract ID and Slug from URL
    cat_id, cat_slug = extract_category_info(category_url)
    if cat_id is None or cat_slug is None:
        print(f"❌ Error: Could not extract category ID and slug from URL: {category_url}")
        print("  Expected format like: .../category/ID/SLUG/...")
        sys.exit(1)
    print(f"Extracted ID: {cat_id}, Slug: {cat_slug}")

    # 2. Fetch Category Name
    cat_name = fetch_category_name(category_url, cat_slug)

    # 3. Load existing categories
    categories = load_json(categories_file)
    if categories is None: # Indicates a loading error
        sys.exit(1)

    # 4. Check for duplicates
    for existing_cat in categories:
        if existing_cat.get("id") == cat_id:
            print(f"ℹ️ Category ID {cat_id} ('{existing_cat.get('name', cat_slug)}') already exists in {categories_file}. No changes made.")
            sys.exit(0)

    # 5. Create new category entry
    new_category = {
        "id": cat_id,
        "slug": cat_slug,
        "name": cat_name,
        "scrape_enabled": False,       # Default: disabled
        "max_pages_to_scrape": 10,     # Default: 10 pages
        "next_page_to_scrape": 1,      # Default: start at page 1
        "books_processed_on_page": 0   # Default: 0 processed
    }
    print(f"Adding new category:")
    print(json.dumps(new_category, indent=2))

    # 6. Append and Save
    categories.append(new_category)
    if save_json(categories, categories_file):
        print(f"✅ Successfully added category ID {cat_id} to {categories_file}.")
    else:
        print(f"❌ Failed to save updated categories to {categories_file}.")
        sys.exit(1)

if __name__ == "__main__":
    main() 
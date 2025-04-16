import cbconnect
import argparse
import sys

def main():
    """Parses arguments and marks a book as downloaded in Couchbase."""
    parser = argparse.ArgumentParser(description="Manually mark a book as downloaded in Couchbase.")
    parser.add_argument("--book-id", required=True, help="The unique ID of the book.")
    parser.add_argument("--title", required=True, help="The title of the book.")
    parser.add_argument("--author", required=True, help="The author(s) of the book.")

    args = parser.parse_args()

    book_id = args.book_id
    title = args.title
    authors = args.author

    print(f"Attempting to mark book ID: {book_id} ('{title}' by {authors}) as downloaded...")

    # Connect to Couchbase
    print("Connecting to Couchbase...")
    cluster, collection = cbconnect.connect_db()

    if not cluster or not collection:
        print("❌ Fatal Error: Could not connect to Couchbase. Aborting.")
        sys.exit(1)

    # Mark as downloaded
    success = cbconnect.mark_as_downloaded(collection, book_id, title, authors)

    # Print result
    if success:
        print(f"✅ Successfully marked book ID {book_id} as downloaded in Couchbase.")
    else:
        print(f"❌ Failed to mark book ID {book_id} in Couchbase.")

    # Close connection
    print("Closing Couchbase connection...")
    cbconnect.close_db(cluster)

if __name__ == "__main__":
    main() 
import time
import os # Added for environment variables
from dotenv import load_dotenv # Added for .env file
from datetime import datetime, timezone, timedelta # Added timedelta
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.exceptions import DocumentNotFoundException, CouchbaseException

load_dotenv() # Load variables from .env file

# --- Configuration Loaded from Environment Variables --- 
# Removed hardcoded credentials

def connect_db(): # Removed config parameter
    """
    Establishes a connection to the Couchbase cluster and opens the specified bucket,
    using credentials loaded from environment variables.

    Returns:
        tuple: (cluster, collection) Couchbase cluster and default collection objects, or (None, None) on error.
    """
    # Get credentials from environment variables
    host = os.environ.get("COUCHBASE_HOST")
    username = os.environ.get("COUCHBASE_USERNAME")
    password = os.environ.get("COUCHBASE_PASSWORD")
    bucket_name = os.environ.get("COUCHBASE_BUCKET")

    # Basic validation for environment variables
    if not all([host, username, password, bucket_name]):
        print("Error: Missing one or more Couchbase environment variables:")
        print("  COUCHBASE_HOST, COUCHBASE_USERNAME, COUCHBASE_PASSWORD, COUCHBASE_BUCKET")
        return None, None

    connection_string = f"couchbase://{host}"
    print(f"Connecting to Couchbase: {connection_string}, Bucket: {bucket_name}")

    try:
        # Align with documentation: Create authenticator and options separately
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)

        # Connect using options
        cluster = Cluster(connection_string, options)

        # Wait until the cluster is ready for use.
        cluster.wait_until_ready(timedelta(seconds=10)) # Adjust timeout as needed

        # Get a reference to our bucket
        bucket = cluster.bucket(bucket_name)
        # Removed bucket wait, cluster wait is sufficient based on example
        # bucket.wait_until_ready(timeout=timedelta(seconds=5))

        print("Couchbase connection successful.")
        # Return default collection as per SDK standard practice
        return cluster, bucket.default_collection()

    except CouchbaseException as e:
        print(f"Error connecting to Couchbase: {e}")
        # Consider specific exception handling (e.g., AuthenticationFailedException)
        return None, None
    except Exception as e: # Catch other potential errors like timeouts
        print(f"An unexpected error occurred during Couchbase connection: {e}")
        return None, None


def check_if_downloaded(collection, book_id):
    """
    Checks if a document for the given book_id exists in the Couchbase collection.

    Args:
        collection: The Couchbase collection object.
        book_id (str): The ID of the book to check.

    Returns:
        bool: True if the book document exists, False otherwise.
    """
    if not collection:
        print("Error: Couchbase collection not available for check.")
        return False # Can't check if not connected

    doc_key = f"book::{book_id}"
    try:
        # Use exists() for a lightweight check without fetching the document body
        result = collection.exists(doc_key)
        # print(f"  CB Check: Key '{doc_key}' exists: {result.exists}") # Verbose logging
        return result.exists
    except CouchbaseException as e:
        print(f"  Error checking Couchbase for key '{doc_key}': {e}")
        return False # Assume not downloaded if error occurs

def mark_as_downloaded(collection, book_id, title, authors):
    """
    Creates or updates a document in Couchbase to mark a book as downloaded.

    Args:
        collection: The Couchbase collection object.
        book_id (str): The ID of the book.
        title (str): The title of the book.
        authors (str): The authors of the book.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    if not collection:
        print("Error: Couchbase collection not available for marking download.")
        return False

    doc_key = f"book::{book_id}"
    # Get current time in UTC and format as ISO 8601 string
    download_time = datetime.now(timezone.utc).isoformat()

    doc_body = {
        "title": title,
        "authors": authors,
        "downloaded_at": download_time
    }

    try:
        # Use upsert to either create the document or replace it if it exists
        result = collection.upsert(doc_key, doc_body)
        # print(f"  CB Marked: Upserted key '{doc_key}' with CAS: {result.cas}") # Verbose logging
        return True
    except CouchbaseException as e:
        print(f"  Error marking book '{doc_key}' as downloaded in Couchbase: {e}")
        return False

def close_db(cluster):
    """Closes the Couchbase cluster connection."""
    if cluster:
        try:
            cluster.close()
            print("Couchbase connection closed.")
        except Exception as e:
            print(f"Error closing Couchbase connection: {e}")

# Example usage (for testing connection)
if __name__ == "__main__":
    # This part only runs when executing cbconnect.py directly
    print("Testing Couchbase connection (using .env variables)...")
    
    # connect_db() now reads directly from environment variables loaded by load_dotenv()
    cluster, collection = connect_db()

    if cluster and collection:
        print("Testing check_if_downloaded (example book ID '12345'):")
        exists = check_if_downloaded(collection, "12345")
        print(f"Book '12345' exists: {exists}")

        print("Testing mark_as_downloaded (example book ID '12345'):")
        success = mark_as_downloaded(collection, "12345", "Test Book Title", "Test Author")
        if success:
            print("Marked '12345' as downloaded.")
            # Verify it exists now
            exists_after = check_if_downloaded(collection, "12345")
            print(f"Book '12345' exists after marking: {exists_after}")
        else:
            print("Failed to mark '12345' as downloaded.")

        close_db(cluster)
    else:
        print("Failed to connect to Couchbase for testing.") 
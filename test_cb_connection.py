import cbconnect
import os

print("--- Couchbase Connection Test ---")

# Ensure .env is loaded (cbconnect does this, but explicit check is fine)
if not all([
    os.environ.get("COUCHBASE_HOST"), 
    os.environ.get("COUCHBASE_USERNAME"),
    # We don't print the password, just check existence
    os.environ.get("COUCHBASE_PASSWORD"), 
    os.environ.get("COUCHBASE_BUCKET")
]):
    print("Error: One or more required environment variables are missing.")
    print("Please ensure COUCHBASE_HOST, COUCHBASE_USERNAME, COUCHBASE_PASSWORD, and COUCHBASE_BUCKET are set in your .env file.")
else:
    print("Environment variables seem to be present.")
    print(f"Attempting connection using:")
    print(f"  Host:   {os.environ.get('COUCHBASE_HOST')}")
    print(f"  Bucket: {os.environ.get('COUCHBASE_BUCKET')}")
    print(f"  User:   {os.environ.get('COUCHBASE_USERNAME')}")
    print("  Pass:   ******") # Don't print password

    # Attempt connection using the function from cbconnect
    cluster, collection = cbconnect.connect_db()

    if cluster and collection:
        print("\nSUCCESS: Connection established successfully!")
        
        # Optional: Try a simple check operation
        try:
            print("Attempting a lightweight check operation (exists check for 'test::connection')...")
            test_key = "test::connection"
            exists_result = collection.exists(test_key)
            print(f"Check operation successful. Key '{test_key}' exists: {exists_result.exists}")
        except Exception as e:
            print(f"Error during check operation: {e}")

        # Close the connection
        cbconnect.close_db(cluster)
    else:
        print("\nFAILED: Could not establish connection.")
        print("Please double-check:")
        print("  1. Couchbase server is running and accessible at the specified host.")
        print("  2. The bucket name is correct.")
        print("  3. The username and password in your .env file are correct.")
        print("  4. Network connectivity/firewalls between this machine and the Couchbase server.")

print("--- Test Complete ---") 
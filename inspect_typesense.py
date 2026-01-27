import json
import os
import time

import typesense
from dotenv import load_dotenv
from typesense import Client


load_dotenv()

def get_typesense_client() -> Client:
    """Returns an initialized Typesense client."""
    return typesense.Client({
        'nodes': [{
            'host': os.getenv('TYPESENSE_HOST'),
            'port': int(os.getenv('TYPESENSE_PORT')), # Must be int
            'protocol': os.getenv('TYPESENSE_PROTOCOL')
        }],
        'api_key': os.getenv('TYPESENSE_API_KEY'),
        'connection_timeout_seconds': int(os.getenv('TYPESENSE_TIMEOUT'))
    })

def wait_for_typesense(client: Client, retries: int = 12, delay: int = 5) -> bool:
    """Polls the Typesense service until it becomes healthy or retries are exhausted.

    This function attempts to connect to the Typesense instance by checking its
    health status. If the server is unreachable or returning an initialization error,
    it waits for a specified delay before retrying.

    Args:
        client: An initialized Typesense client instance.
        retries: The maximum number of connection attempts to make. Defaults to 12.
        delay: The time in seconds to wait between retry attempts. Defaults to 5.

    Returns:
        bool: True if the service returns a healthy status, False if all retries fail.

    Note:
        The connection endpoint is determined by the 'TYPESENSE_HOST' environment
        variable. This function will log status messages and errors directly to
        the console for monitoring during the startup sequence.
    """
    print(f"--- Connecting to Typesense at {os.getenv('TYPESENSE_HOST')} ---")
    for attempt in range(1, retries + 1):
        try:
            status = client.operations.is_healthy()
            if status:
                print("✅ Typesense is READY.")
                return True
        except Exception as e:
            print(f"⚠️ Attempt {attempt}: Server is up but initializing (503)...")
            print(f"🔥 Error: {e}")
            time.sleep(delay)
    return False

def inspect_typesense() -> None:
    """Retrieves and displays the schema and document previews for all Typesense collections.

    This function attempts to connect to the Typesense server, lists all available
    collections, and prints the document count for each. For non-empty collections,
    it exports and prints a JSON-formatted preview of the first five documents.

    Note:
        This function requires the Typesense server to be reachable and assumes
        `get_typesense_client()` and `wait_for_typesense()` are correctly configured.

    Raises:
        json.JSONDecodeError: If the document export from Typesense returns invalid JSON.
        Exception: Catches and prints any connection or API errors during the
            inspection process.
    """
    client = get_typesense_client()

    if not wait_for_typesense(client):
        return

    try:
        collections = client.collections.retrieve()
        if not collections:
            print("📭 No collections found. Run seed script first.")
            return

        for collection in collections:
            name = collection['name']
            count = collection['num_documents']
            print(f"\n📂 Collection: '{name}' | 📄 Total Documents: {count}")

            if count > 0:
                print("--- Previewing first 5 documents ---")
                # Use export with a limit to avoid flooding the terminal
                export_params = {'limit': 5}
                documents = client.collections[name].documents.export(export_params)

                for line in documents.strip().split('\n'):
                    if line:
                        doc_data = json.loads(line)
                        print(json.dumps(doc_data, indent=2))
            else:
                print("   (Empty collection)")

    except Exception as e:
        print(f"🔥 Error during inspection: {e}")

if __name__ == "__main__":
    inspect_typesense()

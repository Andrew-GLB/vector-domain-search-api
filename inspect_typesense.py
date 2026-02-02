import json
import os
import time
from typing import Any, cast

from dotenv import load_dotenv
from typesense.client import Client


load_dotenv()

def get_typesense_client() -> Client:
    """Returns an initialized Typesense client.
    Validates that required environment variables are present to satisfy MyPy.
    """
    host = os.getenv('TYPESENSE_HOST')
    port_str = os.getenv('TYPESENSE_PORT')
    protocol = os.getenv('TYPESENSE_PROTOCOL')
    api_key = os.getenv('TYPESENSE_API_KEY')
    timeout_str = os.getenv('TYPESENSE_TIMEOUT')

    # Explicit check to narrow types from Optional[str] to str
    if not all([host, port_str, protocol, api_key, timeout_str]):
        raise ValueError("One or more TYPESENSE environment variables are missing.")

    # These casts are now safe because of the check above
    # We cast the entire dict to Any to satisfy the strict 'ConfigDict' requirement
    return Client(cast(Any, {
        'nodes': [{
            'host': host,
            'port': int(cast(str, port_str)),
            'protocol': protocol
        }],
        'api_key': api_key,
        'connection_timeout_seconds': int(cast(str, timeout_str))
    }))

def wait_for_typesense(client: Client, retries: int = 12, delay: int = 5) -> bool:
    """Polls the Typesense service until it becomes healthy."""
    host = os.getenv('TYPESENSE_HOST') or "Unknown Host"
    print(f"--- Connecting to Typesense at {host} ---")
    
    for attempt in range(1, retries + 1):
        try:
            if client.operations.is_healthy():
                print("✅ Typesense is READY.")
                return True
        except Exception as e:
            print(f"⚠️ Attempt {attempt}: Server is initializing or unreachable...")
            print(f"🔥 Error: {e}")
            time.sleep(delay)
    return False

def inspect_typesense() -> None:
    """Retrieves and displays the schema and document previews for all collections."""
    client = get_typesense_client()

    if not wait_for_typesense(client):
        print("❌ Could not connect to Typesense. Exiting.")
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
                
                # Cast the params to Any to satisfy 'DocumentExportParameters'
                export_params = {'limit': 5}
                documents = client.collections[name].documents.export(cast(Any, export_params))

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

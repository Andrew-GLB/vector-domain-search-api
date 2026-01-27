import os

from dotenv import load_dotenv
from google import genai


load_dotenv()

def list_available_models() -> None:
    """Lists and prints all available Gemini models and their capabilities.

    This function authenticates using the GEMINI_API_KEY environment variable,
    retrieves the full list of models accessible to the client, and outputs
    their names and supported actions to the console.

    Note:
        Requires the 'GEMINI_API_KEY' environment variable to be set.
    """
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    print("--- Available Models ---")
    # This specifically calls the 'ListModels' mentioned in your error
    for model in client.models.list():
        print(f"Model Name: {model.name} - Supported Actions:   {model.supported_actions}")

if __name__ == "__main__":
    list_available_models()

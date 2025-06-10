import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from app.utils.date_utils import get_formatted_current_time

# Define valid environments
VALID_ENVIRONMENTS = ['dev', 'qa', 'prod']

def load_environment_variables():
    """
    Loads environment variables from the appropriate .env file.
    """
    # Get the environment, default to 'dev'
    current_env = os.getenv('APP_ENV', 'dev').lower()
    if current_env not in VALID_ENVIRONMENTS:
        print(f"Warning: Invalid APP_ENV '{current_env}'. Defaulting to 'dev'.")
        current_env = 'dev'

    # Construct the .env file path
    # This assumes .env files are in the parent directory of 'app'
    env_file_path = Path(__file__).resolve().parent.parent / f".env.{current_env}"

    if env_file_path.exists():
        # Load the environment variables from the file
        load_dotenv(dotenv_path=env_file_path)
        print(f"[{current_env.upper()}]-INFO: Successfully loaded configuration from {env_file_path.name}")
    else:
        print(f"[{current_env.upper()}]-WARNING: Environment file not found at {env_file_path}. Using default environment variables.")

    return current_env

def main():
    """
    The main function of the application.
    """
    current_env = load_environment_variables()
    
    # Read variables loaded from the .env file
    log_level = os.getenv("LOG_LEVEL", "NOT SET")
    api_endpoint = os.getenv("API_ENDPOINT", "NOT SET")

    if len(sys.argv) > 1:
        input_string = sys.argv[1]
        current_time = get_formatted_current_time()

        # Print all the information
        print(f"\n--- Application Output ---")
        print(f"Environment:     {current_env.upper()}")
        print(f"Timestamp:       {current_time}")
        print(f"Log Level:       {log_level}")
        print(f"API Endpoint:    {api_endpoint}")
        print(f"Message:         {input_string}")
        print(f"--------------------------")
    else:
        print("Please provide a string as a command-line argument.")
        print(f"Example: docker run simple-docker-app \"My Message\"")

if __name__ == "__main__":
    main()
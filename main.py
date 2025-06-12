import os
from pathlib import Path
from dotenv import load_dotenv
from app.utils.date_utils import get_formatted_current_time
from app.utils.db_utils import fetch_data_as_csv
from app.utils.s3_utils import create_s3_config, upload_to_s3

VALID_ENVIRONMENTS = ['dev', 'qa', 'prod']

def load_environment_variables():
    current_env = os.getenv('APP_ENV', 'dev').lower()
    if current_env not in VALID_ENVIRONMENTS:
        print(f"Warning: Invalid APP_ENV '{current_env}'. Defaulting to 'dev'.")
        current_env = 'dev'
    
    env_file_path = Path(__file__).resolve().parent.parent / f".env.{current_env}"
    if env_file_path.exists():
        load_dotenv(dotenv_path=env_file_path)
        print(f"[{current_env.upper()}]-INFO: Loaded config from {env_file_path.name}")
    else:
        print(f"[{current_env.upper()}]-WARNING: Env file not found at {env_file_path}.")
    return current_env

def main():
    """
    Main application workflow:
    1. Load environment configuration.
    2. Fetch data from PostgreSQL and save to a local CSV.
    3. Upload the CSV file to an S3 bucket.
    """
    current_env = load_environment_variables()
    print(f"\n--- Starting Data Export for [{current_env.upper()}] Environment ---")

    # 1. Configure s3cmd from environment variables
    create_s3_config()

    # 2. Define the data export query and output file
    # In a real app, this query might come from a config file or arguments
    query_to_run = "SELECT id, name, email, created_at FROM users;"
    output_dir = Path("/tmp")
    output_dir.mkdir(exist_ok=True)
    timestamp = get_formatted_current_time().replace(":", "-").replace(" ", "_")
    local_csv_path = output_dir / f"user_export_{timestamp}.csv"

    # 3. Fetch data from DB and save it locally
    if not fetch_data_as_csv(query_to_run, str(local_csv_path)):
        print("--- Data Export Failed: Could not fetch data from database. ---")
        return

    # 4. Upload the generated file to S3
    s3_filename = f"exports/{local_csv_path.name}"
    if upload_to_s3(str(local_csv_path), s3_filename):
        print("--- Data Export Completed Successfully ---")
    else:
        print("--- Data Export Failed: Could not upload file to S3. ---")

if __name__ == "__main__":
    main()
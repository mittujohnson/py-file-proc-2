import os
import subprocess
from pathlib import Path

def create_s3_config():
    """
    Creates the .s3cfg file in the home directory from environment variables.
    This is required for s3cmd to authenticate.
    """
    home_dir = Path.home()
    s3cfg_path = home_dir / ".s3cfg"
    
    config_content = f"""
[default]
access_key = {os.getenv('AWS_ACCESS_KEY_ID')}
secret_key = {os.getenv('AWS_SECRET_ACCESS_KEY')}
host_base = {os.getenv('S3_HOST')}
host_bucket = {os.getenv('S3_HOST_BUCKET')}
use_https = True
"""
    try:
        with open(s3cfg_path, "w") as f:
            f.write(config_content)
        # Set secure permissions for the config file
        os.chmod(s3cfg_path, 0o600)
        print("s3cmd configuration file created successfully.")
    except IOError as e:
        print(f"ERROR: Could not write .s3cfg file: {e}")

def upload_to_s3(local_filepath, s3_key):
    """
    Uploads a file to S3 using the s3cmd command-line tool.

    Args:
        local_filepath (str): The path of the local file to upload.
        s3_key (str): The destination key (path) in the S3 bucket.

    Returns:
        bool: True if upload is successful, False otherwise.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    
    command = ["s3cmd", "put", local_filepath, s3_uri]
    
    try:
        print(f"Uploading {local_filepath} to {s3_uri}...")
        # Using capture_output=True to get stdout/stderr
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("S3 Upload successful.")
        print(f"s3cmd output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: s3cmd upload failed with return code {e.returncode}")
        print(f"s3cmd stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("ERROR: 's3cmd' command not found. Is it installed and in the system's PATH?")
        return False
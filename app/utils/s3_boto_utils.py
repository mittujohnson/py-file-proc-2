import boto3
from botocore.exceptions import ClientError
import logging
import os

# Configure logging for better error reporting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_s3_client():
    """
    Creates and returns a boto3 S3 client.
    Assumes AWS credentials are configured (e.g., via environment variables,
    shared credentials file, or IAM role).
    """
    try:
        s3_client = boto3.client('s3')
        logging.info("S3 client created successfully.")
        return s3_client
    except ClientError as e:
        logging.error(f"Error creating S3 client: {e}")
        return None

def put_file_to_s3(bucket_name: str, file_path: str, object_name: str = None) -> bool:
    """
    Uploads a file to an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param file_path: Path to the file to upload.
    :param object_name: S3 object name (key). If not specified, file_path basename is used.
    :return: True if file was uploaded successfully, False otherwise.
    """
    s3_client = create_s3_client()
    if not s3_client:
        return False

    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False

    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"File '{file_path}' uploaded to '{bucket_name}/{object_name}' successfully.")
        return True
    except ClientError as e:
        logging.error(f"Error uploading file '{file_path}' to S3: {e}")
        return False

def get_file_from_s3(bucket_name: str, object_name: str, download_path: str) -> bool:
    """
    Downloads a file from an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param object_name: S3 object name (key) to download.
    :param download_path: Path where the file should be saved locally.
    :return: True if file was downloaded successfully, False otherwise.
    """
    s3_client = create_s3_client()
    if not s3_client:
        return False

    try:
        s3_client.download_file(bucket_name, object_name, download_path)
        logging.info(f"File '{object_name}' downloaded from '{bucket_name}' to '{download_path}' successfully.")
        return True
    except ClientError as e:
        logging.error(f"Error downloading file '{object_name}' from S3: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during download: {e}")
        return False

def list_objects_in_s3(bucket_name: str, prefix: str = "") -> list:
    """
    Lists objects in an S3 bucket with an optional prefix.

    :param bucket_name: Name of the S3 bucket.
    :param prefix: Optional prefix to filter objects.
    :return: A list of object keys (strings) if successful, an empty list otherwise.
    """
    s3_client = create_s3_client()
    if not s3_client:
        return []

    object_keys = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in response_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_keys.append(obj["Key"])
        logging.info(f"Listed {len(object_keys)} objects in '{bucket_name}' with prefix '{prefix}'.")
        return object_keys
    except ClientError as e:
        logging.error(f"Error listing objects in '{bucket_name}': {e}")
        return []

def delete_object_from_s3(bucket_name: str, object_name: str) -> bool:
    """
    Deletes an object from an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param object_name: S3 object name (key) to delete.
    :return: True if object was deleted successfully, False otherwise.
    """
    s3_client = create_s3_client()
    if not s3_client:
        return False

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        logging.info(f"Object '{object_name}' deleted from '{bucket_name}' successfully.")
        return True
    except ClientError as e:
        logging.error(f"Error deleting object '{object_name}' from S3: {e}")
        return False

if __name__ == "__main__":
    # Example Usage: Replace with your actual bucket and file names
    # You need to have AWS credentials configured for these examples to work.
    # For instance, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.

    YOUR_BUCKET_NAME = "your-unique-s3-bucket-name"
    LOCAL_FILE_TO_UPLOAD = "example.txt"
    S3_OBJECT_KEY = "my-uploaded-example.txt"
    DOWNLOADED_FILE_PATH = "downloaded_example.txt"

    # Create a dummy file for upload
    with open(LOCAL_FILE_TO_UPLOAD, "w") as f:
        f.write("This is a test file for S3 operations.\n")
        f.write("Hello, S3!")

    print(f"\n--- Attempting to upload '{LOCAL_FILE_TO_UPLOAD}' to S3 ---")
    if put_file_to_s3(YOUR_BUCKET_NAME, LOCAL_FILE_TO_UPLOAD, S3_OBJECT_KEY):
        print(f"Successfully uploaded '{LOCAL_FILE_TO_UPLOAD}'.")
    else:
        print(f"Failed to upload '{LOCAL_FILE_TO_UPLOAD}'.")

    print(f"\n--- Listing objects in '{YOUR_BUCKET_NAME}' ---")
    objects = list_objects_in_s3(YOUR_BUCKET_NAME)
    if objects:
        print("Objects in bucket:")
        for obj_key in objects:
            print(f"- {obj_key}")
    else:
        print("No objects found or failed to list objects.")

    print(f"\n--- Attempting to download '{S3_OBJECT_KEY}' from S3 ---")
    if get_file_from_s3(YOUR_BUCKET_NAME, S3_OBJECT_KEY, DOWNLOADED_FILE_PATH):
        print(f"Successfully downloaded '{S3_OBJECT_KEY}' to '{DOWNLOADED_FILE_PATH}'.")
        with open(DOWNLOADED_FILE_PATH, "r") as f:
            print("Downloaded content:")
            print(f.read())
        os.remove(DOWNLOADED_FILE_PATH) # Clean up downloaded file
    else:
        print(f"Failed to download '{S3_OBJECT_KEY}'.")

    print(f"\n--- Attempting to delete '{S3_OBJECT_KEY}' from S3 ---")
    if delete_object_from_s3(YOUR_BUCKET_NAME, S3_OBJECT_KEY):
        print(f"Successfully deleted '{S3_OBJECT_KEY}'.")
    else:
        print(f"Failed to delete '{S3_OBJECT_KEY}'.")

    # Clean up dummy local file
    if os.path.exists(LOCAL_FILE_TO_UPLOAD):
        os.remove(LOCAL_FILE_TO_UPLOAD)
        print(f"\nCleaned up local file: {LOCAL_FILE_TO_UPLOAD}")
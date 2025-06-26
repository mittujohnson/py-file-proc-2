import boto3
import requests
import os
import io

# --- Configuration ---
# Replace with your S3 bucket name
S3_BUCKET_NAME = 'your-s3-bucket-name'
# Replace with the S3 folder path where your CSV files are located (e.g., 'data/csv_files/')
S3_FOLDER_PATH = 'your/s3/folder/path/'
# Replace with your API endpoint URL
API_ENDPOINT_URL = 'http://your-api-endpoint.com/upload'
# Specify the file extension to process (e.g., '.csv')
FILE_EXTENSION = '.csv'

# --- S3 Client Initialization ---
# Ensure your AWS credentials are configured (e.g., via environment variables, ~/.aws/credentials, or IAM roles)
# If running in an AWS environment (EC2, Lambda), an IAM role is preferred.
s3_client = boto3.client('s3')

def process_s3_files():
    """
    Lists CSV files in a specified S3 folder, posts them to an API endpoint,
    and renames successful files with a '.done' suffix.
    """
    print(f"Starting file processing from S3 folder: s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}")

    try:
        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_FOLDER_PATH)

        # Check if 'Contents' key exists (means there are objects)
        if 'Contents' not in response:
            print(f"No files found in s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}")
            return

        # Filter for files with the specified extension and not already processed (.done)
        csv_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith(FILE_EXTENSION) and not obj['Key'].endswith('.done')
        ]

        if not csv_files:
            print(f"No {FILE_EXTENSION} files to process in s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}")
            return

        print(f"Found {len(csv_files)} {FILE_EXTENSION} files to process:")
        for file_key in csv_files:
            print(f"- {file_key}")

        # Process each file sequentially
        for file_key in csv_files:
            file_name = os.path.basename(file_key)
            print(f"\n--- Processing file: {file_name} (S3 Key: {file_key}) ---")

            try:
                # 1. Download the file from S3
                print(f"Downloading {file_key} from S3...")
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                file_content = obj['Body'].read()
                print("File downloaded.")

                # Prepare the file for API post (using BytesIO as it's in-memory)
                files = {'file': (file_name, file_content, 'text/csv')}

                # 2. Call the API endpoint and post the file
                print(f"Posting {file_name} to API endpoint: {API_ENDPOINT_URL}...")
                api_response = requests.post(API_ENDPOINT_URL, files=files)

                # 3. Check the API response status code
                if api_response.status_code >= 200 and api_response.status_code < 300:
                    print(f"API call successful! Status Code: {api_response.status_code}")
                    print(f"API Response: {api_response.text}")

                    # 4. Rename the file in S3 with ".done" suffix
                    new_file_key = file_key + ".done"
                    print(f"Renaming S3 object from {file_key} to {new_file_key}...")
                    s3_client.copy_object(
                        Bucket=S3_BUCKET_NAME,
                        CopySource={'Bucket': S3_BUCKET_NAME, 'Key': file_key},
                        Key=new_file_key
                    )
                    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                    print(f"File {file_name} successfully renamed to {os.path.basename(new_file_key)} in S3.")
                else:
                    print(f"API call failed! Status Code: {api_response.status_code}")
                    print(f"API Error Response: {api_response.text}")
                    print(f"File {file_name} not renamed due to API failure.")

            except requests.exceptions.RequestException as e:
                print(f"HTTP Request Error for {file_name}: {e}")
                print(f"File {file_name} not renamed due to request error.")
            except s3_client.exceptions.NoSuchKey:
                print(f"S3 Error: File {file_key} not found. It might have been moved or deleted.")
            except Exception as e:
                print(f"An unexpected error occurred while processing {file_name}: {e}")
                print(f"File {file_name} not renamed due to an error.")

    except boto3.exceptions.S3TransferError as e:
        print(f"S3 Transfer Error: {e}")
    except Exception as e:
        print(f"An error occurred during S3 listing or overall processing: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    process_s3_files()

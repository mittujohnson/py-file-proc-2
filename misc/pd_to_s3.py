import pandas as pd
import boto3
from io import StringIO

# --- Configuration ---
# Replace with your AWS S3 bucket name
S3_BUCKET_NAME = 'your-s3-bucket-name'
# Replace with the desired path and filename in S3
# e.g., 'data/my_dataframe.csv' or 'reports/sales_2024.csv'
S3_FILE_KEY = 'your-folder/your-file-name.csv'
# AWS Region (e.g., 'us-east-1', 'eu-west-2')
AWS_REGION = 'your-aws-region'

# --- Prerequisites ---
# Make sure you have the following libraries installed:
# pip install pandas boto3

# Ensure your AWS credentials are configured.
# You can do this by:
# 1. Setting environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN if temporary)
# 2. Using the AWS CLI `aws configure` command to set up ~/.aws/credentials
# 3. Passing credentials directly to boto3.client (less secure for production)

# --- 1. Create a sample Pandas DataFrame ---
# In a real scenario, you would load your data here (e.g., from a database, another CSV, etc.)
data = {
    'col1': [1, 2, 3, 4, 5],
    'col2': ['A', 'B', 'C', 'D', 'E'],
    'col3': [10.1, 20.2, 30.3, 40.4, 50.5]
}
df = pd.DataFrame(data)

print("Sample DataFrame created:")
print(df)
print("-" * 30)

# --- 2. Initialize S3 client ---
# It's good practice to specify the region.
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"S3 client initialized for region: {AWS_REGION}")
except Exception as e:
    print(f"Error initializing S3 client: {e}")
    exit() # Exit if S3 client cannot be initialized

# --- 3. Convert DataFrame to CSV string in memory ---
# Using StringIO allows us to treat a string in memory like a file,
# which is efficient as it avoids writing to a local disk first.
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False) # index=False prevents writing the DataFrame index as a column
print("DataFrame converted to CSV string in memory.")

# --- 4. Upload the CSV string to S3 ---
try:
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY, Body=csv_buffer.getvalue())
    print(f"Successfully uploaded '{S3_FILE_KEY}' to S3 bucket '{S3_BUCKET_NAME}'.")
except Exception as e:
    print(f"Error uploading file to S3: {e}")

# --- Optional: Verify the upload (by trying to read it back) ---
print("\n--- Verifying upload (optional) ---")
try:
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
    body = response['Body'].read().decode('utf-8')
    df_from_s3 = pd.read_csv(StringIO(body))
    print("File successfully read back from S3. First 5 rows:")
    print(df_from_s3.head())
except Exception as e:
    print(f"Error verifying upload: {e}")


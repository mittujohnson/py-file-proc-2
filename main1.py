import psycopg2
import csv
from datetime import datetime
import math
import os

class DatabaseManager:
    """
    Manages database connection and data retrieval from PostgreSQL.
    """
    def __init__(self, db_config):
        """
        Initializes the DatabaseManager with database configuration.

        Args:
            db_config (dict): A dictionary containing database connection details
                              (e.g., host, database, user, password, port).
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database.
        """
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            self.conn = None # Ensure conn is None if connection fails
            self.cursor = None # Ensure cursor is None if connection fails
            raise # Re-raise the exception to be caught by the calling function

    def disconnect(self):
        """
        Closes the database connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Disconnected from the database.")

    def get_total_records(self, table_name="persons"):
        """
        Fetches the total number of records in the specified table.

        Args:
            table_name (str): The name of the table to count records from.

        Returns:
            int: The total number of records, or 0 if an error occurs.
        """
        if not self.conn or not self.cursor:
            print("Database connection not established. Cannot get total records.")
            return 0
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = self.cursor.fetchone()[0]
            print(f"Total records in '{table_name}': {total_records}")
            return total_records
        except Exception as e:
            print(f"Error getting total records from table '{table_name}': {e}")
            return 0

    def fetch_paginated_records(self, table_name, limit, offset):
        """
        Fetches a subset of records from the specified table using LIMIT and OFFSET.

        Args:
            table_name (str): The name of the table to fetch data from.
            limit (int): The maximum number of records to return.
            offset (int): The number of records to skip before starting to return rows.

        Returns:
            list: A list of tuples, where each tuple represents a row.
                  Returns an empty list if no data is found or an error occurs.
            list: A list of column names (headers).
        """
        if not self.conn or not self.cursor:
            print("Database connection not established. Cannot fetch paginated data.")
            return [], []

        try:
            # Fetch column names only once, or ensure they are passed from main
            # For simplicity, we'll fetch them on the first paginated call if not already done.
            self.cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            column_names = [desc[0] for desc in self.cursor.description]

            self.cursor.execute(f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}") # ORDER BY 1 to ensure consistent ordering for pagination
            records = self.cursor.fetchall()
            print(f"Fetched {len(records)} records with LIMIT {limit} OFFSET {offset} from '{table_name}'.")
            return records, column_names
        except Exception as e:
            print(f"Error fetching paginated data from table '{table_name}': {e}")
            return [], []

class FileManager:
    """
    Manages file operations, specifically creating and writing to CSV files.
    """
    def __init__(self, output_directory="output_files"):
        """
        Initializes the FileManager with an output directory.

        Args:
            output_directory (str): The directory where CSV files will be saved.
                                    Created if it doesn't exist.
        """
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)
        print(f"Output directory '{self.output_directory}' ensured.")

    def generate_filename(self, serial_number):
        """
        Generates a filename based on the current date and a serial number.

        Args:
            serial_number (int): The serial number of the file (e.g., 1 for 01.csv).

        Returns:
            str: The generated filename (e.g., "20250626-01.csv").
        """
        rundate = datetime.now().strftime("%Y%m%d")
        # Format serial number with leading zeros, e.g., 1 -> 01, 10 -> 10
        file_serial = f"{serial_number:02d}"
        filename = f"{rundate}-{file_serial}.csv"
        return os.path.join(self.output_directory, filename)

    def write_to_csv(self, filename, headers, data):
        """
        Writes data to a CSV file.

        Args:
            filename (str): The full path to the CSV file to write.
            headers (list): A list of strings for the CSV header row.
            data (list): A list of tuples/lists, where each inner list/tuple is a row of data.
        """
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if headers:
                    csv_writer.writerow(headers) # Write header row
                csv_writer.writerows(data) # Write all data rows
            print(f"Successfully wrote {len(data)} records to '{filename}'.")
        except Exception as e:
            print(f"Error writing to CSV file '{filename}': {e}")

def main(num_files, table_name="persons"):
    """
    Main function to orchestrate the fetching and file generation process.

    Args:
        num_files (int): The number of comma-delimited files to generate.
        table_name (str): The name of the PostgreSQL table to extract data from.
    """
    if num_files <= 0:
        print("Number of files must be a positive integer.")
        return

    # --- Database Configuration ---
    # IMPORTANT: Replace these placeholders with your actual PostgreSQL credentials.
    db_config = {
        "host": "localhost",
        "database": "your_database_name",
        "user": "your_username",
        "password": "your_password",
        "port": 5432
    }

    db_manager = DatabaseManager(db_config)
    file_manager = FileManager() # Default output_files directory

    total_records = 0
    headers = [] # Will store headers once they are known

    try:
        db_manager.connect()
        total_records = db_manager.get_total_records(table_name)
        
        # Get headers by fetching 0 records initially, this sets the cursor description
        _, headers = db_manager.fetch_paginated_records(table_name, limit=0, offset=0)

    except Exception as e:
        print(f"Application terminated due to database connection/initial fetch error: {e}")
        return
    finally:
        # Disconnect is moved to the end of main to keep connection open during file iteration
        pass # Will disconnect after the loop

    if total_records == 0:
        print("No records found in the database. No files will be generated.")
        db_manager.disconnect()
        return

    # Calculate records per file, ensuring all records are covered
    records_per_file = math.ceil(total_records / num_files) 

    print(f"\nTotal records: {total_records}")
    print(f"Calculated records per file: {records_per_file}")
    print(f"Attempting to generate {num_files} files...\n")

    current_offset = 0
    # Iterate based on calculated number of files, not actual data fetched
    for i in range(num_files):
        # Determine the LIMIT for the current query
        # For the last file, it might be less than records_per_file if total_records % num_files != 0
        limit = records_per_file
        
        # If the remaining records are less than 'records_per_file', adjust limit
        if current_offset + limit > total_records:
            limit = total_records - current_offset

        if limit <= 0: # No more records to process
            print(f"No more records to process after {i} files. Stopping file generation.")
            break

        print(f"Processing file {i+1} (Serial {i+1:02d}): LIMIT {limit}, OFFSET {current_offset}")

        # Fetch data for the current file
        file_data, _ = db_manager.fetch_paginated_records(table_name, limit, current_offset)

        if not file_data:
            print(f"File {i+1}: No data returned for this segment (LIMIT {limit}, OFFSET {current_offset}), skipping file generation.")
            # If no data is returned but limit > 0, it indicates an issue or end of records.
            # Increment offset to prevent infinite loop if an issue occurs.
            current_offset += limit 
            continue

        # Generate filename (serial number starts from 1)
        filename = file_manager.generate_filename(i + 1)
        file_manager.write_to_csv(filename, headers, file_data)

        current_offset += len(file_data) # Update offset by actual records fetched

    print("\nFile generation process completed.")
    db_manager.disconnect() # Disconnect after all files are generated

if __name__ == "__main__":
    # Example Usage:
    # To generate 5 files from the 'persons' table:
    main(num_files=5, table_name="persons")

    # To generate 2 files:
    # main(num_files=2, table_name="persons")

    # To generate 1 file:
    # main(num_files=1, table_name="persons")

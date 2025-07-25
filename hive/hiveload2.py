#!/usr/bin/env python3

import pandas as pd
from pyhive import hive
import csv
import os
from contextlib import contextmanager

class HiveCSVLoader:
    def __init__(self, host='localhost', port=10000, username='hive'):
        self.host = host
        self.port = port
        self.username = username
        
    @contextmanager
    def get_connection(self):
        """Create a connection context manager"""
        conn = None
        try:
            conn = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username
            )
            yield conn
        finally:
            if conn:
                conn.close()
    
    def create_table_from_csv(self, csv_file_path, table_name, database='default'):
        """
        Create a Hive table based on CSV structure
        """
        # Read CSV to infer schema
        df = pd.read_csv(csv_file_path, nrows=5)  # Read first few rows to infer schema
        
        # Generate column definitions
        column_defs = []
        for col in df.columns:
            # Simple type inference - you might want to make this more sophisticated
            sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
            
            if pd.api.types.is_integer_dtype(df[col]):
                hive_type = "INT"
            elif pd.api.types.is_float_dtype(df[col]):
                hive_type = "DOUBLE"
            elif pd.api.types.is_bool_dtype(df[col]):
                hive_type = "BOOLEAN"
            else:
                hive_type = "STRING"
                
            # Clean column name (remove spaces, special chars)
            clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_')
            column_defs.append(f"`{clean_col}` {hive_type}")
        
        # Create table DDL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} (
            {', '.join(column_defs)}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        STORED AS TEXTFILE
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create database if not exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            
            # Drop table if exists (optional)
            cursor.execute(f"DROP TABLE IF EXISTS {database}.{table_name}")
            
            # Create table
            print(f"Creating table: {database}.{table_name}")
            cursor.execute(create_table_sql)
            print(f"Table {database}.{table_name} created successfully")
            
        return column_defs
    
    def load_csv_to_table(self, csv_file_path, table_name, database='default', 
                         batch_size=1000, skip_header=True):
        """
        Load CSV data into Hive table using batch inserts
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            
            # Clean column names to match table schema
            df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') 
                         for col in df.columns]
            
            # Convert DataFrame to list of tuples
            data_rows = [tuple(row) for row in df.values]
            
            # Prepare insert statement
            columns = ', '.join([f"`{col}`" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_sql = f"INSERT INTO {database}.{table_name} ({columns}) VALUES ({placeholders})"
            
            # Batch insert
            print(f"Loading {len(data_rows)} rows into {database}.{table_name}")
            
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    print(f"Inserted batch {i//batch_size + 1}, rows {i+1}-{min(i+batch_size, len(data_rows))}")
                except Exception as e:
                    print(f"Error inserting batch {i//batch_size + 1}: {e}")
                    # Continue with next batch
                    continue
            
            print(f"Data loading completed for table {database}.{table_name}")
    
    def load_csv_via_hdfs(self, csv_file_path, table_name, database='default'):
        """
        Alternative method: Load CSV via HDFS (more efficient for large files)
        This requires the CSV file to be accessible from the Hive container
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Load data using LOAD DATA INPATH (requires file in HDFS)
            load_sql = f"""
            LOAD DATA LOCAL INPATH '{csv_file_path}'
            INTO TABLE {database}.{table_name}
            """
            
            try:
                cursor.execute(load_sql)
                print(f"Data loaded successfully into {database}.{table_name}")
            except Exception as e:
                print(f"Error loading data: {e}")
                print("Note: File must be accessible from Hive container")
    
    def query_table(self, table_name, database='default', limit=10):
        """
        Query the table to verify data loading
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {database}.{table_name}")
            count = cursor.fetchone()[0]
            print(f"Total rows in {database}.{table_name}: {count}")
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {database}.{table_name} LIMIT {limit}")
            results = cursor.fetchall()
            
            # Get column names
            cursor.execute(f"DESCRIBE {database}.{table_name}")
            columns = [row[0] for row in cursor.fetchall()]
            
            print(f"\nSample data from {database}.{table_name}:")
            print("-" * 50)
            print("\t".join(columns))
            print("-" * 50)
            for row in results:
                print("\t".join([str(val) for val in row]))


def main():
    # Example usage
    loader = HiveCSVLoader(host='localhost', port=10000, username='hive')
    
    # Configuration
    csv_file = 'sample_data.csv'  # Replace with your CSV file path
    table_name = 'my_table'
    database_name = 'my_database'
    
    try:
        # Step 1: Create table based on CSV structure
        print("Step 1: Creating table...")
        loader.create_table_from_csv(csv_file, table_name, database_name)
        
        # Step 2: Load CSV data into table
        print("\nStep 2: Loading data...")
        loader.load_csv_to_table(csv_file, table_name, database_name)
        
        # Step 3: Verify data loading
        print("\nStep 3: Verifying data...")
        loader.query_table(table_name, database_name)
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure Hive containers are running")
        print("2. Check if HiveServer2 is accessible on port 10000")
        print("3. Verify CSV file path and format")
        print("4. Install required packages: pip install pyhive pandas")


if __name__ == "__main__":
    main()
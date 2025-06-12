import os
import psycopg2
import csv

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using environment variables.

    Returns:
        psycopg2.connection: A connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect to PostgreSQL database: {e}")
        return None

def fetch_data_as_csv(query, output_filepath):
    """
    Executes a query, fetches all results, and writes them to a CSV file.

    Args:
        query (str): The SQL query to execute.
        output_filepath (str): The path to the output CSV file.

    Returns:
        bool: True if successful, False otherwise.
    """
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            print(f"Executing query: {query}")
            cursor.execute(query)
            
            headers = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            with open(output_filepath, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"Successfully wrote {len(rows)} rows to {output_filepath}")
            return True
    except (psycopg2.Error, IOError) as e:
        print(f"ERROR: Failed to fetch data or write CSV: {e}")
        return False
    finally:
        if conn:
            conn.close()
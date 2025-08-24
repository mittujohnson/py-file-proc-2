import sys
from pyhive import hive
from datetime import datetime, timedelta
from config import HIVE_HOST, HIVE_PORT, HIVE_USER
from hive_processor import get_latest_partition_date, process_single_day

def main():
    """
    Main function to orchestrate the entire pipeline with a daily loop.
    """
    conn = None
    try:
        print("Connecting to HiveServer2...")
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER)
        cursor = conn.cursor()
        print("Successfully connected to Hive.")

        latest_date = get_latest_partition_date(cursor)
        today = datetime.now().date()
        
        # Start from the day after the last processed date
        current_day_to_process = latest_date + timedelta(days=1)

        if current_day_to_process > today:
            print(f"Table is up to date. Latest partition: {latest_date}. No processing needed.")
            
        while current_day_to_process <= today:
            process_single_day(cursor, current_day_to_process)
            current_day_to_process += timedelta(days=1)
            
        print("\nProgram completed successfully!")
        
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        print("Program failed. Exiting with status 1.")
        sys.exit(1)
        
    finally:
        if conn:
            conn.close()
            print("Hive connection closed.")

if __name__ == "__main__":
    main()

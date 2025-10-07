import subprocess
import shlex
import os

def load_csv_to_hdfs_with_beeline(
    keytab_path,
    principal,
    jdbc_url,
    csv_local_path,
    hdfs_target_path
):
    """
    Executes a 'beeline' command using a keytab for authentication 
    to load a local CSV file into an HDFS location via Hive's LOAD DATA INPATH.

    Args:
        keytab_path (str): Full path to the Kerberos keytab file.
        principal (str): Kerberos principal (e.g., 'user@REALM.COM').
        jdbc_url (str): JDBC connection URL for the Hive server (e.g., 'jdbc:hive2://host:port/database').
        csv_local_path (str): Local path to the CSV file (e.g., '/tmp/data.csv').
        hdfs_target_path (str): HDFS path where the data should be loaded 
                                (e.g., '/user/hive/warehouse/my_table/data').
    """
    # 1. Kerberos Authentication (kinit)
    # The 'kinit' command uses the keytab to obtain a Kerberos ticket.
    print("--- 1. Running kinit for Kerberos authentication ---")
    kinit_command = f"kinit -kt {keytab_path} {principal}"
    
    try:
        # shlex.split helps safely split the command string into a list of arguments
        kinit_result = subprocess.run(shlex.split(kinit_command), check=True, capture_output=True, text=True)
        print("kinit successful.")
    except subprocess.CalledProcessError as e:
        print(f"kinit failed with error code {e.returncode}.")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return  # Stop execution if kinit fails
    except FileNotFoundError:
        print("Error: 'kinit' command not found. Ensure Kerberos tools are installed and in PATH.")
        return

    # 2. Beeline Command Construction
    # The 'beeline' command connects and executes the Hive SQL query.
    hive_sql_query = f"LOAD DATA LOCAL INPATH '{csv_local_path}' INTO TABLE my_table LOCATION '{hdfs_target_path}';"
    
    # NOTE: Kerberos authentication via 'kinit' is sufficient; 'beeline' 
    # will use the ticket-granting ticket (TGT) already established.
    beeline_command = f"beeline -u '{jdbc_url}' -e \"{hive_sql_query}\""

    print("\n--- 2. Executing Beeline command ---")
    print(f"Beeline Command: {beeline_command}")
    
    try:
        # Execute the beeline command
        # 'shell=True' is used here to simplify command execution as the SQL query
        # contains internal quotes and special characters, making shlex.split 
        # complex for this specific case. Using 'shell=True' requires caution.
        beeline_result = subprocess.run(
            beeline_command, 
            shell=True,
            check=True, 
            capture_output=True, 
            text=True,
            # Set the environment variable to ensure beeline doesn't ask for a password
            # This is often needed when using Kerberos
            env={**os.environ, "HADOOP_USER_NAME": principal.split('@')[0]}
        )
        
        print("\nBeeline execution successful.")
        print("--- Beeline Output ---")
        print(beeline_result.stdout)
        
    except subprocess.CalledProcessError as e:
        print(f"\nBeeline failed with error code {e.returncode}.")
        print("--- Beeline Error Output ---")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        
    except FileNotFoundError:
        print("Error: 'beeline' command not found. Ensure Hive client tools are installed and in PATH.")


# --- Configuration ---
# You MUST replace these placeholders with your actual values.
KEYTAB_FILE = "/etc/security/keytabs/myuser.keytab"
KERBEROS_PRINCIPAL = "myuser@EXAMPLE.COM" 
HIVE_JDBC_URL = "jdbc:hive2://hive-server.example.com:10000/default;principal=hive/_HOST@EXAMPLE.COM"
LOCAL_CSV_FILE = "/tmp/my_data.csv" # Ensure this file exists on the server
HDFS_TARGET_DIR = "/user/myuser/data/my_table_data" # The HDFS directory the data will be loaded into

# --- Run the function ---
if __name__ == "__main__":
    load_csv_to_hdfs_with_beeline(
        KEYTAB_FILE,
        KERBEROS_PRINCIPAL,
        HIVE_JDBC_URL,
        LOCAL_CSV_FILE,
        HDFS_TARGET_DIR
    )
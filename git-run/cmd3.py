from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- Configuration ---
# 1. Define the public URL for your raw script on GitHub
# IMPORTANT: Use the **raw** file URL (e.g., raw.githubusercontent.com/user/repo/branch/script.sh)
GITHUB_SCRIPT_URL = "YOUR_RAW_GITHUB_SCRIPT_URL_HERE"
SCRIPT_NAME = "my_github_script.sh"
# Define the temporary path where the script will be saved
TEMP_SCRIPT_PATH = f"/tmp/{SCRIPT_NAME}"
# ---------------------

with DAG(
    dag_id="download_chmod_and_execute_github_script",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["github", "bash", "chmod", "example"],
) as dag:
    
    # 1. Download the script from the raw GitHub URL
    download_script_task = BashOperator(
        task_id="download_github_script",
        # 'curl -Lso' means: Follow redirects (-L), silent (-s), output to file (-o)
        bash_command=f"curl -Lso {TEMP_SCRIPT_PATH} {GITHUB_SCRIPT_URL}",
    )
    
    # 2. Grant execute permission using chmod +x
    chmod_script_task = BashOperator(
        task_id="grant_execute_permission",
        # 'chmod +x' adds the execute permission for all users
        bash_command=f"chmod +x {TEMP_SCRIPT_PATH}",
    )
    
    # 3. Execute the script
    execute_local_script_task = BashOperator(
        task_id="execute_local_script",
        # Run the script, optionally passing arguments after the path
        # Example: {TEMP_SCRIPT_PATH} arg1 arg2
        bash_command=f"{TEMP_SCRIPT_PATH}",
    )
    
    # Define the execution order
    download_script_task >> chmod_script_task >> execute_local_script_task
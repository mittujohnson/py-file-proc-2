from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- Configuration ---
# IMPORTANT: Use the **raw** file URL (e.g., raw.githubusercontent.com/user/repo/branch/script.sh)
GITHUB_SCRIPT_URL = "YOUR_RAW_GITHUB_SCRIPT_URL_HERE"
SCRIPT_NAME = "my_github_script.sh"
LOG_FILE_NAME = "my_github_script_output.log"

# Define the paths where the script and log file will be saved
TEMP_SCRIPT_PATH = f"/tmp/{SCRIPT_NAME}"
TEMP_LOG_PATH = f"/tmp/{LOG_FILE_NAME}"
# ---------------------

with DAG(
    dag_id="script_with_env_dump_and_logging",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["github", "bash", "logging", "debug"],
) as dag:
    
    # NEW TASK: Print all environment variables to the task log
    print_env_task = BashOperator(
        task_id="print_environment_variables",
        # 'printenv' is a standard Linux/Unix utility to display all environment variables
        bash_command="echo '--- START ENVIRONMENT VARIABLES ---' && printenv && echo '--- END ENVIRONMENT VARIABLES ---'",
    )
    
    # 1. Download the script from the raw GitHub URL
    download_script_task = BashOperator(
        task_id="download_github_script",
        bash_command=f"curl -Lso {TEMP_SCRIPT_PATH} {GITHUB_SCRIPT_URL}",
    )
    
    # 2. Grant execute permission using chmod +x
    chmod_script_task = BashOperator(
        task_id="grant_execute_permission",
        bash_command=f"chmod +x {TEMP_SCRIPT_PATH}",
    )
    
    # 3. Execute the script AND redirect all output to a log file
    execute_and_log_task = BashOperator(
        task_id="execute_script_and_log_output",
        bash_command=f"{TEMP_SCRIPT_PATH} > {TEMP_LOG_PATH} 2>&1",
    )
    
    # 4. Print the contents of the log file to Airflow's task logs
    read_log_task = BashOperator(
        task_id="print_log_to_airflow_ui",
        bash_command=f"echo '--- Script Log Contents ({TEMP_LOG_PATH}) ---' && cat {TEMP_LOG_PATH}",
        trigger_rule="all_done",
    )
    
    # Define the execution order
    # The environment check runs first
    print_env_task >> download_script_task >> chmod_script_task >> execute_and_log_task >> read_log_task
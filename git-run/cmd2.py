from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Define the public URL for your raw script on GitHub
# REMEMBER: This must be the **raw** file URL (e.g., raw.githubusercontent.com/...)
GITHUB_SCRIPT_URL = "YOUR_RAW_GITHUB_SCRIPT_URL_HERE"
SCRIPT_NAME = "my_github_script.sh"

with DAG(
    dag_id="execute_github_shell_script",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["github", "bash", "example"],
) as dag:
    
    # Option A: Fetch and Execute in a single command (more concise)
    # This pipes the script content directly to 'bash -s' for execution.
    fetch_and_run_script_task = BashOperator(
        task_id="fetch_and_run_github_script",
        bash_command=f"curl -s {GITHUB_SCRIPT_URL} | bash -s -- arg1 arg2",
        # Use 'bash -s --' to read the script from standard input.
        # 'arg1 arg2' are optional arguments you'd pass to the script.
    )

    # --- OR ---

    # Option B: Download the script to a temporary location, then execute it (more readable/debuggable)
    
    # 1. Download the script
    download_script_task = BashOperator(
        task_id="download_github_script",
        bash_command=f"curl -Lso /tmp/{SCRIPT_NAME} {GITHUB_SCRIPT_URL}",
        # -L: Follow redirects, -s: Silent, -o: Output to file
    )
    
    # 2. Make it executable and run it
    execute_local_script_task = BashOperator(
        task_id="execute_local_script",
        bash_command=f"chmod +x /tmp/{SCRIPT_NAME} && /tmp/{SCRIPT_NAME} arg1 arg2",
    )
    
    # Define the dependency for Option B
    download_script_task >> execute_local_script_task
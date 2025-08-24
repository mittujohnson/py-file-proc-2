from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hive_etl_pipeline",
    schedule=None,  # Set a schedule like "@daily" or "0 0 * * *"
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["hive", "etl"],
) as dag:
    
    # Task to run the Python script that contains the Hive logic
    # The 'python3' command executes the hive_driver_script.py file
    run_hive_etl_task = BashOperator(
        task_id="run_hive_etl_process",
        bash_command="python3 /path/to/your/hive_driver_script.py",
    )

    # You can add more tasks here for a more complex pipeline
    # For example, a task to run 'MSCK REPAIR' or to send a success notification

    # Define the task dependencies
    # In this simple example, we only have one task, but if you had
    # others, you would define their order here.
    # e.g., task1 >> task2 >> task3

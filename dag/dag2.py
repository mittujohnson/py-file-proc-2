from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG.
# These will be applied to all tasks in the DAG.
with DAG(
    dag_id="dynamic_parallel_tasks_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["dynamic", "parallel"],
) as dag:
    
    # ------------------
    # User-defined tasks
    # ------------------
    # This is the list of task configurations. Each dictionary specifies
    # the task_id and the corresponding bash parameter.
    task_configs = [
        {"task_id": "parallel_task_a", "bash_param": "data_file_1.csv"},
        {"task_id": "parallel_task_b", "bash_param": "report_type_2"},
        {"task_id": "parallel_task_c", "bash_param": "user_id_345"},
        {"task_id": "parallel_task_d", "bash_param": "log_file_xyz"},
        {"task_id": "parallel_task_e", "bash_param": "database_name_prod"},
    ]

    # ---------------
    # Define tasks
    # ---------------

    # 1. Start Task
    # This task will run first.
    start_task = BashOperator(
        task_id="start_processing",
        bash_command='echo "Starting the data processing workflow..."'
    )

    # 2. Parallel Tasks
    # Dynamically create BashOperator tasks from the task_configs list.
    # We loop through the list of dictionaries and use the values to
    # set the task_id and construct the bash_command.
    parallel_tasks = [
        BashOperator(
            task_id=config["task_id"],
            bash_command=f'echo "Executing a script with parameter: {config["bash_param"]}"'
            # In a real-world scenario, you could replace the above command with something like:
            # bash_command=f'my_script.sh --parameter {config["bash_param"]}'
        )
        for config in task_configs
    ]

    # 3. End Task
    # This task will only run after all parallel tasks have completed.
    end_task = BashOperator(
        task_id="end_processing",
        bash_command='echo "All parallel tasks have finished. Ending the workflow."'
    )

    # ------------------
    # Define dependencies
    # ------------------
    # The start_task runs, then all parallel_tasks run, and finally the end_task runs.
    # The '>>' operator can be used with a list of tasks to create parallel dependencies.
    start_task >> parallel_tasks >> end_task

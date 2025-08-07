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
    # This is the list of task names that will be created dynamically
    # and run in parallel. You can modify this list as needed.
    task_names = [
        "parallel_task_a",
        "parallel_task_b",
        "parallel_task_c",
        "parallel_task_d",
        "parallel_task_e",
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
    # Dynamically create BashOperator tasks from the task_names list.
    # We use a list comprehension to make this clean and efficient.
    # The bash_command now explicitly passes the task_name as an argument.
    parallel_tasks = [
        BashOperator(
            task_id=task_name,
            bash_command=f'echo "Executing a script with parameter: {task_name}"'
            # In a real-world scenario, you could replace the above command with something like:
            # bash_command=f'my_script.sh --task-name {task_name}'
        )
        for task_name in task_names
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

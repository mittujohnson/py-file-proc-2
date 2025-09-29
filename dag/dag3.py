from __future__ import annotations

import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    dag_id="xcom_example_dag",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["xcom_example"],
) as dag:
    @task(task_id="pushing_task")
    def push_data():
        """
        Pushes a simple string value to XCom.
        The return value of this decorated function is automatically pushed to XCom
        with a key of 'return_value'.
        """
        print("Pushing a value to XCom.")
        return "Hello from the pushing task!"

    @task(task_id="pulling_task")
    def pull_data(pulled_value: str):
        """
        Pulls the value from the 'pushing_task' using the TaskFlow API.
        The 'pulled_value' parameter automatically receives the 'return_value' XCom
        from the upstream task.
        """
        print("Pulling a value from XCom.")
        print(f"The value received is: {pulled_value}")

    # The dependency between the tasks
    pull_data(pull_data.override(task_id="pulling_task")(push_data()))


    from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="parent_dag",
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["parent"],
) as dag:
    # This is a task that must complete successfully to trigger the next DAG
    parent_task = BashOperator(
        task_id="parent_task",
        bash_command="echo 'Running parent task...'; sleep 5; echo 'Parent task complete!'"
    )

    # The TriggerDagRunOperator triggers the dependent_dag
    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dependent_dag",  # This must match the ID of the dependent DAG
        # Pass a dictionary of config values to the dependent DAG (optional)
        conf={"message": "This is a message from the parent DAG."}
    )

    # Set the dependency: trigger the dependent DAG only after the parent_task succeeds
    parent_task >> trigger_dependent_dag



    from __future__ import annotations
import datetime
import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dependent_dag",
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None, # A scheduled interval is not needed as it's triggered externally
    catchup=False,
    tags=["dependent"],
) as dag:
    def process_parent_data(**context):
        """
        Retrieves and uses the configuration data passed by the parent DAG.
        The conf is available in the 'dag_run' object of the context.
        """
        dag_run_conf = context['dag_run'].conf or {}
        message = dag_run_conf.get("message", "No message received.")
        print(f"Received message from parent DAG: {message}")
        print("Starting dependent DAG run...")

    # The first task in the dependent DAG
    dependent_task = PythonOperator(
        task_id="dependent_task",
        python_callable=process_parent_data,
    )


    from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# The Python function you want to call from the DAG.
# It takes five keyword arguments.
def my_python_program(arg1, arg2, arg3, arg4, arg5):
    """
    This is the Python function that will be executed as an Airflow task.
    It takes five arguments and prints them to the logs.
    """
    print(f"Argument 1: {arg1}")
    print(f"Argument 2: {arg2}")
    print(f"Argument 3: {arg3}")
    print(f"Argument 4: {arg4}")
    print(f"Argument 5: {arg5}")
    return "Program executed successfully!"


with DAG(
    dag_id="python_operator_with_args",
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Use op_kwargs to pass keyword arguments to the function
    run_python_task = PythonOperator(
        task_id="run_my_python_program",
        python_callable=my_python_program,
        op_kwargs={
            "arg1": "value_for_arg1",
            "arg2": 123,
            "arg3": True,
            "arg4": ["a", "b", "c"],
            "arg5": {"key": "value"},
        },
    )

    # You can also chain tasks, for example:
    # another_task = BashOperator(task_id="next_step", bash_command="echo 'Next step...'")
    # run_python_task >> another_task


    from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="date_arg_example",
    start_date=datetime.datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["templating"],
) as dag:
    # `{{ ds }}` gets replaced by the execution date of the DAG in 'YYYY-MM-DD' format.
    run_with_date = BashOperator(
        task_id="run_with_date_arg",
        bash_command="echo 'Processing data for date: {{ ds }}' && your_program.sh -d {{ ds }}",
    )


    from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="call_python_script_from_bash",
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bash"],
) as dag:
    # Use {{ ds }} to dynamically pass the date to the script
    call_script_with_date_arg = BashOperator(
        task_id="run_python_script",
        bash_command="python my_python_program.py -d '{{ ds }}'",
    )


    # This function would contain the core logic of your program
def my_python_program(execution_date):
    """
    This function will be called by the PythonOperator.
    It takes one argument, which will be the execution date.
    """
    print(f"Running program for date: {execution_date}")
    # Add your program's logic here, for example:
    # import pandas as pd
    # df = pd.read_csv(f'data/sales_{execution_date}.csv')
    # ... your processing code ...
    return f"Program finished for {execution_date}"


from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# The python function defined above
def my_python_program(execution_date):
    print(f"Running program for date: {execution_date}")
    return f"Program finished for {execution_date}"

with DAG(
    dag_id="python_operator_date_arg",
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["python"],
) as dag:
    # Pass the date using op_kwargs and the {{ ds }} template
    run_program = PythonOperator(
        task_id="run_python_program",
        python_callable=my_python_program,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
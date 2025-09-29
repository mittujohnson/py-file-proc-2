from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="upstream_data_prep",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule="@daily", # Scheduled to run daily
    catchup=False,
    tags=["external_dependency"],
) as dag:
    
    # This task simulates the final step (e.g., writing clean data to S3).
    # This is the task the downstream DAG will wait for.
    data_completion_task = BashOperator(
        task_id="data_completion_task",
        bash_command="echo 'Upstream data preparation finished successfully!';"
    )



    ---------

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="downstream_reporting",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule="@daily", # Scheduled at the same frequency as the parent
    catchup=False,
    tags=["external_dependency"],
) as dag:
    
    # 1. External Task Sensor Configuration
    wait_for_upstream_data = ExternalTaskSensor(
        task_id="wait_for_upstream_data",
        # The ID of the DAG this sensor is watching
        external_dag_id="upstream_data_prep", 
        # The specific task ID in the external DAG to wait for
        external_task_id="data_completion_task",
        # Ensures the sensor checks the run corresponding to its own execution date
        ##execution_date_fn=lambda dt: dt, 
        execution_delta=timedelta(minutes=15)
# The sensor looks for the Parent DAG run that is 15 minutes older than the current Child DAG run.
        poke_interval=120,    # Check every 120 seconds (2 minutes)
        timeout=86400,        # Fail the sensor after 24 hours if the task hasn't finished
        mode="reschedule",    # Releases the worker slot while waiting (resource efficient)
    )

    # 2. Task that runs AFTER the external task is successful
    generate_report = BashOperator(
        task_id="generate_report",
        bash_command="echo 'Dependency met! Starting report generation using the new data.'"
    )
    
    # Define the dependency: The report must wait for the data completion sensor.
    wait_for_upstream_data >> generate_report
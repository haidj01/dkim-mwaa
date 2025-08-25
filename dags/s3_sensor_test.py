from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime
from util import s3_sensor

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
)
def file_sensor_test():
    # Create the sensor task
    wait_for_file = s3_sensor(
        task_id="s3_sensor",
        bucket="dkim-dataprocessing",
        key="csv/emp.csv"
    )

    # Optional: Add a task to run after sensor succeeds
    def process_after_sensor():
        print("File detected! Processing...")

    process_task = PythonOperator(
        task_id="process_file",
        python_callable=process_after_sensor
    )

    wait_for_file >> process_task

file_sensor_test()
import pendulum

from airflow import DAG
from airflow.sensors.file_with_md5 import FileWithMd5Sensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = dict(
    owner="file-sensor-example-owner",
)

dag = DAG(
    "file_sensor_example",
    default_args=default_args,
    # Start it 2020-01-01
    start_date=datetime(
        2020, 1, 1,
        tzinfo=pendulum.timezone("Asia/Singapore")
    ),
    # Don't need to catchup, just run the latest
    catchup=False,
    # Start this dag once every 5 mins
    schedule_interval=timedelta(seconds=30),
    # There should not be more than one copy of this dag active at any one time
    max_active_runs=1,
)

sense_file = FileWithMd5Sensor(
    task_id="sense_csv_availability",
    conn_id="sales_csv",
    soft_fail=True,
    poke_interval=10,
    timeout=25,
    mode="reschedule",
    dag=dag
)

def handle_file(**context):
    ti = context["task_instance"]
    ti.xcom_pull(key=self.xcom_key, value=ret)

operate_on_file = PythonOperator(
    task_id="do_something_with_file",
    provide_context=True,
    python_callable=handle_file,
    dag=dag
)

sense_file >> operate_on_file

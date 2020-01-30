import pendulum

from airflow import DAG
from airflow.sensors.file_with_md5 import FileWithMd5Sensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = dict(
    owner="example-owner",
)

dag = DAG(
    "file_with_md5_example",
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
    xcom_key_file_path="sales_csv_file_path",
    xcom_key_md5_path="sales_csv_md5_path",
    soft_fail=True,
    poke_interval=10,
    timeout=25,
    mode="reschedule",
    dag=dag
)

def handle_file(**context):
    ti = context["task_instance"]
    file_path = ti.xcom_pull(task_ids="sense_csv_availability", key="sales_csv_file_path")
    md5_path = ti.xcom_pull(task_ids="sense_csv_availability", key="sales_csv_md5_path")

    print(file_path)
    with open(file_path) as f:
        print(f.read())
    print(md5_path)
    with open(md5_path) as f:
        print(f.read())

operate_on_file = PythonOperator(
    task_id="do_something_with_file",
    provide_context=True,
    python_callable=handle_file,
    dag=dag
)

remove_files = BashOperator(
    task_id="remove_files",
    bash_command=(
        "rm "
        "{{ task_instance.xcom_pull(task_ids='sense_csv_availability', key='sales_csv_file_path') }} "
        "{{ task_instance.xcom_pull(task_ids='sense_csv_availability', key='sales_csv_md5_path') }}"
    ),
    dag=dag
)

sense_file >> operate_on_file >> remove_files

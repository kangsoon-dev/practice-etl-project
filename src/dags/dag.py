from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from ingest_script import process_jobs, archive_records, load_good_jobs
import datetime

default_args = {
    'owner': 'Kang Soon Ang',
    'start_date': datetime.datetime(2021, 1, 1),
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=10)
}

dag = DAG(
    dag_id='process_json_data',
    #schedule_interval=datetime.timedelta(minutes=1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False)

# task to process all files inside incoming folder
t_process_incoming_files = BranchPythonOperator(
    task_id='process_incoming_files',
    python_callable=process_jobs,
    op_kwargs = {"base_dir":"./data/"},
    dag=dag,
)

t_archive_records = PythonOperator(
    task_id='archive_records',
    python_callable=archive_records,
    op_kwargs = {"base_dir":"./data/", "new_good_lines_dir":"processed/new_good_lines.txt", "new_bad_lines_dir":"processed/new_bad_lines.txt"},
    do_xcom_push = True,
    dag=dag,
)

t_load_db = PythonOperator(
    task_id='load_good_jobs',
    python_callable=load_good_jobs,
    op_kwargs = {"new_good_lines_dir":t_archive_records.output},
    dag=dag,
)

# The trigger rule 'none_failed_or_skipped' ensures that the dummy task is executed if at least one parent succeeds
t_endRun = DummyOperator(
    task_id='end_run', 
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

t_process_incoming_files >> t_archive_records >> t_load_db >> t_endRun
t_process_incoming_files >> t_endRun


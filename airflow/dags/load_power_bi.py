from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# === Cấu hình DAG ===
default_args = {
    'owner': 'group_03',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG(
    dag_id='send_data_to_powerbi',
    default_args=default_args,
    schedule_interval='*/3 * * * *',  # Mỗi 3 phút
    # schedule_interval='0 2 * * *',  # Chạy lúc 02:00 sáng mỗi ngày
    start_date=datetime(2025, 7, 10),
    catchup=False,
    tags=['powerbi', 'hdfs']
)

dag_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(dag_dir, '..', '..'))
path_compact_csv = os.path.join(project_dir, 'hadoop', 'compact_csv.py')
path_load_data = os.path.join(project_dir, 'powerbi', 'load_data.py')

compact_task = BashOperator(
    task_id='compact_csv_to_one_file',
    bash_command=f'spark-submit {path_compact_csv} && sleep 10',
    dag=dag
)

load_data = BashOperator(
    task_id='load_data_power_bi',
    bash_command=f'python3 {path_load_data}',
    dag=dag
)

compact_task >> load_data

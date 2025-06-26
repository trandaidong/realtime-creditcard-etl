from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'group_03',
    'start_date': datetime(2025, 6, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'shutdown_kafka_hadoop',
    default_args=default_args,
    schedule_interval=None,  # chạy thủ công
    catchup=False,
    description='Tắt Spark Streaming, Kafka Producer, Hadoop, Kafka, ZooKeeper',
) as dag:

    # # 1. Dừng Kafka Producer (nếu là process nền)
    # stop_kafka_producer = BashOperator(
    #     task_id='stop_kafka_producer',
    #     bash_command="pkill -f GD1_kafka.ipynb || true",
    # )

    # # 2. Dừng Spark Streaming notebook
    # stop_spark_streaming = BashOperator(
    #     task_id='stop_spark_streaming',
    #     bash_command="pkill -f GD1_Spark_streaming.ipynb || true",
    # )

    # 3. Stop YARN
    stop_yarn = BashOperator(
        task_id='stop_yarn',
        bash_command="'stop-yarn.sh'",
    )

    # 4. Stop HDFS
    stop_hdfs = BashOperator(
        task_id='stop_hdfs',
        bash_command="'stop-dfs.sh'",
    )

    # # 5. Stop Kafka
    # stop_kafka = BashOperator(
    #     task_id='stop_kafka',
    #     bash_command="ps aux | grep kafka.Kafka | grep -v grep | awk '{print $2}' | xargs -r kill",
    # )

    # # 6. Stop ZooKeeper
    # stop_zookeeper = BashOperator(
    #     task_id='stop_zookeeper',
    #     bash_command="ps aux | grep zookeeper | grep -v grep | awk '{print $2}' | xargs -r kill",
    # )

    # # 7. Stop SSH (nếu cần)
    # stop_ssh = BashOperator(
    #     task_id='stop_ssh',
    #     bash_command='service ssh stop || true',
    # )

    # Thứ tự tắt: streaming trước, producer, rồi cụm Hadoop, Kafka
    # [stop_spark_streaming, stop_kafka_producer] >> stop_yarn >> stop_hdfs >> stop_kafka >> stop_zookeeper >> stop_ssh
    stop_yarn >> stop_hdfs

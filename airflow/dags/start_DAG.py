from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'group_03',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'start_kafka_hadoop_and_run_notebooks',
    default_args=default_args,
    start_date =  datetime(2025, 6,22),
    # schedule_interval=None,  # chạy thủ công
    schedule=None,
    catchup=False,
    description='Khởi động Kafka, Hadoop và chạy 2 notebook xử lý dữ liệu',
) as dag:
    
    # 1. Khởi động ZooKeeper
#     start_zookeeper = BashOperator(
#     task_id='start_zookeeper',
#     bash_command='bash -c "cd /home/panda/kafka_2.13-3.9.1 && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 & sleep 5"',
# )
    start_zookeeper = BashOperator(
        task_id='start_zookeeper',
        bash_command="""
        cd /home/panda/kafka_2.13-3.9.1 &&
        nohup bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1 &

        echo "🕒 Waiting for ZooKeeper..."

        for i in {1..10}; do
            if nc -z localhost 2181; then
                echo "✅ ZooKeeper is up."
                exit 0
            fi
            sleep 2
        done

        echo "❌ ZooKeeper failed to start in 20s"
        exit 1
        """,
        dag=dag
    )


    # # 2. Khởi động Kafka Server
    # start_kafka = BashOperator(
    #     task_id='start_kafka',
    #     bash_command="""
    #     cd /home/panda/kafka_2.13-3.9.1 || exit 1
    #     echo "Starting Kafka..." >> /tmp/kafka_debug.log
    #     sleep 5
    #     nohup bin/kafka-server-start.sh config/server.properties >> /tmp/kafka.log 2>&1 &
    #     echo "Kafka started at $(date)" >> /tmp/kafka_debug.log
    #     """,
    # )


    # # 3. Tạo Kafka Topic
    # create_kafka_topic = BashOperator(
    #     task_id='create_kafka_topic',
    #     bash_command="""
    #         sleep 10
    #         /home/panda/kafka_2.13-3.9.1/bin/kafka-topics.sh \
    #         --create \
    #         --topic transactions \
    #         --bootstrap-server localhost:9092 \
    #         --partitions 1 \
    #         --replication-factor 1 || true
    #     """
    # )
    # 4. Khởi động SSH và Hadoop (do wsl của tôi là 20, không tự kick hoạt ssh nên mỗi lần muốn chạy hadoop phải khởi động ssh trước, có thể bỏ qua)
    # start_ssh = BashOperator(
    #     task_id='start_ssh',
    #     bash_command='sudo /usr/sbin/service ssh start',
    # )

    # start_hdfs = BashOperator(
    #     task_id='start_hdfs',
    #     bash_command="'start-dfs.sh'",
    # )

    # start_yarn = BashOperator(
    #     task_id='start_yarn',
    #     bash_command="'start-yarn.sh'",
    # )

    # # 5. Chạy notebook kafka producer
    # run_kafka_notebook = BashOperator(
    #     task_id='run_kafka_notebook',
    #     bash_command="""
    #     source /home/panda/Documents/ODAP/Project/venv310/bin/activate && \
    #     jupyter nbconvert --to notebook --execute /home/panda/Documents/ODAP/Project/kafka/producer.ipynb --output /tmp/output_kafka.ipynb 
    #     """,
    # )

    # # 6. Chạy notebook Spark Streaming
    # run_spark_streaming_notebook = BashOperator(
    #     task_id='run_spark_streaming_notebook',
    #     bash_command="""
    #     source /home/ld/miniconda3/etc/profile.d/conda.sh && \
    #     conda activate spark-env && \
    #     jupyter nbconvert --to notebook --execute /home/ld/Hk2_Nam3/DE_final_project/GD1_Spark_streaming.ipynb --output /tmp/output_spark.ipynb > /tmp/spark_streaming.log 2>&1 &
    #     """,
    # )
    start_zookeeper
    # start_zookeeper >> start_hdfs >> start_yarn
    # Thiết lập thứ tự task
    # start_zookeeper >> start_kafka >> create_kafka_topic>> start_hdfs >> start_yarn >> run_kafka_notebook
    # start_yarn >> run_kafka_notebook >> run_spark_streaming_notebook

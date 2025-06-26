from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from hdfs import InsecureClient
import json
import requests
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
    schedule_interval='*/10 * * * *',  # Mỗi 10 phút
    start_date=datetime(2025, 6, 26),
    catchup=False,
    tags=['powerbi', 'hdfs']
)

# === Thông tin HDFS & Power BI ===
FOLDER_HDFS_PATH = '/transactions_by_hour_compacted/'
HDFS_CLIENT = InsecureClient('http://localhost:9870', user='panda')
# PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/88dcb9eb-c4d0-4f67-8b9a-d0c4a1ae501c/rows?experience=power-bi&key=uLrnDuq85qTeWWWfTmZy5owDgmqeBI3m4Ef%2FP3AEZ9ZpHJrPx33S7AAFB5Sy3ucYxmDFr9gqS83%2F9UxACGm1Pg%3D%3D"
PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/c66338c9-77f3-4ffe-ad88-14192604b2f7/rows?experience=power-bi&key=XpLZ%2BiR2Yi0%2FkAI9JgXBloN%2F%2BSYj7bhR0ZudRVk5hn8Is%2Byy4KiCvWnDtVMy6UOpXfN%2BENiNMuSg58YmkYfoCw%3D%3D"
LAST_PUSH_FILE = '/tmp/last_push_time.txt'  # Nơi lưu timestamp cuối

def send_new_data():
    # 1. Luu timestamp lần dau
    if not os.path.exists(LAST_PUSH_FILE):
        with open(LAST_PUSH_FILE, "w") as f:
            f.write("1970-01-01T00:00:00Z")

    # 2. Đọc dữ liệu từ HDFS
    # Lấy danh sách file trong thư mục
    files = HDFS_CLIENT.list(FOLDER_HDFS_PATH)

    # Lọc ra file .csv chứa chữ 'part'
    csv_files = [f for f in files if f.endswith('.csv') and 'part' in f]

    # Sắp xếp để lấy file mới nhất
    csv_files.sort(reverse=True)

    # Kiểm tra file
    if not csv_files:
        raise Exception("Không tìm thấy file CSV phù hợp.")

    # Lấy tên file đầu tiên
    latest_file = csv_files[0]
    with HDFS_CLIENT.read(FOLDER_HDFS_PATH + latest_file, encoding='utf-8') as reader:
        df = pd.read_csv(reader, header = 0)

    # Đọc mốc thời gian cuối
    with open(LAST_PUSH_FILE, "r") as f:
        last_push_time = pd.to_datetime(f.read().strip())

    # 3. Lọc & chuyển đổi dữ liệu
    rows = []
    for _, row in df.iterrows():
        try:
            user_val = int(row["User"])
        except ValueError:
            # print(row)
            continue  # bỏ qua dòng lỗi
        # print(pd.to_datetime(row["event_time"], utc=True))
        if pd.to_datetime(row["event_time"], utc=True) > last_push_time:
            rows.append({
                "User": int(row["User"]),
                "Card": int(row["Card"]),
                "event_time": row["event_time"],
                "Amount": float(row["Amount_casted"]),
                "Use Chip": str(row["Use Chip"]),
                "Merchant Name": str(row["Merchant Name"]),
                "Merchant City": str(row["Merchant City"]),
                "Merchant State": str(row["Merchant State"]),
                "Zip": float(row["Zip"]),
                "MCC": float(row["MCC"]) ,
                "Errors": "" if pd.isna(row["Errors"]) else str(row["Errors"]),
                "Is Fraud": str(row["Is Fraud"])
            })

    # 4. Đẩy dữ liệu theo batch
    headers = {'Content-Type': 'application/json'}
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        res = requests.post(PUSH_URL, headers=headers, data=json.dumps(batch))
        print(f"✅ Batch {i} status: {res.status_code}")

    # 5. Lưu lại timestamp mới
    if rows:
        # Ép về datetime để gọi .isoformat()
        latest_time = max([pd.to_datetime(r["event_time"]) for r in rows])

        # Ghi vào file
        with open(LAST_PUSH_FILE, "w") as f:
            f.write(latest_time.isoformat())

# === Định nghĩa task ===
send_task = PythonOperator(
    task_id='send_to_power_bi',
    python_callable=send_new_data,
    dag=dag
)

compact_task = BashOperator(
    task_id='compact_csv_to_one_file',
    bash_command='spark-submit /home/panda/Documents/ODAP/Project/hadoop/compact_csv.py && sleep 10',
    dag=dag
)

compact_task >> send_task

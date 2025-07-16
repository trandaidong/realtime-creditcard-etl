import pandas as pd
from hdfs import InsecureClient
import requests
import json
import os

# === Thông tin HDFS & Power BI ===
FOLDER_HDFS_PATH = '/transactions_compacted/'
HDFS_CLIENT = InsecureClient('http://localhost:9870', user='panda')
# PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/b58ae7bd-da6d-4351-abc4-60d1fa433847/rows?ctid=40127cd4-45f3-49a3-b05d-315a43a9f033&experience=power-bi&key=mM3Nozpm93Z8o3TzkLVzRUcOJrrUY5mlFUs7sF6WX%2Fy%2B%2FztfuJYfo%2BmZNszqbNNer56JyYkl7UlXCyeDCWGg5Q%3D%3D"
# PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/e54e8796-dae7-47db-995c-a4b8e2ea207f/rows?experience=power-bi&key=25LNO3DpF5izqrN6KuxXOMJbAlxvyuQjLRw31j5iA8Jh%2B9XyXsDtLwDPJW%2BhlvSqbHe0S%2B%2Bo7z01aiqCxnEYlQ%3D%3D"
PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/93861e98-1b48-4acb-89ed-deb3672f64b6/rows?experience=power-bi&key=kg1p0dFi9KhoK%2B7vPyL40sgFRBx5MkvqDC8c6GPbz%2BjWDzEqRJ%2B4PT%2B720Abm4zAY6RPDtpQWLfjlOoISEFnTA%3D%3D"
# Đọc dữ liệu từ HDFS
# Lấy danh sách file trong thư mục
files = HDFS_CLIENT.list(FOLDER_HDFS_PATH,status=True)


csv_files = [
    (name, metadata['modificationTime'])
    for name, metadata in files
    if name.endswith('.csv') and 'part' in name
]

# Nếu có file phù hợp
if csv_files:
    csv_files.sort(key=lambda x: x[1], reverse=True)
    latest_file = csv_files[0][0]
    print(f"Lastest_file: {latest_file}")
else:
    print("No .csv file found")

with HDFS_CLIENT.read(FOLDER_HDFS_PATH + latest_file, encoding='utf-8') as reader:
    df = pd.read_csv(reader, header = 0)


# 3. Lọc & chuyển đổi dữ liệu
rows = []
for _, row in df.iterrows():
    try:
        user_val = int(row["Card"])
    except ValueError:
        continue  # bỏ qua dòng lỗi
    rows.append({
        "User": str(row["User"]),
        "Card": str(row["Card"]),
        "event_time": row["event_time"],
        "Hour": int(row["Hour"]),
        "Amount": float(row["Amount_casted"]),
        "Use Chip": str(row["Use Chip"]),
        "Merchant Name": str(row["Merchant Name"]),
        "Merchant City": str(row["Merchant City"]),
        "Merchant State": str(row["Merchant State"]),
        "Zip": float(row["Zip"]),
        "MCC": float(row["MCC"]) ,
        "Errors": "" if pd.isna(row["Errors"]) else str(row["Errors"]),
        "Is Fraud": str(row["Is Fraud"]),
        "DayOfWeek": str(row["DayOfWeek"])
    })
print(f"Tổng số dòng cần push: {len(rows)}")
for i in rows:
    print(i)
# 4. Đẩy dữ liệu theo batch
headers = {'Content-Type': 'application/json'}
batch_size = 100
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    res = requests.post(PUSH_URL, headers=headers, data=json.dumps(batch))
    print(f"✅ Batch {i} status: {res.status_code}")

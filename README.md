Đảm bảo đã cài đặt Kafka, Hadoop và Spark trên máy
----- Các yêu cầu cần thiết ------
Bước đầu tiên cần tạo môi trường ảo để chạy
Trong project mở cửa số terminal lên và chạy:
    python3.10 -m venv venv310
Kích hoạt môi truờng ảo cho Project
    source venv310/bin/activate
Tải các thư viện cần thiết liên quan
    pip install -r requirements.txt
Cấp quyền thực thi file 
    chmod +x install_airflow.sh
Chạy lệnh sau để cài được phiên bản airflow phù hợp, không bị xung đột
    ./install_airflow.sh
Khởi tạo cơ sở dữ liệu Airflow
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db init
-- Tạo user để đăng nhập web UI
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
Mở ternimal mới của thư mục Project
    source venv310/bin/activate
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow scheduler
Mở ternimal mới của thư mục Project
    source venv310/bin/activate
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow webserver --port 8080
-> Lúc này có thể mở localhost:8080 để xem màn hình web của Airflow
--------------------------------------------
ĐẢM BẢO ĐÃ KHỞI ĐỘNG SERVER KAFKA VÀ HADOOP
--------------------------------------------
Quy trình chạy pipeline
B1: Sinh data nếu chưa có -> Chạy thư mục Generate_data.py trong thư mục data để sinh ra file data.csv 
B2: Chạy script producer.ipynb để thực hiện đưa dữ liệu vào topic 'transactions'
B3: Chạy script comsumer_stream.ipynb để thực hiện chuẩn hóa dữ liệu và đưa vào hadoop
B4: Mở localhost:8080 (mặc định của airflow) để chạy DAG send_data_to_powerbi
B5: Truy cập đường link power bi trong thư mục powerbi

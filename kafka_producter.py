import time
import json
import os
import pandas as pd
from kafka import KafkaProducer

# Cấu hình các tham số kết nối
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'ecommerce-events'
DATASET_NAME = '2019-Oct.csv'  # Đã cấu hình theo đúng file thực tế của bạn

DATASET_PATH = os.path.join('dataset', DATASET_NAME)

def initialize_producer():
    """Khởi tạo kết nối an toàn tới Kafka Broker"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[-] Kết nối Kafka Broker thành công!")
        return producer
    except Exception as e:
        print(f"[!] Lỗi kết nối Kafka: {e}")
        return None

def process_and_stream_data():
    producer = initialize_producer()
    if not producer:
        return

    if not os.path.exists(DATASET_PATH):
        print(f"[!] Lỗi: Không tìm thấy file dữ liệu tại {DATASET_PATH}.")
        return

    print("[-] Đang đọc và làm sạch Dataset bằng Pandas...")
    
    # Giữ đúng 5 cột cốt lõi phục vụ bài toán xử lý dữ liệu lớn
    keep_columns = ['event_time', 'event_type', 'product_id', 'category_id', 'user_id']
    
    # Cơ chế đọc chunking (mỗi lần 50,000 dòng) giúp máy ảo không bị crash RAM
    chunk_size = 50000 
    for chunk in pd.read_csv(DATASET_PATH, usecols=keep_columns, chunksize=chunk_size):
        
        # Bộ lọc yêu cầu logic: Chỉ lấy loại hành vi "view"
        filtered_df = chunk[chunk['event_type'] == 'view']
        
        print(f"[-] Bắt đầu phát cụm dữ liệu chứa {len(filtered_df)} bản ghi...")
        
        record_count = 0
        for _, row in filtered_df.iterrows():
            message = {
                "event_time": str(row['event_time']),
                "event_type": str(row['event_type']),
                "product_id": int(row['product_id']),
                "category_id": int(row['category_id']),
                "user_id": int(row['user_id'])
            }
            
            # Gửi tin nhắn JSON lên Kafka Topic
            producer.send(TOPIC_NAME, value=message)
            record_count += 1
            
            # GIẢ LẬP STREAMING: Đẩy đều đặn 30 bản ghi mỗi giây
            if record_count % 30 == 0:
                time.sleep(1)
                print(f"[Streamed] Đã đẩy tích lũy {record_count} sự kiện 'view' vào Kafka...")
                
        del filtered_df

    producer.flush()
    producer.close()
    print("[-] Hoàn thành tiến trình giả lập stream dữ liệu!")

if __name__ == "__main__":
    process_and_stream_data()

# HDFS Anomaly Detection Big Data

Đề tài: Xây dựng hệ thống Big Data giám sát và phát hiện bất thường log HDFS theo thời gian thực sử dụng Apache Kafka, Spark Structured Streaming và Spark MLlib.

## Kiến trúc hệ thống

Event_traces.csv  
→ Python Producer  
→ Apache Kafka  
→ Spark Structured Streaming  
→ Chuyển Event thành vector E1–E29  
→ Random Forest MLlib  
→ Normal / Anomaly  
→ Delta Lake  
→ Streamlit Dashboard

## Cấu trúc mã nguồn

- `Phan07_HuanLuyenRandomForest`: Huấn luyện mô hình Random Forest
- `Phan08_DanhGiaMoHinh`: Kiểm tra và đánh giá mô hình
- `Phan09_KafkaProducer`: Producer gửi log HDFS vào Kafka
- `Phan10_SparkStreamingPrediction`: Spark Structured Streaming dự đoán realtime
- `Phan12_Dashboard`: Dashboard giám sát realtime
- `Phan13_DemoEndToEnd`: Script khởi động toàn bộ hệ thống

## Kết quả mô hình

- Accuracy: 99.9843%
- AUC: 99.9966%
- Anomaly Precision: 99.6704%
- Anomaly Recall: 99.7900%
- Anomaly F1-score: 99.7301%

## Công nghệ sử dụng

- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- Spark MLlib
- Delta Lake
- Streamlit
- Python

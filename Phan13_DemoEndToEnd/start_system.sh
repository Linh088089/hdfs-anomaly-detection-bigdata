#!/bin/bash

PROJECT="/data/bigdata"
cd "$PROJECT" || exit 1

mkdir -p logs

echo "=================================================="
echo " HDFS ANOMALY DETECTION SYSTEM"
echo " RESET + START"
echo "=================================================="

# ==========================================================
# 1. DỪNG PROCESS CŨ
# ==========================================================

echo ""
echo "[1/9] Dung cac tien trinh cu..."

pkill -f "dashboard/dashboard.py" 2>/dev/null
pkill -f "hdfs_producer.py" 2>/dev/null
pkill -f "hdfs_streaming_predict.py" 2>/dev/null
pkill -f "kafka.Kafka" 2>/dev/null
pkill -f "QuorumPeerMain" 2>/dev/null

echo "Cho cac process cu tat hoan toan..."
sleep 8


# ==========================================================
# 2. RESET ZOOKEEPER TEMP DATA
# ==========================================================

echo ""
echo "[2/9] Reset ZooKeeper session cu..."

rm -rf /tmp/zookeeper

echo "ZooKeeper temp data da reset."


# ==========================================================
# 3. RESET KAFKA LOG DATA
# ==========================================================

echo ""
echo "[3/9] Reset Kafka local logs..."

rm -rf /tmp/kafka-logs

echo "Kafka local logs da reset."


# ==========================================================
# 4. RESET DELTA + CHECKPOINT
# ==========================================================

echo ""
echo "[4/9] Reset Delta Lake..."

rm -rf delta/hdfs_predictions
rm -rf delta/checkpoints/hdfs_predictions

echo "Delta Lake da reset."


# ==========================================================
# 5. START ZOOKEEPER
# ==========================================================

echo ""
echo "[5/9] Khoi dong ZooKeeper..."

nohup kafka/app/bin/zookeeper-server-start.sh \
kafka/app/config/zookeeper.properties \
> logs/zookeeper.log 2>&1 &

echo "Dang cho ZooKeeper mo port 2181..."

for i in {1..30}
do
    if ss -ltn | grep -q ":2181"; then
        echo "ZooKeeper da san sang."
        break
    fi

    sleep 1
done

if ! ss -ltn | grep -q ":2181"; then
    echo "LOI: ZooKeeper khong khoi dong duoc."
    echo "Xem log: logs/zookeeper.log"
    exit 1
fi


# ==========================================================
# 6. START KAFKA
# ==========================================================

echo ""
echo "[6/9] Khoi dong Kafka Broker..."

nohup kafka/app/bin/kafka-server-start.sh \
kafka/app/config/server.properties \
> logs/kafka.log 2>&1 &

echo "Dang cho Kafka mo port 9092..."

for i in {1..40}
do
    if ss -ltn | grep -q ":9092"; then
        echo "Kafka port 9092 da mo."
        break
    fi

    if grep -q "Exiting Kafka due to fatal exception" logs/kafka.log 2>/dev/null; then
        echo "LOI: Kafka bi dung khi khoi dong."
        echo "Xem log: logs/kafka.log"
        exit 1
    fi

    sleep 1
done

if ! ss -ltn | grep -q ":9092"; then
    echo "LOI: Kafka khong mo duoc port 9092."
    echo "Xem log: logs/kafka.log"
    exit 1
fi

echo "Dang kiem tra Kafka API..."

for i in {1..30}
do
    if kafka/app/bin/kafka-topics.sh \
        --list \
        --bootstrap-server localhost:9092 \
        >/dev/null 2>&1
    then
        echo "Kafka Broker da san sang."
        break
    fi

    sleep 1
done

if ! kafka/app/bin/kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092 \
    >/dev/null 2>&1
then
    echo "LOI: Kafka chua phan hoi API."
    echo "Xem log: logs/kafka.log"
    exit 1
fi


# ==========================================================
# 7. CREATE TOPIC
# ==========================================================

echo ""
echo "[7/9] Tao topic hdfs-logs..."

kafka/app/bin/kafka-topics.sh \
--create \
--topic hdfs-logs \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

if [ $? -ne 0 ]; then
    echo "LOI: Khong tao duoc topic hdfs-logs."
    exit 1
fi

echo "Topic hdfs-logs da tao."


# ==========================================================
# 8. START SPARK STREAMING
# ==========================================================

echo ""
echo "[8/9] Khoi dong Spark Structured Streaming..."

source /data/bigdata/project/venv/bin/activate

export PYSPARK_PYTHON=/data/bigdata/project/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/data/bigdata/project/venv/bin/python
export SETUPTOOLS_USE_DISTUTILS=local

nohup spark/app/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.3.2 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
streaming/hdfs_streaming_predict.py \
> logs/spark_streaming.log 2>&1 &

echo "Dang cho Spark Streaming khoi dong..."
sleep 15

if grep -q "Traceback" logs/spark_streaming.log 2>/dev/null; then
    echo "LOI: Spark Streaming bi loi."
    echo "Xem log: logs/spark_streaming.log"
    exit 1
fi

echo "Spark Structured Streaming da khoi dong."


# ==========================================================
# 9. START PRODUCER + DASHBOARD
# ==========================================================

echo ""
echo "[9/9] Khoi dong Producer va Dashboard..."

nohup project/venv/bin/python \
producer/hdfs_producer.py \
> logs/producer.log 2>&1 &

nohup project/venv/bin/streamlit run \
dashboard/dashboard.py \
--server.headless true \
> logs/dashboard.log 2>&1 &

sleep 5

echo ""
echo "=================================================="
echo " HE THONG DA KHOI DONG"
echo "=================================================="
echo "Producer    : 1 Block ID / 5 giay"
echo "Kafka topic : hdfs-logs"
echo "Delta       : delta/hdfs_predictions"
echo "Dashboard   : http://localhost:8501"
echo "=================================================="

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    udf,
    when,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import RandomForestClassificationModel


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("HDFSRealtimeAnomalyDetection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =========================================================
# KAFKA MESSAGE SCHEMA
# =========================================================

schema = StructType([
    StructField("BlockId", StringType(), True),
    StructField("Label", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Features", StringType(), True),
    StructField("TimeInterval", StringType(), True),
    StructField("Latency", StringType(), True)
])


# =========================================================
# CHUYỂN CHUỖI EVENT -> VECTOR E1-E29
# =========================================================

def events_to_vector(event_sequence):

    counts = [0.0] * 29

    if event_sequence is None:
        return Vectors.dense(counts)

    events = re.findall(
        r"E(\d+)",
        str(event_sequence)
    )

    for event_number in events:

        index = int(event_number) - 1

        if 0 <= index < 29:
            counts[index] += 1.0

    return Vectors.dense(counts)


events_to_vector_udf = udf(
    events_to_vector,
    VectorUDT()
)


# =========================================================
# ĐỌC STREAM TỪ KAFKA
# =========================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "hdfs-logs"
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# =========================================================
# GIẢI MÃ JSON
# =========================================================

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data"),

        col("partition").alias(
            "KafkaPartition"
        ),

        col("offset").alias(
            "KafkaOffset"
        ),

        col("timestamp").alias(
            "KafkaTimestamp"
        )
    )
    .select(
        "data.*",
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"
    )
)


# =========================================================
# ĐỔI TÊN FEATURES GỐC
# QUAN TRỌNG:
# tránh xung đột Features và features
# =========================================================

prepared_df = (
    parsed_df
    .withColumnRenamed(
        "Features",
        "EventSequence"
    )
    .withColumn(
        "features",
        events_to_vector_udf(
            col("EventSequence")
        )
    )
)


# =========================================================
# LOAD RANDOM FOREST MODEL
# =========================================================

model = (
    RandomForestClassificationModel
    .load(
        "models/random_forest_hdfs"
    )
)


# =========================================================
# DỰ ĐOÁN
# =========================================================

predictions = model.transform(
    prepared_df
)


# =========================================================
# NORMAL / ANOMALY
# =========================================================

result_df = (
    predictions
    .withColumn(
        "PredictionLabel",
        when(
            col("prediction") == 1.0,
            "Anomaly"
        ).otherwise(
            "Normal"
        )
    )
    .withColumn(
        "ProcessedAt",
        current_timestamp()
    )
)


# =========================================================
# OUTPUT CONSOLE
# =========================================================

console_query = (
    result_df
    .select(
        "BlockId",
        "EventSequence",
        "Label",
        "prediction",
        "PredictionLabel",
        "KafkaOffset",
        "KafkaTimestamp",
        "ProcessedAt"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option(
        "truncate",
        False
    )
    .start()
)


# =========================================================
# OUTPUT DELTA LAKE
# =========================================================

delta_output_df = (
    result_df
    .select(
        col("BlockId"),
        col("Type"),

        # đổi lại tên thành Features
        # để Dashboard tiếp tục dùng bình thường
        col("EventSequence").alias(
            "Features"
        ),

        col("Label"),
        col("prediction"),
        col("PredictionLabel"),
        col("KafkaPartition"),
        col("KafkaOffset"),
        col("KafkaTimestamp"),
        col("ProcessedAt")
    )
)


delta_query = (
    delta_output_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "delta/checkpoints/hdfs_predictions"
    )
    .start(
        "delta/hdfs_predictions"
    )
)


# =========================================================
# STATUS
# =========================================================

print("============================================")
print("HDFS REALTIME ANOMALY DETECTION")
print("============================================")
print("Kafka topic : hdfs-logs")
print("Delta path  : delta/hdfs_predictions")
print("Model       : models/random_forest_hdfs")
print("Trang thai  : DANG CHO DU LIEU...")
print("============================================")


spark.streams.awaitAnyTermination()

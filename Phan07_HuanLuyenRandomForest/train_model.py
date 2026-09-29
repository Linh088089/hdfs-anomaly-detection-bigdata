from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum as spark_sum
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator
)

# =========================================================
# 1. Khởi tạo Spark
# =========================================================
spark = SparkSession.builder \
    .appName("HDFS_Anomaly_Training") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================================================
# 2. Đọc dữ liệu
# =========================================================
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/raw/Event_occurrence_matrix.csv")

# =========================================================
# 3. Quy đổi nhãn
# Success -> 0.0 = Normal
# Fail    -> 1.0 = Anomaly
# =========================================================
df = df.withColumn(
    "label",
    when(col("Label") == "Fail", 1.0).otherwise(0.0)
)

# =========================================================
# 4. Tạo vector đặc trưng E1-E29
# =========================================================
feature_cols = [f"E{i}" for i in range(1, 30)]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

prepared_df = assembler.transform(df)

# =========================================================
# 5. Chia dữ liệu Train/Test
# =========================================================
train_df, test_df = prepared_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

train_count = train_df.count()
test_count = test_df.count()

print("\n==============================")
print("KET QUA CHIA DU LIEU")
print("==============================")
print("Train:", train_count)
print("Test :", test_count)
print("Tong :", train_count + test_count)
print("==============================")

# =========================================================
# 6. Huấn luyện Random Forest
# =========================================================
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=10,
    seed=42
)

print("\n==============================")
print("BAT DAU HUAN LUYEN RANDOM FOREST")
print("==============================")

model = rf.fit(train_df)

print("\n==============================")
print("HUAN LUYEN HOAN TAT")
print("==============================")

# =========================================================
# 7. Dự đoán trên tập Test
# =========================================================
predictions = model.transform(test_df)

print("\n==============================")
print("MOT SO KET QUA DU DOAN")
print("==============================")

predictions.select(
    "BlockId",
    "label",
    "prediction",
    "probability"
).show(10, truncate=False)

# =========================================================
# 8. Đánh giá tổng thể
# =========================================================
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

auc_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

accuracy = accuracy_evaluator.evaluate(predictions)
precision = precision_evaluator.evaluate(predictions)
recall = recall_evaluator.evaluate(predictions)
f1 = f1_evaluator.evaluate(predictions)
auc = auc_evaluator.evaluate(predictions)

print("\n==============================")
print("KET QUA DANH GIA MO HINH")
print("==============================")
print(f"Accuracy : {accuracy:.6f}")
print(f"Precision: {precision:.6f}")
print(f"Recall   : {recall:.6f}")
print(f"F1-score : {f1:.6f}")
print(f"AUC      : {auc:.6f}")
print("==============================")

# =========================================================
# 9. Confusion Matrix
# =========================================================
print("\n==============================")
print("CONFUSION MATRIX")
print("==============================")

predictions.groupBy(
    "label",
    "prediction"
).count().orderBy(
    "label",
    "prediction"
).show()

# =========================================================
# 10. Tính riêng chỉ số cho lớp Anomaly
# label = 1.0 là Anomaly
# =========================================================
counts = predictions.select(
    spark_sum(
        when(
            (col("label") == 1.0) & (col("prediction") == 1.0),
            1
        ).otherwise(0)
    ).alias("TP"),

    spark_sum(
        when(
            (col("label") == 0.0) & (col("prediction") == 1.0),
            1
        ).otherwise(0)
    ).alias("FP"),

    spark_sum(
        when(
            (col("label") == 1.0) & (col("prediction") == 0.0),
            1
        ).otherwise(0)
    ).alias("FN"),

    spark_sum(
        when(
            (col("label") == 0.0) & (col("prediction") == 0.0),
            1
        ).otherwise(0)
    ).alias("TN")
).collect()[0]

tp = counts["TP"]
fp = counts["FP"]
fn = counts["FN"]
tn = counts["TN"]

anomaly_precision = tp / (tp + fp) if (tp + fp) > 0 else 0
anomaly_recall = tp / (tp + fn) if (tp + fn) > 0 else 0

anomaly_f1 = (
    2 * anomaly_precision * anomaly_recall
    / (anomaly_precision + anomaly_recall)
    if (anomaly_precision + anomaly_recall) > 0
    else 0
)

print("\n==============================")
print("DANH GIA RIENG LOP ANOMALY")
print("==============================")
print("TP:", tp)
print("FP:", fp)
print("FN:", fn)
print("TN:", tn)
print(f"Precision Anomaly: {anomaly_precision:.6f}")
print(f"Recall Anomaly   : {anomaly_recall:.6f}")
print(f"F1 Anomaly       : {anomaly_f1:.6f}")
print("==============================")

# =========================================================
# 11. Lưu mô hình
# =========================================================
model_path = "models/random_forest_hdfs"

model.write() \
    .overwrite() \
    .save(model_path)

print("\n==============================")
print("LUU MO HINH THANH CONG")
print("==============================")
print("Duong dan:", model_path)
print("==============================")

# =========================================================
# 12. Dừng Spark
# =========================================================
spark.stop()

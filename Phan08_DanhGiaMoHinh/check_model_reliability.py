from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator
)


# =========================================================
# SPARK
# =========================================================

spark = (
    SparkSession.builder
    .appName("CheckHDFSModelReliability")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =========================================================
# LOAD DATA
# =========================================================

DATA_PATH = "data/raw/Event_occurrence_matrix.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
)


print("\n============================================")
print("1. KIEM TRA COT DU LIEU")
print("============================================")

print("Tat ca cot:")
print(df.columns)


# =========================================================
# LABEL
# =========================================================

df = df.withColumn(
    "label",
    when(
        col("Label") == "Fail",
        1.0
    ).otherwise(0.0)
)


# =========================================================
# FEATURE E1-E29
# =========================================================

feature_cols = [
    f"E{i}"
    for i in range(1, 30)
]

print("\nFeature dung de train:")
print(feature_cols)


# =========================================================
# KIEM TRA LEAKAGE
# =========================================================

print("\n============================================")
print("2. KIEM TRA DATA LEAKAGE")
print("============================================")

for forbidden_col in [
    "Label",
    "label",
    "Type",
    "BlockId"
]:

    if forbidden_col in feature_cols:
        print(
            "CANH BAO:",
            forbidden_col,
            "dang nam trong feature!"
        )
    else:
        print(
            "OK:",
            forbidden_col,
            "KHONG nam trong feature"
        )


# =========================================================
# VECTOR
# =========================================================

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

prepared_df = assembler.transform(df)


# =========================================================
# RANDOM SPLIT
# =========================================================

train_df, test_df = (
    prepared_df.randomSplit(
        [0.8, 0.2],
        seed=42
    )
)


train_count = train_df.count()
test_count = test_df.count()


print("\n============================================")
print("3. SO LUONG TRAIN / TEST")
print("============================================")

print("Train:", train_count)
print("Test :", test_count)
print(
    "Tong :",
    train_count + test_count
)


# =========================================================
# KIEM TRA TRUNG BLOCK ID
# =========================================================

print("\n============================================")
print("4. KIEM TRA TRUNG BLOCK ID")
print("============================================")

train_blocks = (
    train_df
    .select("BlockId")
    .distinct()
)

test_blocks = (
    test_df
    .select("BlockId")
    .distinct()
)

overlap_count = (
    train_blocks
    .join(
        test_blocks,
        on="BlockId",
        how="inner"
    )
    .count()
)

print(
    "So BlockId trung giua train va test:",
    overlap_count
)


if overlap_count == 0:

    print(
        "OK: Khong co BlockId trung giua train va test"
    )

else:

    print(
        "CANH BAO: Co BlockId trung giua train va test"
    )


# =========================================================
# PHAN BO NHAN
# =========================================================

print("\n============================================")
print("5. PHAN BO NHAN TRAIN")
print("============================================")

train_df.groupBy(
    "label"
).count().orderBy(
    "label"
).show()


print("\n============================================")
print("6. PHAN BO NHAN TEST")
print("============================================")

test_df.groupBy(
    "label"
).count().orderBy(
    "label"
).show()


# =========================================================
# TRAIN RANDOM FOREST
# =========================================================

print("\n============================================")
print("7. TRAIN RANDOM FOREST")
print("============================================")

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=10,
    seed=42
)

model = rf.fit(
    train_df
)


predictions = model.transform(
    test_df
)


# =========================================================
# METRICS
# =========================================================

accuracy_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

precision_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

f1_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

auc_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)


accuracy = accuracy_eval.evaluate(
    predictions
)

precision = precision_eval.evaluate(
    predictions
)

recall = recall_eval.evaluate(
    predictions
)

f1 = f1_eval.evaluate(
    predictions
)

auc = auc_eval.evaluate(
    predictions
)


print("\n============================================")
print("8. KET QUA MODEL")
print("============================================")

print(
    f"Accuracy          : {accuracy:.6f}"
)

print(
    f"WeightedPrecision : {precision:.6f}"
)

print(
    f"WeightedRecall    : {recall:.6f}"
)

print(
    f"WeightedF1        : {f1:.6f}"
)

print(
    f"AUC               : {auc:.6f}"
)


# =========================================================
# CONFUSION MATRIX
# =========================================================

print("\n============================================")
print("9. CONFUSION MATRIX")
print("============================================")

(
    predictions
    .groupBy(
        "label",
        "prediction"
    )
    .count()
    .orderBy(
        "label",
        "prediction"
    )
    .show()
)


# =========================================================
# DUPLICATE FEATURE PATTERN CHECK
# =========================================================

print("\n============================================")
print("10. KIEM TRA CAC VECTOR E1-E29 TRUNG NHAU")
print("============================================")

feature_group_df = (
    df.groupBy(
        *feature_cols
    )
    .count()
)

duplicate_pattern_count = (
    feature_group_df
    .filter(
        col("count") > 1
    )
    .count()
)

print(
    "So pattern E1-E29 bi lap lai:",
    duplicate_pattern_count
)


# =========================================================
# SAME FEATURE PATTERN WITH DIFFERENT LABEL
# =========================================================

print("\n============================================")
print("11. KIEM TRA CUNG PATTERN NHUNG KHAC LABEL")
print("============================================")

pattern_label_df = (
    df.groupBy(
        *feature_cols
    )
    .agg(
        {
            "label": "count"
        }
    )
)

# kiểm tra bằng cách group pattern + label trước
pattern_label_pairs = (
    df.groupBy(
        *feature_cols,
        "label"
    )
    .count()
)

ambiguous_patterns = (
    pattern_label_pairs
    .groupBy(
        *feature_cols
    )
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)

print(
    "So pattern E1-E29 co ca Normal va Anomaly:",
    ambiguous_patterns
)


print("\n============================================")
print("HOAN TAT KIEM TRA")
print("============================================")


spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Cloud paths
# ============================================================

LAMP_GCS_PATH = (
    "gs://mbta-fleet-operations-alu/"
    "raw/lamp_subway"
)


# ============================================================
# Spark session
#
# No .master("local[*]") here.
#
# Google Cloud Managed Spark provides the Spark cluster and
# workers for us.
# ============================================================

spark = (
    SparkSession.builder
    .appName("mbta-lamp-serverless-profile")
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


# ============================================================
# Read historical LAMP data directly from GCS
# ============================================================

lamp_df = (
    spark.read
    .parquet(
        LAMP_GCS_PATH
    )
)


# ============================================================
# Profile dataset
# ============================================================

print()
print("================================")
print("MBTA SERVERLESS SPARK PROFILE")
print("================================")


print()
print("Source:")
print(
    LAMP_GCS_PATH
)


print()
print("Schema:")

lamp_df.printSchema()


print()
print("Total rows:")

row_count = (
    lamp_df.count()
)

print(
    row_count
)


print()
print("Number of columns:")

print(
    len(
        lamp_df.columns
    )
)


print()
print("Service-date coverage:")

(
    lamp_df
    .groupBy(
        "service_date"
    )
    .agg(
        F.count("*").alias(
            "row_count"
        )
    )
    .orderBy(
        "service_date"
    )
    .show(
        100,
        truncate=False,
    )
)


print()
print("Sample rows:")

lamp_df.show(
    5,
    truncate=False,
)


print()
print("================================")
print("SERVERLESS SPARK PROFILE COMPLETE")
print("================================")


spark.stop()
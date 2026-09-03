from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

LAMP_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "lamp_subway"
)


# ============================================================
# Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("mbta-lamp-profile")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read historical LAMP Parquet data
#
# Spark reads all service_date=* partitions underneath the
# parent directory.
# ============================================================

lamp_df = (
    spark.read
    .parquet(
        str(LAMP_DIRECTORY)
    )
)


# ============================================================
# Basic profiling
# ============================================================

print()
print("================================")
print("MBTA LAMP SPARK PROFILE")
print("================================")

print()
print("Schema:")
lamp_df.printSchema()


print()
print("Total rows:")

row_count = lamp_df.count()

print(row_count)


print()
print("Number of columns:")

print(
    len(lamp_df.columns)
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
print("SPARK PROFILE COMPLETE")
print("================================")


spark.stop()
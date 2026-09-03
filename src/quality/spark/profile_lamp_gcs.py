from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Cloud configuration
# ============================================================

GCP_PROJECT = "mbta-fleet-operations-alu"

LAMP_GCS_PATH = (
    "gs://mbta-fleet-operations-alu/"
    "raw/lamp_subway"
)


# ============================================================
# Spark session
#
# Spark 4.2.0 is using Hadoop 3.5.0.
#
# Hadoop 3.5 includes native support for the gs:// filesystem,
# so we do not need to load a separate GCS connector JAR.
#
# Application Default Credentials come from:
#
#   gcloud auth application-default login
# ============================================================

spark = (
    SparkSession.builder
    .appName("mbta-lamp-gcs-profile")
    .master("local[*]")

    .config(
        "spark.hadoop.fs.gs.impl",
        "org.apache.hadoop.fs.gs.GoogleHadoopFileSystem",
    )

    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "org.apache.hadoop.fs.gs.Gs",
    )

    .config(
        "spark.hadoop.fs.gs.project.id",
        GCP_PROJECT,
    )

    .config(
        "spark.hadoop.fs.gs.auth.type",
        "APPLICATION_DEFAULT",
    )

    .getOrCreate()
)


spark.sparkContext.setLogLevel(
    "WARN"
)


# ============================================================
# Read LAMP directly from Google Cloud Storage
# ============================================================

lamp_df = (
    spark.read
    .parquet(
        LAMP_GCS_PATH
    )
)


# ============================================================
# Profile
# ============================================================

print()
print("================================")
print("MBTA LAMP GCS SPARK PROFILE")
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
print("GCS SPARK PROFILE COMPLETE")
print("================================")


spark.stop()
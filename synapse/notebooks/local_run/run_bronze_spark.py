"""
Runs the Bronze ingestion using PySpark + Delta Lake locally.
Set JAVA_HOME and HADOOP_HOME before running.
"""

import os

os.environ['JAVA_HOME']   = r'C:\Program Files\OpenLogic\jdk-11.0.23.9-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType
)
from delta.tables import DeltaTable

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data", "sample").replace("\\", "/")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "local", "bronze_spark").replace("\\", "/")

spark = SparkSession.builder \
    .appName("FIFA2026_Bronze_Spark") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.driver.memory", "2g") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} started")


# ── 1. Matches ───────────────────────────────────────────────────────────────
print("\n=== Bronze: Matches ===")

df_matches = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_DIR}/matches.csv") \
    .dropDuplicates(["date", "home_team", "away_team"]) \
    .withColumn("match_id",
        F.md5(F.concat_ws("_",
            F.col("date").cast(StringType()),
            F.col("home_team"),
            F.col("away_team")
        ))
    ) \
    .withColumn("total_goals", F.col("home_score") + F.col("away_score")) \
    .withColumn("result",
        F.when(F.col("home_score") > F.col("away_score"), "HOME_WIN")
         .when(F.col("home_score") < F.col("away_score"), "AWAY_WIN")
         .otherwise("DRAW")
    ) \
    .withColumn("_ingestion_date", F.current_date()) \
    .withColumn("_ingestion_ts",   F.current_timestamp())

matches_path = f"{OUTPUT_DIR}/matches"
df_matches.write.format("delta").mode("overwrite").save(matches_path)
print(f"  Written {df_matches.count():,} rows as Delta -> {matches_path}")
df_matches.groupBy("result").count().show()


# ── 2. Rankings ──────────────────────────────────────────────────────────────
print("\n=== Bronze: Rankings ===")

df_rankings = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_DIR}/rankings.csv") \
    .dropDuplicates(["country_abrv", "rank_date"]) \
    .withColumn("_ingestion_date", F.current_date())

rankings_path = f"{OUTPUT_DIR}/rankings"
df_rankings.write.format("delta").mode("overwrite").save(rankings_path)
print(f"  Written {df_rankings.count():,} rows as Delta -> {rankings_path}")


# ── 3. Players ───────────────────────────────────────────────────────────────
print("\n=== Bronze: Players ===")

df_players = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_DIR}/players.csv") \
    .dropDuplicates(["sofifa_id"]) \
    .withColumn("_ingestion_date", F.current_date())

players_path = f"{OUTPUT_DIR}/players"
df_players.write.format("delta").mode("overwrite").save(players_path)
print(f"  Written {df_players.count():,} rows as Delta -> {players_path}")


# ── 4. Show Delta table history ───────────────────────────────────────────────
print("\n=== Delta Table History (matches) ===")
dt = DeltaTable.forPath(spark, matches_path)
dt.history(3).select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

print("\nBronze Spark ingestion complete!")
spark.stop()

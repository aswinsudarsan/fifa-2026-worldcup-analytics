"""
Notebook: 05_synapse_link_live_scores
Purpose:  Read live match data from Cosmos DB via Synapse Link (analytical store)
          — zero ETL, near-real-time analytics on transactional data
Spark Pool: sparkfifa2026

Synapse Link eliminates the need to export data from Cosmos DB.
The analytical store is a column-oriented copy updated automatically.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("FIFA2026_SynapseLink_LiveScores") \
    .getOrCreate()

COSMOS_ACCOUNT  = "cosmos-fifa2026-dev"
COSMOS_DATABASE = "fifa2026"
STORAGE_ACCOUNT = "adlsfifa2026dev"
GOLD_BASE       = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ── Read from Cosmos DB analytical store via Synapse Link ─────────────────────
# No ETL pipeline needed — data syncs automatically from OLTP → analytical store
print("Reading live_matches from Cosmos DB analytical store ...")

df_live_matches = spark.read \
    .format("cosmos.olap") \
    .option("spark.synapse.linkedService", "ls_cosmosdb_fifa2026") \
    .option("spark.cosmos.container", "live_matches") \
    .load()

df_live_scores = spark.read \
    .format("cosmos.olap") \
    .option("spark.synapse.linkedService", "ls_cosmosdb_fifa2026") \
    .option("spark.cosmos.container", "live_scores") \
    .load()

df_player_events = spark.read \
    .format("cosmos.olap") \
    .option("spark.synapse.linkedService", "ls_cosmosdb_fifa2026") \
    .option("spark.cosmos.container", "player_events") \
    .load()

print(f"Live matches:   {df_live_matches.count():,}")
print(f"Live scores:    {df_live_scores.count():,}")
print(f"Player events:  {df_player_events.count():,}")

df_live_matches.printSchema()


# ── Enrich live scores with predictions ──────────────────────────────────────
print("\nEnriching live data with pre-match predictions ...")

df_predictions = spark.read.format("delta").load(f"{GOLD_BASE}/wc2026_group_predictions")

df_live_enriched = df_live_matches \
    .join(
        df_predictions.select("home_team", "away_team",
                              "predicted_outcome", "home_win_prob",
                              "draw_prob", "away_win_prob"),
        on=["home_team", "away_team"],
        how="left"
    ) \
    .withColumn("prediction_correct",
        F.when(
            (F.col("current_result") == F.col("predicted_outcome")) &
            F.col("match_status").isin(["FT", "AET", "PEN"]),
            True
        ).otherwise(None)
    )


# ── Goal events analysis ──────────────────────────────────────────────────────
df_goal_events = df_player_events \
    .filter(F.col("event_type") == "GOAL") \
    .groupBy("match_id", "team", "player_name") \
    .agg(
        F.count("event_id").alias("goals"),
        F.collect_list("minute").alias("goal_minutes")
    ) \
    .orderBy(F.col("goals").desc())

print("\nTop scorers in live matches:")
df_goal_events.show(10, truncate=False)


# ── Write live results to Gold Delta (incremental merge) ─────────────────────
from delta.tables import DeltaTable

live_results_path = f"{GOLD_BASE}/live_match_results"

live_write = df_live_enriched.select(
    "match_id", "home_team", "away_team",
    "home_score", "away_score", "match_status",
    "current_result", "predicted_outcome",
    "prediction_correct", "home_win_prob",
    F.current_timestamp().alias("last_updated")
)

if DeltaTable.isDeltaTable(spark, live_results_path):
    dt = DeltaTable.forPath(spark, live_results_path)
    dt.alias("existing").merge(
        live_write.alias("updates"),
        "existing.match_id = updates.match_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    live_write.write.format("delta").save(live_results_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.live_match_results
    USING DELTA LOCATION '{live_results_path}'
""")

print("\nSynapse Link live scores notebook complete!")
print("Data flows: Cosmos DB (OLTP) → Analytical Store → Spark → Delta Lake → Power BI")

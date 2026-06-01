"""
Notebook: 03_gold_aggregation
Layer: Gold (Silver → Aggregated, report-ready tables)
Spark Pool: sparkfifa2026

Purpose:
  Build denormalized Gold tables that Power BI and the Synapse Dedicated
  SQL Pool consume directly — zero additional transformation required.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("FIFA2026_Gold_Aggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

STORAGE_ACCOUNT = "adlsfifa2026dev"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

df_matches = spark.read.format("delta").load(f"{SILVER_BASE}/matches_enriched")
df_form    = spark.read.format("delta").load(f"{SILVER_BASE}/team_form")
df_squads  = spark.read.format("delta").load(f"{SILVER_BASE}/squad_stats")


# ── Gold 1: dim_team ──────────────────────────────────────────────────────────
print("Building gold.dim_team ...")

# All distinct teams with their latest rankings and squad stats
latest_ranking = spark.read.format("delta").load(f"{STORAGE_ACCOUNT.replace('adls', 'abfss://bronze@adls')}/rankings") \
    if False else \
    spark.read.format("delta").load(f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/rankings")

latest_per_team = latest_ranking \
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("country_full").orderBy(F.col("rank_date").desc())
    )) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

dim_team = latest_per_team \
    .join(df_squads.withColumnRenamed("team", "country_full"),
          on="country_full", how="left") \
    .select(
        F.col("country_full").alias("team_name"),
        F.col("country_abrv").alias("team_code"),
        F.col("confederation"),
        F.col("rank").alias("current_fifa_rank"),
        F.col("total_points").alias("current_fifa_points"),
        F.col("squad_avg_overall"),
        F.col("squad_avg_age"),
        F.col("squad_avg_value"),
        F.col("best_player_rating"),
        F.col("squad_size"),
    )

dim_team.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_team")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_team USING DELTA LOCATION '{GOLD_BASE}/dim_team'")
print(f"  dim_team rows: {dim_team.count():,}")


# ── Gold 2: fact_matches ──────────────────────────────────────────────────────
print("Building gold.fact_matches ...")

fact_matches = df_matches.select(
    "match_id",
    "date",
    F.year("date").alias("match_year"),
    F.month("date").alias("match_month"),
    "tournament",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "result",
    "total_goals",
    "neutral",
    "city",
    "country",
    "home_fifa_rank",
    "away_fifa_rank",
    "rank_diff",
    "points_diff",
    "home_form_pts_last5",
    "away_form_pts_last5",
    "home_form_pts_last10",
    "away_form_pts_last10",
    "home_avg_gf_last5",
    "away_avg_gf_last5",
    "h2h_home_win_rate",
    "h2h_total",
    "home_squad_avg_overall",
    "away_squad_avg_overall",
)

fact_matches.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("match_year") \
    .save(f"{GOLD_BASE}/fact_matches")

spark.sql(f"CREATE TABLE IF NOT EXISTS gold.fact_matches USING DELTA LOCATION '{GOLD_BASE}/fact_matches'")
print(f"  fact_matches rows: {fact_matches.count():,}")


# ── Gold 3: agg_team_performance ─────────────────────────────────────────────
print("Building gold.agg_team_performance ...")

# Pivot: one row per team with all-time and WC-only aggregates
def team_stats(df, alias_prefix: str = ""):
    home = df.groupBy(F.col("home_team").alias("team")).agg(
        F.sum(F.when(F.col("result") == "HOME_WIN", 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("result") == "DRAW", 1).otherwise(0)).alias("draws"),
        F.sum(F.when(F.col("result") == "AWAY_WIN", 1).otherwise(0)).alias("losses"),
        F.sum("home_score").alias("goals_for"),
        F.sum("away_score").alias("goals_against"),
        F.count("match_id").alias("matches_played"),
        F.avg("total_goals").alias("avg_goals_per_game"),
    )
    away = df.groupBy(F.col("away_team").alias("team")).agg(
        F.sum(F.when(F.col("result") == "AWAY_WIN", 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("result") == "DRAW", 1).otherwise(0)).alias("draws"),
        F.sum(F.when(F.col("result") == "HOME_WIN", 1).otherwise(0)).alias("losses"),
        F.sum("away_score").alias("goals_for"),
        F.sum("home_score").alias("goals_against"),
        F.count("match_id").alias("matches_played"),
        F.avg("total_goals").alias("avg_goals_per_game"),
    )
    combined = home.union(away).groupBy("team").agg(
        F.sum("wins").alias(f"{alias_prefix}wins"),
        F.sum("draws").alias(f"{alias_prefix}draws"),
        F.sum("losses").alias(f"{alias_prefix}losses"),
        F.sum("goals_for").alias(f"{alias_prefix}goals_for"),
        F.sum("goals_against").alias(f"{alias_prefix}goals_against"),
        F.sum("matches_played").alias(f"{alias_prefix}matches"),
    ) \
    .withColumn(f"{alias_prefix}win_pct",
        F.col(f"{alias_prefix}wins") / F.col(f"{alias_prefix}matches")) \
    .withColumn(f"{alias_prefix}goal_diff",
        F.col(f"{alias_prefix}goals_for") - F.col(f"{alias_prefix}goals_against"))
    return combined

all_time = team_stats(fact_matches, "alltime_")
wc_only  = team_stats(
    fact_matches.filter(F.col("tournament") == "FIFA World Cup"),
    "wc_"
)

agg_team = all_time.join(wc_only, on="team", how="left") \
    .join(dim_team.select("team_name", "current_fifa_rank", "squad_avg_overall", "confederation")
                  .withColumnRenamed("team_name", "team"),
          on="team", how="left")

agg_team.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/agg_team_performance")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.agg_team_performance USING DELTA LOCATION '{GOLD_BASE}/agg_team_performance'")
print(f"  agg_team_performance rows: {agg_team.count():,}")


# ── Gold 4: agg_tournament_summary ───────────────────────────────────────────
print("Building gold.agg_tournament_summary ...")

agg_tournament = fact_matches \
    .groupBy("tournament", "match_year") \
    .agg(
        F.count("match_id").alias("total_matches"),
        F.sum("total_goals").alias("total_goals"),
        F.avg("total_goals").alias("avg_goals_per_match"),
        F.sum(F.when(F.col("result") == "HOME_WIN", 1).otherwise(0)).alias("home_wins"),
        F.sum(F.when(F.col("result") == "DRAW", 1).otherwise(0)).alias("draws"),
        F.sum(F.when(F.col("result") == "AWAY_WIN", 1).otherwise(0)).alias("away_wins"),
    ) \
    .withColumn("home_win_pct", F.col("home_wins") / F.col("total_matches")) \
    .orderBy("match_year", "tournament")

agg_tournament.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/agg_tournament_summary")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.agg_tournament_summary USING DELTA LOCATION '{GOLD_BASE}/agg_tournament_summary'")


# ── Gold 5: wc2026_qualified_teams ───────────────────────────────────────────
print("Building gold.wc2026_qualified_teams ...")

# 48 qualified teams for FIFA 2026 (hard-coded for now, replace with API data)
wc2026_teams = [
    ("Argentina",    "ARG", "CONMEBOL", 1),
    ("France",       "FRA", "UEFA",     2),
    ("England",      "ENG", "UEFA",     4),
    ("Brazil",       "BRA", "CONMEBOL", 5),
    ("Portugal",     "POR", "UEFA",     6),
    ("Spain",        "ESP", "UEFA",     8),
    ("Netherlands",  "NED", "UEFA",     7),
    ("Germany",      "GER", "UEFA",    14),
    ("Belgium",      "BEL", "UEFA",     3),
    ("Uruguay",      "URU", "CONMEBOL",15),
    ("Colombia",     "COL", "CONMEBOL",11),
    ("Mexico",       "MEX", "CONCACAF",13),
    ("United States","USA", "CONCACAF",16),
    ("Canada",       "CAN", "CONCACAF",48),
    ("Morocco",      "MAR", "CAF",     12),
    ("Senegal",      "SEN", "CAF",     17),
    ("Japan",        "JPN", "AFC",     18),
    ("South Korea",  "KOR", "AFC",     23),
    ("Australia",    "AUS", "AFC",     22),
    ("Saudi Arabia", "KSA", "AFC",     56),
    ("Iran",         "IRN", "AFC",     21),
    ("Ecuador",      "ECU", "CONMEBOL",43),
    ("Chile",        "CHI", "CONMEBOL",34),
    ("Peru",         "PER", "CONMEBOL",21),
    ("Venezuela",    "VEN", "CONMEBOL",26),
    ("Bolivia",      "BOL", "CONMEBOL",85),
    ("Paraguay",     "PAR", "CONMEBOL",61),
    ("Poland",       "POL", "UEFA",    24),
    ("Croatia",      "CRO", "UEFA",    10),
    ("Denmark",      "DEN", "UEFA",    19),
    ("Austria",      "AUT", "UEFA",    25),
    ("Switzerland",  "SUI", "UEFA",    20),
    ("Turkey",       "TUR", "UEFA",    40),
    ("Ukraine",      "UKR", "UEFA",    22),
    ("Serbia",       "SRB", "UEFA",    33),
    ("Albania",      "ALB", "UEFA",    65),
    ("Hungary",      "HUN", "UEFA",    29),
    ("Slovakia",     "SVK", "UEFA",    48),
    ("Nigeria",      "NGA", "CAF",     39),
    ("Egypt",        "EGY", "CAF",     34),
    ("Cameroon",     "CMR", "CAF",     43),
    ("Ghana",        "GHA", "CAF",     60),
    ("Tunisia",      "TUN", "CAF",     31),
    ("South Africa", "RSA", "CAF",     62),
    ("Côte d'Ivoire","CIV", "CAF",     41),
    ("Democratic Republic of Congo","COD","CAF",73),
    ("Algeria",      "ALG", "CAF",     35),
    ("Panama",       "PAN", "CONCACAF",81),
]

wc2026_df = spark.createDataFrame(
    wc2026_teams,
    ["team_name", "team_code", "confederation", "current_rank"]
).join(
    agg_team.select(
        "team", "alltime_wins", "alltime_matches", "alltime_win_pct",
        "wc_wins", "wc_matches", "squad_avg_overall"
    ).withColumnRenamed("team", "team_name"),
    on="team_name",
    how="left"
)

wc2026_df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/wc2026_qualified_teams")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.wc2026_qualified_teams USING DELTA LOCATION '{GOLD_BASE}/wc2026_qualified_teams'")
print(f"  wc2026_qualified_teams rows: {wc2026_df.count():,}")


print("\nGold aggregation complete!")
spark.sql("SHOW TABLES IN gold").show(truncate=False)

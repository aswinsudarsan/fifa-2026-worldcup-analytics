"""
Notebook: 04_ml_prediction_model
Purpose: Train a match outcome prediction model using PySpark MLlib GBTClassifier
         Predict FIFA 2026 World Cup results for all 48 teams / 104 matches
Spark Pool: sparkfifa2026
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, StandardScaler,
    Imputer, OneHotEncoder
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import itertools
import json

spark = SparkSession.builder \
    .appName("FIFA2026_Prediction_Model") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

STORAGE_ACCOUNT = "adlsfifa2026dev"
SILVER_BASE  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE    = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
MODELS_BASE  = f"abfss://ml-models@{STORAGE_ACCOUNT}.dfs.core.windows.net"


# ── 1. Feature Engineering ────────────────────────────────────────────────────
print("=== Step 1: Load and prepare features ===")

df_silver = spark.read.format("delta").load(f"{SILVER_BASE}/matches_enriched")

# Use only major tournament matches for better signal
df_train_raw = df_silver.filter(
    F.col("tournament").isin([
        "FIFA World Cup",
        "FIFA World Cup qualification",
        "UEFA Euro",
        "Copa América",
        "AFC Asian Cup",
        "Africa Cup of Nations",
        "FIFA Confederations Cup",
        "CONCACAF Gold Cup",
    ])
) \
.filter(F.col("date") >= "1994-01-01") \
.filter(F.col("home_score").isNotNull() & F.col("away_score").isNotNull())

# Target: 0=HOME_WIN, 1=DRAW, 2=AWAY_WIN
df_labeled = df_train_raw.withColumn(
    "label",
    F.when(F.col("result") == "HOME_WIN", 0.0)
     .when(F.col("result") == "DRAW",     1.0)
     .otherwise(2.0)
)

# Tournament stage encoding
df_labeled = df_labeled.withColumn(
    "is_knockout",
    F.when(F.col("tournament") == "FIFA World Cup", 1).otherwise(0)
)

feature_cols = [
    "rank_diff",
    "points_diff",
    "home_fifa_rank",
    "away_fifa_rank",
    "home_form_pts_last5",
    "away_form_pts_last5",
    "home_form_pts_last10",
    "away_form_pts_last10",
    "home_avg_gf_last5",
    "away_avg_gf_last5",
    "home_avg_ga_last5",
    "away_avg_ga_last5",
    "h2h_home_win_rate",
    "h2h_total",
    "home_squad_avg_overall",
    "away_squad_avg_overall",
    "is_knockout",
    "neutral",
]

# Cast booleans
df_labeled = df_labeled \
    .withColumn("neutral", F.col("neutral").cast(DoubleType())) \
    .withColumn("is_knockout", F.col("is_knockout").cast(DoubleType()))

for c in feature_cols:
    df_labeled = df_labeled.withColumn(c, F.col(c).cast(DoubleType()))

df_features = df_labeled.select(feature_cols + ["label", "match_id", "date",
                                                 "home_team", "away_team"])
df_features = df_features.na.fill(0.5, ["h2h_home_win_rate"])

print(f"Training dataset size: {df_features.count():,}")
df_features.groupBy("label").count().show()


# ── 2. Train/Test Split ───────────────────────────────────────────────────────
print("\n=== Step 2: Train/test split (80/20 temporal) ===")

split_date = "2020-01-01"
df_train = df_features.filter(F.col("date") < split_date)
df_test  = df_features.filter(F.col("date") >= split_date)

print(f"Train: {df_train.count():,} | Test: {df_test.count():,}")


# ── 3. ML Pipeline ───────────────────────────────────────────────────────────
print("\n=== Step 3: Build ML pipeline ===")

imputer = Imputer(
    inputCols=feature_cols,
    outputCols=[f"{c}_imputed" for c in feature_cols],
    strategy="mean"
)

assembler = VectorAssembler(
    inputCols=[f"{c}_imputed" for c in feature_cols],
    outputCol="features_raw"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    subsamplingRate=0.8,
    seed=42,
)

pipeline = Pipeline(stages=[imputer, assembler, scaler, gbt])


# ── 4. Hyperparameter Tuning via CrossValidator ───────────────────────────────
print("\n=== Step 4: Cross-validation (3-fold) ===")

param_grid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [3, 5]) \
    .addGrid(gbt.maxIter,  [50, 100]) \
    .build()

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42,
    parallelism=2
)

cv_model = cv.fit(df_train)
best_model = cv_model.bestModel

print(f"Best params: maxDepth={best_model.stages[-1].getMaxDepth()}, "
      f"maxIter={best_model.stages[-1].getMaxIter()}")


# ── 5. Evaluate on Test Set ───────────────────────────────────────────────────
print("\n=== Step 5: Model evaluation ===")

df_predictions = best_model.transform(df_test)

accuracy  = evaluator.evaluate(df_predictions)
f1_eval   = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
precision = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
recall    = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")

print(f"Accuracy:  {accuracy:.4f}")
print(f"F1:        {f1_eval.evaluate(df_predictions):.4f}")
print(f"Precision: {precision.evaluate(df_predictions):.4f}")
print(f"Recall:    {recall.evaluate(df_predictions):.4f}")

# Confusion matrix
df_predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

# Feature importances
gbt_stage = best_model.stages[-1]
importances = gbt_stage.featureImportances
feature_importance_df = spark.createDataFrame(
    sorted(zip(feature_cols, importances.toArray()), key=lambda x: -x[1]),
    ["feature", "importance"]
)
print("\nTop 10 feature importances:")
feature_importance_df.show(10)


# ── 6. Save Model ─────────────────────────────────────────────────────────────
print("\n=== Step 6: Save model to ADLS ===")

model_path = f"{MODELS_BASE}/gbt_match_predictor_v1"
best_model.save(model_path)
print(f"Model saved to: {model_path}")

# Save metadata
metadata = {
    "model_type": "GBTClassifier",
    "accuracy": round(accuracy, 4),
    "training_date": "2026-01-01",
    "features": feature_cols,
    "label_mapping": {"0": "HOME_WIN", "1": "DRAW", "2": "AWAY_WIN"},
    "training_samples": df_train.count(),
    "test_samples": df_test.count(),
}
spark.createDataFrame([metadata]).write \
    .mode("overwrite") \
    .json(f"{MODELS_BASE}/gbt_match_predictor_v1_metadata")


# ── 7. Predict 2026 World Cup Group Stage ────────────────────────────────────
print("\n=== Step 7: Simulate FIFA 2026 Group Stage ===")

from pyspark.ml import PipelineModel

model = PipelineModel.load(model_path)

# Define 2026 World Cup groups (A-L, 4 teams each)
groups = {
    "A": ["United States", "Panama", "Bolivia", ""],
    "B": ["Argentina", "Chile", "Peru", ""],
    "C": ["Brazil", "Colombia", "Venezuela", ""],
    "D": ["France", "Belgium", "Serbia", ""],
    "E": ["Spain", "Turkey", "Albania", ""],
    "F": ["England", "Denmark", "Slovakia", ""],
    "G": ["Portugal", "Poland", "Croatia", ""],
    "H": ["Netherlands", "Austria", "Ukraine", ""],
    "I": ["Germany", "Switzerland", "Hungary", ""],
    "J": ["Morocco", "Senegal", "Algeria", ""],
    "K": ["Nigeria", "Egypt", "Cameroon", ""],
    "L": ["Japan", "South Korea", "Iran", ""],
}

# Generate all round-robin fixtures within each group
fixtures = []
for group, teams in groups.items():
    teams_filtered = [t for t in teams if t]
    for home, away in itertools.combinations(teams_filtered, 2):
        fixtures.append((group, home, away, "FIFA World Cup", 0.0, 0))

schema = "group STRING, home_team STRING, away_team STRING, tournament STRING, neutral DOUBLE, is_knockout INT"
df_fixtures = spark.createDataFrame(fixtures, schema)

# Join with gold dim_team to get ranking features
df_dim = spark.read.format("delta").load(f"{GOLD_BASE}/dim_team") \
    .select("team_name", "current_fifa_rank", "squad_avg_overall")

df_fix_enriched = df_fixtures \
    .join(df_dim.withColumnRenamed("team_name", "home_team")
                .withColumnRenamed("current_fifa_rank", "home_fifa_rank")
                .withColumnRenamed("squad_avg_overall", "home_squad_avg_overall"),
          on="home_team", how="left") \
    .join(df_dim.withColumnRenamed("team_name", "away_team")
                .withColumnRenamed("current_fifa_rank", "away_fifa_rank")
                .withColumnRenamed("squad_avg_overall", "away_squad_avg_overall"),
          on="away_team", how="left") \
    .withColumn("rank_diff",  F.col("home_fifa_rank") - F.col("away_fifa_rank")) \
    .withColumn("points_diff", F.lit(0.0)) \
    .withColumn("home_form_pts_last5",  F.lit(1.5)) \
    .withColumn("away_form_pts_last5",  F.lit(1.5)) \
    .withColumn("home_form_pts_last10", F.lit(1.5)) \
    .withColumn("away_form_pts_last10", F.lit(1.5)) \
    .withColumn("home_avg_gf_last5",    F.lit(1.3)) \
    .withColumn("away_avg_gf_last5",    F.lit(1.3)) \
    .withColumn("home_avg_ga_last5",    F.lit(1.0)) \
    .withColumn("away_avg_ga_last5",    F.lit(1.0)) \
    .withColumn("h2h_home_win_rate",    F.lit(0.5)) \
    .withColumn("h2h_total",            F.lit(5.0)) \
    .withColumn("neutral",              F.lit(0.0)) \
    .withColumn("is_knockout",          F.lit(0.0)) \
    .withColumn("label",                F.lit(0.0))   # placeholder

for c in feature_cols:
    df_fix_enriched = df_fix_enriched.withColumn(c, F.col(c).cast(DoubleType()))

df_group_predictions = model.transform(df_fix_enriched) \
    .select(
        "group",
        "home_team",
        "away_team",
        "prediction",
        "probability",
    ) \
    .withColumn("predicted_outcome",
        F.when(F.col("prediction") == 0, "HOME_WIN")
         .when(F.col("prediction") == 1, "DRAW")
         .otherwise("AWAY_WIN")
    ) \
    .withColumn("home_win_prob",  F.round(F.col("probability").getItem(0), 4)) \
    .withColumn("draw_prob",      F.round(F.col("probability").getItem(1), 4)) \
    .withColumn("away_win_prob",  F.round(F.col("probability").getItem(2), 4))

print("Group stage predictions (sample):")
df_group_predictions.select(
    "group", "home_team", "away_team", "predicted_outcome",
    "home_win_prob", "draw_prob", "away_win_prob"
).orderBy("group").show(30, truncate=False)

# ── 8. Save Predictions to Gold ───────────────────────────────────────────────
df_group_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_BASE}/wc2026_group_predictions")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.wc2026_group_predictions
    USING DELTA LOCATION '{GOLD_BASE}/wc2026_group_predictions'
""")

print("\nPrediction model complete! Results saved to gold.wc2026_group_predictions")

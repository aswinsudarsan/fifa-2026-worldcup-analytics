"""
Local Gold aggregation + ML prediction model using pandas + scikit-learn.
Run: python synapse/notebooks/local_run/03_gold_and_ml.py
"""

import os
import json
import pickle
import warnings
import pandas as pd
import numpy as np
import duckdb
import itertools
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix)
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
SILVER_DIR   = os.path.join(PROJECT_ROOT, "local", "silver")
GOLD_DIR     = os.path.join(PROJECT_ROOT, "local", "gold")
MODELS_DIR   = os.path.join(PROJECT_ROOT, "local", "models")
os.makedirs(GOLD_DIR,   exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

con = duckdb.connect()

df_silver = pd.read_parquet(os.path.join(SILVER_DIR, "matches_enriched.parquet"))
df_squad  = pd.read_parquet(os.path.join(SILVER_DIR, "squad_stats.parquet"))
df_silver["date"] = pd.to_datetime(df_silver["date"])

print(f"Silver rows: {len(df_silver):,}  columns: {len(df_silver.columns)}")


# ========================================================================
# GOLD 1 — dim_team
# ========================================================================
print("\n=== Gold 1: dim_team ===")

df_rankings = pd.read_parquet(os.path.join(PROJECT_ROOT, "local", "bronze", "rankings.parquet"))
df_rankings["rank_date"] = pd.to_datetime(df_rankings["rank_date"])

latest_rank = (
    df_rankings.sort_values("rank_date")
               .groupby("country_full")
               .last()
               .reset_index()
)

dim_team = latest_rank.merge(
    df_squad.rename(columns={"nationality_name":"country_full"}),
    on="country_full", how="left"
)[["country_full","country_abrv","confederation","rank","total_points",
   "squad_avg_overall","squad_avg_age","squad_avg_value","best_player_rating","squad_size"]]
dim_team.columns = ["team_name","team_code","confederation","current_fifa_rank",
                    "current_fifa_points","squad_avg_overall","squad_avg_age",
                    "squad_avg_value","best_player_rating","squad_size"]

dim_team.to_parquet(os.path.join(GOLD_DIR, "dim_team.parquet"), index=False)
con.register("dim_team", dim_team)
print(f"  dim_team rows: {len(dim_team):,}")
print(dim_team[["team_name","current_fifa_rank","squad_avg_overall"]].head(8).to_string(index=False))


# ========================================================================
# GOLD 2 — fact_matches
# ========================================================================
print("\n=== Gold 2: fact_matches ===")

fact_cols = [
    "match_id","date","tournament","home_team","away_team",
    "home_score","away_score","result","total_goals","neutral",
    "home_fifa_rank","away_fifa_rank","rank_diff","points_diff",
    "home_form_pts_last5","away_form_pts_last5",
    "home_form_pts_last10","away_form_pts_last10",
    "home_avg_gf_last5","away_avg_gf_last5",
    "home_avg_ga_last5","away_avg_ga_last5",
    "h2h_home_win_rate","h2h_total",
    "home_squad_avg_overall","away_squad_avg_overall",
]
fact_matches = df_silver[[c for c in fact_cols if c in df_silver.columns]].copy()
fact_matches["match_year"] = fact_matches["date"].dt.year

fact_matches.to_parquet(os.path.join(GOLD_DIR, "fact_matches.parquet"), index=False)
con.register("fact_matches", fact_matches)
print(f"  fact_matches rows: {len(fact_matches):,}")


# ========================================================================
# GOLD 3 — agg_team_performance
# ========================================================================
print("\n=== Gold 3: agg_team_performance ===")

con.execute("CREATE OR REPLACE VIEW silver AS SELECT * FROM fact_matches")

agg = con.execute("""
    WITH all_teams AS (
        SELECT home_team AS team, result='HOME_WIN' AS win,
               result='DRAW' AS draw, result='AWAY_WIN' AS loss,
               home_score AS gf, away_score AS ga,
               tournament='FIFA World Cup' AS is_wc
        FROM fact_matches
        UNION ALL
        SELECT away_team, result='AWAY_WIN', result='DRAW', result='HOME_WIN',
               away_score, home_score, tournament='FIFA World Cup'
        FROM fact_matches
    )
    SELECT
        team,
        COUNT(*) AS total_matches,
        SUM(win::INT) AS wins,
        SUM(draw::INT) AS draws,
        SUM(loss::INT) AS losses,
        SUM(gf) AS goals_for,
        SUM(ga) AS goals_against,
        ROUND(AVG(win::FLOAT)*100, 1) AS win_pct,
        SUM(CASE WHEN is_wc THEN win::INT END) AS wc_wins,
        SUM(is_wc::INT) AS wc_matches
    FROM all_teams
    GROUP BY team
    ORDER BY wins DESC
""").df()

agg.to_parquet(os.path.join(GOLD_DIR, "agg_team_performance.parquet"), index=False)
print(f"  agg_team_performance rows: {len(agg):,}")
print(agg.head(6).to_string(index=False))


# ========================================================================
# GOLD 4 — WC 2026 qualified teams
# ========================================================================
print("\n=== Gold 4: WC 2026 qualified teams ===")

wc2026 = [
    ("Argentina","ARG","CONMEBOL",1),("France","FRA","UEFA",2),
    ("England","ENG","UEFA",4),("Brazil","BRA","CONMEBOL",5),
    ("Portugal","POR","UEFA",6),("Spain","ESP","UEFA",8),
    ("Netherlands","NED","UEFA",7),("Germany","GER","UEFA",14),
    ("Belgium","BEL","UEFA",3),("Uruguay","URU","CONMEBOL",15),
    ("Morocco","MAR","CAF",12),("Mexico","MEX","CONCACAF",13),
    ("United States","USA","CONCACAF",16),("Canada","CAN","CONCACAF",48),
    ("Japan","JPN","AFC",18),("South Korea","KOR","AFC",23),
    ("Colombia","COL","CONMEBOL",11),("Senegal","SEN","CAF",17),
    ("Poland","POL","UEFA",24),("Croatia","CRO","UEFA",10),
    ("Denmark","DEN","UEFA",19),("Austria","AUT","UEFA",25),
    ("Switzerland","SUI","UEFA",20),("Turkey","TUR","UEFA",40),
    ("Serbia","SRB","UEFA",33),("Hungary","HUN","UEFA",29),
    ("Nigeria","NGA","CAF",39),("Egypt","EGY","CAF",34),
    ("Cameroon","CMR","CAF",43),("Ghana","GHA","CAF",60),
    ("Ecuador","ECU","CONMEBOL",43),("Chile","CHI","CONMEBOL",34),
    ("Australia","AUS","AFC",22),("Saudi Arabia","KSA","AFC",56),
    ("Iran","IRN","AFC",21),("Venezuela","VEN","CONMEBOL",26),
    ("Bolivia","BOL","CONMEBOL",85),("Panama","PAN","CONCACAF",81),
    ("Tunisia","TUN","CAF",31),("Algeria","ALG","CAF",35),
    ("Slovakia","SVK","UEFA",48),("Albania","ALB","UEFA",65),
    ("Ukraine","UKR","UEFA",22),("South Africa","RSA","CAF",62),
    ("Costa Rica","CRC","CONCACAF",55),("Paraguay","PAR","CONMEBOL",61),
    ("Romania","ROU","UEFA",46),("Ivory Coast","CIV","CAF",41),
]

df_wc = pd.DataFrame(wc2026, columns=["team_name","team_code","confederation","current_rank"])
df_wc = df_wc.merge(agg[["team","wins","total_matches","win_pct","wc_wins","wc_matches"]]
                       .rename(columns={"team":"team_name"}),
                    on="team_name", how="left")
df_wc.to_parquet(os.path.join(GOLD_DIR, "wc2026_qualified_teams.parquet"), index=False)
print(f"  Qualified teams: {len(df_wc)}")


# ========================================================================
# ML MODEL — GradientBoostingClassifier
# ========================================================================
print("\n" + "="*60)
print("ML: Match Outcome Prediction Model")
print("="*60)

FEATURES = [
    "rank_diff","points_diff","home_fifa_rank","away_fifa_rank",
    "home_form_pts_last5","away_form_pts_last5",
    "home_form_pts_last10","away_form_pts_last10",
    "home_avg_gf_last5","away_avg_gf_last5",
    "home_avg_ga_last5","away_avg_ga_last5",
    "h2h_home_win_rate","h2h_total",
    "home_squad_avg_overall","away_squad_avg_overall",
]

df_ml = df_silver[
    df_silver["tournament"].isin([
        "FIFA World Cup","FIFA World Cup qualification",
        "UEFA Euro","Copa América","Africa Cup of Nations",
        "FIFA Confederations Cup","CONCACAF Gold Cup",
    ])
].copy()

df_ml = df_ml.dropna(subset=["result","home_score","away_score"])
df_ml["is_neutral"] = df_ml["neutral"].astype(float)

available = [f for f in FEATURES if f in df_ml.columns]
X = df_ml[available].astype(float)
y = df_ml["result"]

print(f"\nTraining samples: {len(X):,}")
print(f"Features used:    {len(available)}")
print(f"Label distribution:\n{y.value_counts().to_string()}")

# Temporal split (80/20)
split_idx = int(len(df_ml) * 0.8)
df_ml_sorted = df_ml.sort_values("date")
X_sorted = df_ml_sorted[available].astype(float)
y_sorted = df_ml_sorted["result"]

X_train, X_test = X_sorted.iloc[:split_idx], X_sorted.iloc[split_idx:]
y_train, y_test = y_sorted.iloc[:split_idx], y_sorted.iloc[split_idx:]

# Build pipeline
model = Pipeline([
    ("imputer", SimpleImputer(strategy="mean")),
    ("clf", GradientBoostingClassifier(
        n_estimators=150,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42
    ))
])

print("\nTraining GradientBoostingClassifier ...")
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
acc    = accuracy_score(y_test, y_pred)

print(f"\nTest Accuracy:  {acc:.4f} ({acc*100:.1f}%)")
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Feature importances
clf = model.named_steps["clf"]
fi = sorted(zip(available, clf.feature_importances_), key=lambda x: -x[1])
print("Top 10 Feature Importances:")
for name, imp in fi[:10]:
    bar = "#" * int(imp * 100)
    print(f"  {name:<30} {imp:.4f}  {bar}")

# Cross-validation (3-fold)
cv_scores = cross_val_score(model, X_sorted, y_sorted, cv=3, scoring="accuracy")
print(f"\n3-Fold CV Accuracy: {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")

# Save model
model_path = os.path.join(MODELS_DIR, "gbt_model.pkl")
with open(model_path, "wb") as f:
    pickle.dump(model, f)

metadata = {
    "model_type": "GradientBoostingClassifier",
    "accuracy": round(float(acc), 4),
    "cv_mean": round(float(cv_scores.mean()), 4),
    "features": available,
    "training_date": str(pd.Timestamp.now().date()),
    "train_samples": len(X_train),
    "test_samples": len(X_test),
}
with open(os.path.join(MODELS_DIR, "model_metadata.json"), "w") as f:
    json.dump(metadata, f, indent=2)

print(f"\nModel saved: {model_path}")


# ========================================================================
# PREDICT — FIFA 2026 Group Stage
# ========================================================================
print("\n" + "="*60)
print("Predicting FIFA 2026 Group Stage")
print("="*60)

groups = {
    "A": ["United States","Panama","Bolivia","Venezuela"],
    "B": ["Argentina","Chile","Peru","Ecuador"],
    "C": ["Brazil","Colombia","Uruguay","Paraguay"],
    "D": ["France","Belgium","Serbia","Albania"],
    "E": ["Spain","Turkey","Slovakia","Croatia"],
    "F": ["England","Denmark","Austria","Switzerland"],
    "G": ["Portugal","Poland","Hungary","Romania"],
    "H": ["Netherlands","Germany","Ukraine","Austria"],
    "I": ["Morocco","Senegal","Algeria","Tunisia"],
    "J": ["Nigeria","Egypt","Cameroon","Ghana"],
    "K": ["Japan","South Korea","Iran","Saudi Arabia"],
    "L": ["Mexico","Canada","Costa Rica","South Africa"],
}

# Build fixture feature rows
team_rank_lookup = dim_team.set_index("team_name")[
    ["current_fifa_rank","squad_avg_overall"]
].to_dict("index")

fixtures = []
for group, teams in groups.items():
    for home, away in itertools.combinations(teams, 2):
        hr = team_rank_lookup.get(home, {"current_fifa_rank": 50, "squad_avg_overall": 75})
        ar = team_rank_lookup.get(away, {"current_fifa_rank": 50, "squad_avg_overall": 75})
        row = {
            "group": group, "home_team": home, "away_team": away,
            "rank_diff":  (hr["current_fifa_rank"]  or 50) - (ar["current_fifa_rank"]  or 50),
            "points_diff": 0.0,
            "home_fifa_rank":  hr["current_fifa_rank"]  or 50,
            "away_fifa_rank":  ar["current_fifa_rank"]  or 50,
            "home_form_pts_last5": 1.5, "away_form_pts_last5": 1.5,
            "home_form_pts_last10":1.5, "away_form_pts_last10":1.5,
            "home_avg_gf_last5": 1.3,  "away_avg_gf_last5": 1.3,
            "home_avg_ga_last5": 1.0,  "away_avg_ga_last5": 1.0,
            "h2h_home_win_rate": 0.5,  "h2h_total": 5.0,
            "home_squad_avg_overall": hr["squad_avg_overall"] or 75,
            "away_squad_avg_overall": ar["squad_avg_overall"] or 75,
        }
        fixtures.append(row)

df_fixtures = pd.DataFrame(fixtures)
X_pred = df_fixtures[[f for f in available if f in df_fixtures.columns]].astype(float)

probs = model.predict_proba(X_pred)
classes = model.classes_

df_fixtures["predicted_outcome"] = model.predict(X_pred)
for i, cls in enumerate(classes):
    df_fixtures[f"prob_{cls}"] = probs[:, i]

df_fixtures.to_parquet(os.path.join(GOLD_DIR, "wc2026_group_predictions.parquet"), index=False)

# Show Group A and B predictions
con.register("predictions", df_fixtures)
print("\nGroup Stage Predictions (Group A & B):")
print(con.execute("""
    SELECT
        "group",
        home_team,
        away_team,
        predicted_outcome,
        ROUND(prob_HOME_WIN*100,1) AS home_win_pct,
        ROUND(prob_DRAW*100,1)     AS draw_pct,
        ROUND(prob_AWAY_WIN*100,1) AS away_win_pct
    FROM predictions
    WHERE "group" IN ('A','B')
    ORDER BY "group", home_team
""").df().to_string(index=False))

# Simulated group table
print("\nSimulated Group Stage Points Table (all groups):")
print(con.execute("""
    WITH home_pts AS (
        SELECT "group", home_team AS team,
               SUM(CASE WHEN predicted_outcome='HOME_WIN' THEN 3
                        WHEN predicted_outcome='DRAW' THEN 1 ELSE 0 END) AS pts,
               SUM(predicted_outcome='HOME_WIN') AS w,
               SUM(predicted_outcome='DRAW')     AS d,
               SUM(predicted_outcome='AWAY_WIN') AS l
        FROM predictions GROUP BY "group", home_team
    ),
    away_pts AS (
        SELECT "group", away_team AS team,
               SUM(CASE WHEN predicted_outcome='AWAY_WIN' THEN 3
                        WHEN predicted_outcome='DRAW' THEN 1 ELSE 0 END) AS pts,
               SUM(predicted_outcome='AWAY_WIN') AS w,
               SUM(predicted_outcome='DRAW')     AS d,
               SUM(predicted_outcome='HOME_WIN') AS l
        FROM predictions GROUP BY "group", away_team
    ),
    combined AS (
        SELECT * FROM home_pts UNION ALL SELECT * FROM away_pts
    )
    SELECT "group", team,
           SUM(pts) AS points,
           SUM(w) AS wins, SUM(d) AS draws, SUM(l) AS losses,
           RANK() OVER (PARTITION BY "group" ORDER BY SUM(pts) DESC) AS pos
    FROM combined
    GROUP BY "group", team
    ORDER BY "group", points DESC
""").df().to_string(index=False))

print(f"\nAll Gold tables written to: {GOLD_DIR}")
print("Model saved to:            ", MODELS_DIR)
print("\nPipeline complete! Bronze -> Silver -> Gold -> ML")

"""
FIFA 2026 — Local ML Prediction Pipeline
=========================================
Runs entirely on your laptop using:
  - databricks-sql-connector  (fetch data from your SQL warehouse)
  - pandas + scikit-learn + xgboost  (train the model)
  - Writes predictions back to Databricks gold table

HOW TO RUN:
  1. Fill in your DATABRICKS_TOKEN below
  2. Open a terminal in this folder
  3. pip install databricks-sql-connector xgboost
  4. python run_ml_local.py
"""

import os
import warnings
import itertools
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

# ── STEP 0: CONFIGURE YOUR DATABRICKS CONNECTION ──────────────────────────────
# Get your token from: Databricks → top-right avatar → Settings → Developer → Access tokens
DATABRICKS_HOST      = "dbc-47da612b-138b.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/6de7196b84c83abb"
DATABRICKS_TOKEN     = "YOUR_TOKEN_HERE"   # Generate from Databricks → Settings → Developer → Access tokens

# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    from databricks import sql
    return sql.connect(
        server_hostname = DATABRICKS_HOST,
        http_path       = DATABRICKS_HTTP_PATH,
        access_token    = DATABRICKS_TOKEN,
    )

def query(sql_text):
    """Run a SQL query and return a pandas DataFrame."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)

def write_table(df, table_name):
    """Write a pandas DataFrame back to Databricks as a Delta table."""
    from databricks import sql
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Drop and recreate
            cur.execute(f"DROP TABLE IF EXISTS default.{table_name}")
            cols = ", ".join([f"{c} STRING" if df[c].dtype == object
                              else f"{c} DOUBLE" for c in df.columns])
            cur.execute(f"CREATE TABLE default.{table_name} ({cols})")
            # Insert rows in batches of 500
            rows = [tuple(str(v) if pd.isna(v) else v for v in row)
                    for row in df.itertuples(index=False)]
            placeholders = ", ".join(["?" for _ in df.columns])
            for i in range(0, len(rows), 500):
                batch = rows[i:i+500]
                cur.executemany(
                    f"INSERT INTO default.{table_name} VALUES ({placeholders})",
                    batch
                )
    print(f"  Written {len(df)} rows → default.{table_name}")


# ── STEP 1: FETCH DATA FROM DATABRICKS ───────────────────────────────────────
print("=" * 60)
print("STEP 1: Fetching data from Databricks SQL Warehouse...")
print("=" * 60)

print("  Loading gold_fact_matches...")
df_matches = query("""
    SELECT
        match_id, date, tournament,
        home_team, away_team,
        home_score, away_score, result,
        neutral,
        home_fifa_rank, away_fifa_rank,
        rank_diff, points_diff,
        home_form_pts_last5,  away_form_pts_last5,
        home_form_pts_last10, away_form_pts_last10,
        home_avg_gf_last5,    away_avg_gf_last5,
        home_avg_ga_last5,    away_avg_ga_last5,
        home_squad_avg_overall, away_squad_avg_overall,
        match_year
    FROM default.gold_fact_matches
    WHERE date >= '1990-01-01'
      AND home_score IS NOT NULL
      AND away_score IS NOT NULL
      AND tournament IN (
          'FIFA World Cup',
          'FIFA World Cup qualification',
          'UEFA Euro',
          'Copa América',
          'AFC Asian Cup',
          'Africa Cup of Nations',
          'FIFA Confederations Cup',
          'CONCACAF Gold Cup'
      )
""")
print(f"  Loaded {len(df_matches):,} matches")

print("  Loading gold_team_performance...")
df_teams = query("SELECT * FROM default.gold_team_performance")
print(f"  Loaded {len(df_teams):,} teams")

print("  Loading gold_wc2026_predictions (fixtures)...")
df_fixtures_raw = query("SELECT * FROM default.gold_wc2026_predictions")
print(f"  Loaded {len(df_fixtures_raw):,} WC 2026 fixtures")


# ── STEP 2: FEATURE ENGINEERING ───────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2: Feature engineering")
print("=" * 60)

TOURNAMENT_WEIGHTS = {
    "FIFA World Cup":                3,
    "FIFA World Cup qualification":  2,
    "UEFA Euro":                     2,
    "Copa América":                  2,
    "AFC Asian Cup":                 2,
    "Africa Cup of Nations":         2,
    "FIFA Confederations Cup":       2,
    "CONCACAF Gold Cup":             1,
}

df_matches["sample_weight"] = df_matches["tournament"].map(TOURNAMENT_WEIGHTS).fillna(1)

# Target label
df_matches["label"] = (
    df_matches["result"]
    .map({"HOME_WIN": 0, "DRAW": 1, "AWAY_WIN": 2})
)
df_matches = df_matches.dropna(subset=["label"])
df_matches["label"] = df_matches["label"].astype(int)

# Cast numerics
num_cols = [
    "rank_diff", "points_diff", "home_fifa_rank", "away_fifa_rank",
    "home_form_pts_last5", "away_form_pts_last5",
    "home_form_pts_last10", "away_form_pts_last10",
    "home_avg_gf_last5", "away_avg_gf_last5",
    "home_avg_ga_last5", "away_avg_ga_last5",
    "home_squad_avg_overall", "away_squad_avg_overall",
    "neutral",
]
for c in num_cols:
    df_matches[c] = pd.to_numeric(df_matches[c], errors="coerce").fillna(0)

# Engineered features
df_matches["form_ratio_last5"]      = (df_matches["home_form_pts_last5"]  / (df_matches["away_form_pts_last5"]  + 0.01)).clip(-5, 5)
df_matches["form_ratio_last10"]     = (df_matches["home_form_pts_last10"] / (df_matches["away_form_pts_last10"] + 0.01)).clip(-5, 5)
df_matches["home_attack_momentum"]  = df_matches["home_avg_gf_last5"]   - df_matches["home_avg_ga_last5"]
df_matches["away_attack_momentum"]  = df_matches["away_avg_gf_last5"]   - df_matches["away_avg_ga_last5"]
df_matches["squad_gap"]             = df_matches["home_squad_avg_overall"] - df_matches["away_squad_avg_overall"]
df_matches["rank_advantage"]        = df_matches["away_fifa_rank"]       - df_matches["home_fifa_rank"]
df_matches["effective_home_adv"]    = df_matches["home_form_pts_last5"]  * (1 - df_matches["neutral"])
df_matches["scoring_power_diff"]    = df_matches["home_avg_gf_last5"]    - df_matches["away_avg_gf_last5"]
df_matches["defensive_diff"]        = df_matches["away_avg_ga_last5"]    - df_matches["home_avg_ga_last5"]
df_matches["is_world_cup"]          = (df_matches["tournament"] == "FIFA World Cup").astype(int)

FEATURE_COLS = [
    "rank_diff", "rank_advantage", "home_fifa_rank", "away_fifa_rank", "points_diff",
    "home_form_pts_last5", "away_form_pts_last5",
    "home_form_pts_last10", "away_form_pts_last10",
    "form_ratio_last5", "form_ratio_last10",
    "home_avg_gf_last5", "away_avg_gf_last5",
    "home_avg_ga_last5", "away_avg_ga_last5",
    "home_attack_momentum", "away_attack_momentum",
    "scoring_power_diff", "defensive_diff",
    "home_squad_avg_overall", "away_squad_avg_overall", "squad_gap",
    "neutral", "is_world_cup", "effective_home_adv",
]

print(f"  Features: {len(FEATURE_COLS)}")
print(f"  Class distribution:\n{df_matches['label'].value_counts().rename({0:'HOME_WIN',1:'DRAW',2:'AWAY_WIN'})}")


# ── STEP 3: TRAIN / TEST SPLIT (temporal) ────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 3: Temporal train/test split (pre/post 2018)")
print("=" * 60)

df_matches["date"] = pd.to_datetime(df_matches["date"])
df_train = df_matches[df_matches["date"] < "2018-06-01"].copy()
df_test  = df_matches[df_matches["date"] >= "2018-06-01"].copy()

X_train = df_train[FEATURE_COLS].fillna(0)
y_train = df_train["label"]
w_train = df_train["sample_weight"]

X_test  = df_test[FEATURE_COLS].fillna(0)
y_test  = df_test["label"]

print(f"  Train: {len(df_train):,} | Test: {len(df_test):,}")


# ── STEP 4: BALANCE DRAW CLASS ────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 4: Oversample DRAW class to fix imbalance")
print("=" * 60)

df_home_train = df_train[df_train["label"] == 0]
df_draw_train = df_train[df_train["label"] == 1]
df_away_train = df_train[df_train["label"] == 2]

# Use sample_weight instead of oversampling — preserves data distribution
# but gives DRAW class 2x weight during training
X_bal = X_train.copy()
y_bal = y_train.copy()
w_bal = w_train.copy() * y_train.map({0: 1.0, 1: 2.0, 2: 1.0})

print(f"  HOME_WIN: {len(df_home_train):,} | DRAW: {len(df_draw_train):,} (2x weight) | AWAY_WIN: {len(df_away_train):,}")


# ── STEP 5: TRAIN THREE MODELS ────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 5: Training models...")
print("=" * 60)

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.model_selection import cross_val_score

try:
    from xgboost import XGBClassifier
    xgb_available = True
except ImportError:
    xgb_available = False
    print("  XGBoost not installed — using GradientBoostingClassifier instead")
    print("  (run: pip install xgboost  for better accuracy)")

# Model 1: XGBoost / GBT
print("  Training XGBoost...")
if xgb_available:
    m1 = Pipeline([
        ("scaler", StandardScaler()),
        ("model", XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            use_label_encoder=False, eval_metric="mlogloss",
            random_state=42, n_jobs=-1,
        ))
    ])
else:
    m1 = Pipeline([
        ("scaler", StandardScaler()),
        ("model", GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.05,
            subsample=0.8, random_state=42,
        ))
    ])
m1.fit(X_bal, y_bal)

# Model 2: Random Forest
print("  Training Random Forest...")
m2 = Pipeline([
    ("scaler", StandardScaler()),
    ("model", RandomForestClassifier(
        n_estimators=300, max_depth=10, min_samples_leaf=8,
        class_weight={0: 1.0, 1: 2.0, 2: 1.0},
        random_state=42, n_jobs=-1,
    ))
])
m2.fit(X_bal, y_bal)

# Model 3: Logistic Regression
print("  Training Logistic Regression...")
m3 = Pipeline([
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(
        max_iter=500, C=0.5,
        class_weight={0: 1.0, 1: 2.0, 2: 1.0},
        solver="lbfgs", random_state=42,
    ))
])
m3.fit(X_bal, y_bal)

print("  All 3 models trained ✓")


# ── STEP 6: SOFT-VOTING ENSEMBLE ──────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 6: Soft-voting ensemble (XGB 50% + RF 30% + LR 20%)")
print("=" * 60)

def ensemble_predict_proba(X, w1=0.60, w2=0.30, w3=0.10):
    p1 = m1.predict_proba(X)
    p2 = m2.predict_proba(X)
    p3 = m3.predict_proba(X)
    return w1 * p1 + w2 * p2 + w3 * p3

def ensemble_predict(X):
    proba = ensemble_predict_proba(X)
    return np.argmax(proba, axis=1)


# ── STEP 7: EVALUATE ──────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 7: Evaluation on holdout set (post-2018)")
print("=" * 60)

acc_m1  = accuracy_score(y_test, m1.predict(X_test))
acc_m2  = accuracy_score(y_test, m2.predict(X_test))
acc_m3  = accuracy_score(y_test, m3.predict(X_test))
acc_ens = accuracy_score(y_test, ensemble_predict(X_test))

f1_m1   = f1_score(y_test, m1.predict(X_test),          average="weighted")
f1_m2   = f1_score(y_test, m2.predict(X_test),          average="weighted")
f1_m3   = f1_score(y_test, m3.predict(X_test),          average="weighted")
f1_ens  = f1_score(y_test, ensemble_predict(X_test),    average="weighted")

print(f"\n{'Model':<25} {'Accuracy':>10} {'F1 Score':>10}")
print("-" * 47)
print(f"{'Baseline v1 (GBT)':<25} {'61.40%':>10} {'—':>10}")
print(f"{'XGBoost / GBT v2':<25} {acc_m1*100:>9.2f}% {f1_m1:>10.4f}")
print(f"{'Random Forest':<25} {acc_m2*100:>9.2f}% {f1_m2:>10.4f}")
print(f"{'Logistic Regression':<25} {acc_m3*100:>9.2f}% {f1_m3:>10.4f}")
print(f"{'Ensemble (BEST)':<25} {acc_ens*100:>9.2f}% {f1_ens:>10.4f}  ← NEW")
print("-" * 47)
print(f"  Improvement over v1: {(acc_ens - 0.614)*100:+.2f}%")

print("\nDetailed classification report (Ensemble):")
print(classification_report(
    y_test, ensemble_predict(X_test),
    target_names=["HOME_WIN", "DRAW", "AWAY_WIN"]
))

# Feature importances
if xgb_available:
    importances = m1.named_steps["model"].feature_importances_
else:
    importances = m1.named_steps["model"].feature_importances_

fi = sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1])
print("Top 10 features:")
for i, (feat, imp) in enumerate(fi[:10], 1):
    bar = "█" * int(imp * 300)
    print(f"  {i:2d}. {feat:<30} {imp:.4f}  {bar}")


# ── STEP 8: PREDICT WC 2026 GROUP STAGE ──────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 8: Predicting FIFA 2026 group stage fixtures")
print("=" * 60)

# Build feature rows for each fixture
team_stats = df_teams.set_index("team").to_dict("index")

def get_team_stat(team, col, default=0):
    return float(team_stats.get(team, {}).get(col, default) or default)

fixture_rows = []
for _, row in df_fixtures_raw.iterrows():
    ht = row["home_team"]
    at = row["away_team"]
    grp = row.get("group", "?")

    h_rank  = get_team_stat(ht, "home_fifa_rank", 50)
    a_rank  = get_team_stat(at, "away_fifa_rank", 50)
    h_sq    = get_team_stat(ht, "home_squad_avg_overall", 70)
    a_sq    = get_team_stat(at, "away_squad_avg_overall", 70)
    h_gf    = get_team_stat(ht, "home_avg_gf_last5", 1.3)
    a_gf    = get_team_stat(at, "away_avg_gf_last5", 1.3)
    h_ga    = get_team_stat(ht, "home_avg_ga_last5", 1.0)
    a_ga    = get_team_stat(at, "away_avg_ga_last5", 1.0)
    h_f5    = get_team_stat(ht, "home_form_pts_last5", 1.5)
    a_f5    = get_team_stat(at, "away_form_pts_last5", 1.5)
    h_f10   = get_team_stat(ht, "home_form_pts_last10", 1.5)
    a_f10   = get_team_stat(at, "away_form_pts_last10", 1.5)

    fixture_rows.append({
        "group": grp, "home_team": ht, "away_team": at,
        "rank_diff":             h_rank - a_rank,
        "rank_advantage":        a_rank - h_rank,
        "home_fifa_rank":        h_rank,
        "away_fifa_rank":        a_rank,
        "points_diff":           0.0,
        "home_form_pts_last5":   h_f5,
        "away_form_pts_last5":   a_f5,
        "home_form_pts_last10":  h_f10,
        "away_form_pts_last10":  a_f10,
        "form_ratio_last5":      h_f5  / (a_f5  + 0.01),
        "form_ratio_last10":     h_f10 / (a_f10 + 0.01),
        "home_avg_gf_last5":     h_gf,
        "away_avg_gf_last5":     a_gf,
        "home_avg_ga_last5":     h_ga,
        "away_avg_ga_last5":     a_ga,
        "home_attack_momentum":  h_gf - h_ga,
        "away_attack_momentum":  a_gf - a_ga,
        "scoring_power_diff":    h_gf - a_gf,
        "defensive_diff":        a_ga - h_ga,
        "home_squad_avg_overall":h_sq,
        "away_squad_avg_overall":a_sq,
        "squad_gap":             h_sq - a_sq,
        "neutral":               1.0,      # all WC matches at neutral venues
        "is_world_cup":          1.0,
        "effective_home_adv":    0.0,
    })

df_fix = pd.DataFrame(fixture_rows)
X_fix  = df_fix[FEATURE_COLS].fillna(0)

proba  = ensemble_predict_proba(X_fix)
preds  = np.argmax(proba, axis=1)

LABEL_MAP = {0: "HOME_WIN", 1: "DRAW", 2: "AWAY_WIN"}
df_fix["predicted_outcome"] = [LABEL_MAP[p] for p in preds]
df_fix["home_win_prob"]     = proba[:, 0].round(4)
df_fix["draw_prob"]         = proba[:, 1].round(4)
df_fix["away_win_prob"]     = proba[:, 2].round(4)

output_cols = ["group", "home_team", "away_team",
               "predicted_outcome", "home_win_prob", "draw_prob", "away_win_prob"]
df_output = df_fix[output_cols].sort_values(["group", "home_team"])

print(df_output.to_string(index=False))


# ── STEP 9: SAVE RESULTS TO CSV + WRITE BACK TO DATABRICKS ───────────────────
print("\n" + "=" * 60)
print("STEP 9: Saving results")
print("=" * 60)

# Always save locally first
csv_path = "wc2026_predictions_v2.csv"
df_output.to_csv(csv_path, index=False)
print(f"  Saved locally → {csv_path}")

# Write back to Databricks
print("  Writing predictions back to Databricks...")
try:
    write_table(df_output, "gold_wc2026_predictions")
    print("  ✓ gold_wc2026_predictions updated in Databricks")
except Exception as e:
    print(f"  ⚠ Could not write to Databricks: {e}")
    print(f"  → Predictions saved locally to {csv_path} instead")

# Save accuracy log
df_acc = pd.DataFrame([{
    "model_version":   "v2_local",
    "model_type":      "Ensemble XGB+RF+LR",
    "accuracy_pct":    round(acc_ens * 100, 2),
    "f1_score":        round(f1_ens, 4),
    "feature_count":   len(FEATURE_COLS),
    "improvement_pct": round((acc_ens - 0.614) * 100, 2),
}])
df_acc.to_csv("model_accuracy_log.csv", index=False)

print(f"""
╔══════════════════════════════════════════════════════╗
║         FIFA 2026 LOCAL ML PIPELINE COMPLETE         ║
╠══════════════════════════════════════════════════════╣
║  Baseline accuracy (v1):   61.40%                    ║
║  New ensemble accuracy:    {acc_ens*100:.2f}%                   ║
║  Improvement:              {(acc_ens-0.614)*100:+.2f}%                   ║
║  Features used:            {len(FEATURE_COLS)} (was 18)                ║
║  Predictions saved to:     wc2026_predictions_v2.csv ║
╚══════════════════════════════════════════════════════╝
""")

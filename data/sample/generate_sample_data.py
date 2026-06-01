"""
Script: generate_sample_data.py
Purpose: Generate realistic sample CSV files for local testing
         (when you don't have Kaggle API access)
Run: python data/sample/generate_sample_data.py
"""

import csv
import random
import os
from datetime import date, timedelta

random.seed(42)
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

TEAMS = [
    ("Argentina", "ARG", "CONMEBOL", 1),
    ("France",    "FRA", "UEFA",     2),
    ("England",   "ENG", "UEFA",     4),
    ("Brazil",    "BRA", "CONMEBOL", 5),
    ("Portugal",  "POR", "UEFA",     6),
    ("Spain",     "ESP", "UEFA",     8),
    ("Germany",   "GER", "UEFA",    14),
    ("Morocco",   "MAR", "CAF",     12),
    ("Japan",     "JPN", "AFC",     18),
    ("Mexico",    "MEX", "CONCACAF",13),
    ("USA",       "USA", "CONCACAF",16),
    ("Senegal",   "SEN", "CAF",     17),
]

TOURNAMENTS = [
    "FIFA World Cup",
    "FIFA World Cup qualification",
    "UEFA Euro",
    "Copa América",
    "Friendly",
]


def random_date(start_year=2000, end_year=2024):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


# ── matches.csv ───────────────────────────────────────────────────────────────
print("Generating matches.csv ...")
matches_path = os.path.join(OUTPUT_DIR, "matches.csv")
with open(matches_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["date", "home_team", "away_team", "home_score",
                     "away_score", "tournament", "city", "country", "neutral"])
    for _ in range(2000):
        home, away = random.sample([t[0] for t in TEAMS], 2)
        hs = random.choices([0,1,2,3,4,5], weights=[20,30,25,15,7,3])[0]
        as_ = random.choices([0,1,2,3,4,5], weights=[20,30,25,15,7,3])[0]
        writer.writerow([
            random_date(),
            home, away, hs, as_,
            random.choice(TOURNAMENTS),
            "City", "Country",
            random.choice([True, False])
        ])
print(f"  Written: {matches_path}")


# ── rankings.csv ─────────────────────────────────────────────────────────────
print("Generating rankings.csv ...")
rankings_path = os.path.join(OUTPUT_DIR, "rankings.csv")
with open(rankings_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "rank", "country_full", "country_abrv", "total_points",
        "previous_points", "rank_change", "cur_year_avg",
        "cur_year_avg_weighted", "last_year_avg",
        "last_year_avg_weighted", "two_year_ago_avg",
        "three_year_ago_avg", "confederation", "rank_date"
    ])
    # Generate monthly rankings from 2000 to 2026
    current = date(2000, 1, 1)
    end = date(2026, 6, 1)
    while current < end:
        teams_shuffled = TEAMS.copy()
        random.shuffle(teams_shuffled)
        for i, (name, abrv, conf, base_rank) in enumerate(teams_shuffled):
            rank = base_rank + random.randint(-3, 3)
            points = 1800 - rank * 30 + random.uniform(-50, 50)
            writer.writerow([
                rank, name, abrv,
                round(points, 2), round(points - random.uniform(-20, 20), 2),
                random.randint(-5, 5),
                round(random.uniform(0.8, 1.5), 4),
                round(random.uniform(0.8, 1.5), 4),
                round(random.uniform(0.7, 1.4), 4),
                round(random.uniform(0.7, 1.4), 4),
                round(random.uniform(0.6, 1.3), 4),
                round(random.uniform(0.5, 1.2), 4),
                conf,
                current.isoformat()
            ])
        # Move to next month
        month = current.month + 1 if current.month < 12 else 1
        year  = current.year + (1 if current.month == 12 else 0)
        current = date(year, month, 1)
print(f"  Written: {rankings_path}")


# ── players.csv ───────────────────────────────────────────────────────────────
print("Generating players.csv ...")
players_path = os.path.join(OUTPUT_DIR, "players.csv")
first_names = ["Lionel", "Kylian", "Cristiano", "Erling", "Vinicius",
               "Pedri", "Jude", "Lamine", "Phil", "Marcus", "Rodri"]
last_names  = ["Messi", "Mbappé", "Ronaldo", "Haaland", "Jr",
               "Barça", "Bellingham", "Yamal", "Foden", "Rashford", "Rodri"]

with open(players_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "sofifa_id", "player_url", "short_name", "long_name",
        "player_positions", "overall", "potential", "value_eur",
        "wage_eur", "age", "dob", "nationality_name", "nationality_id", "club_name"
    ])
    pid = 100000
    for i in range(500):
        team = random.choice(TEAMS)
        overall = random.randint(65, 95)
        age = random.randint(18, 36)
        dob = date(2026 - age, random.randint(1, 12), random.randint(1, 28))
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        writer.writerow([
            pid + i,
            f"https://sofifa.com/player/{pid + i}",
            fn,
            f"{fn} {ln}",
            random.choice(["GK","CB","LB","RB","CDM","CM","CAM","LW","RW","ST"]),
            overall,
            min(99, overall + random.randint(0, 8)),
            round(random.uniform(500000, 180000000), 0),
            round(random.uniform(5000, 350000), 0),
            age,
            dob.isoformat(),
            team[0],
            random.randint(1, 200),
            f"Club {random.randint(1, 50)}"
        ])
print(f"  Written: {players_path}")

print("\nSample data generation complete!")
print(f"Files in: {OUTPUT_DIR}")

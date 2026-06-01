# FIFA 2026 World Cup Analytics & Prediction Platform

An end-to-end Azure data engineering project that ingests historical FIFA match data, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), trains a match outcome prediction model using PySpark MLlib, and serves insights through Power BI dashboards.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  ┌───────────────┐ │
│  │ FIFA API /   │  │ Kaggle CSV   │  │ Cosmos DB      │  │ REST APIs     │ │
│  │ Historical   │  │ Datasets     │  │ (Live Scores)  │  │ (Rankings)    │ │
│  └──────┬───────┘  └──────┬───────┘  └───────┬────────┘  └──────┬────────┘ │
└─────────┼─────────────────┼──────────────────┼──────────────────┼──────────┘
          │                 │                  │                  │
          ▼                 ▼                  │                  ▼
┌─────────────────────────────────────────┐   │   ┌──────────────────────────┐
│       AZURE DATA FACTORY (ADF)          │   │   │   SYNAPSE LINK           │
│  ┌─────────────────────────────────┐    │   │   │  (Cosmos DB → Synapse)   │
│  │  Copy Activity / HTTP Connector │    │   │   └──────────────────────────┘
│  │  Trigger: Daily Schedule        │    │   │
│  └─────────────────────────────────┘    │   │
└──────────────────┬──────────────────────┘   │
                   │                          │
                   ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  AZURE DATA LAKE STORAGE GEN2 (ADLS Gen2)                    │
│                                                                               │
│   raw/                  bronze/               silver/           gold/         │
│   ├── fifa_matches/     ├── matches/           ├── matches/      ├── teams/   │
│   ├── player_stats/     ├── players/           ├── players/      ├── players/ │
│   ├── rankings/         ├── rankings/          ├── rankings/     └── predict/ │
│   └── live_scores/      └── live_scores/       └── features/                 │
│                                                                               │
│   (Delta Lake format across all layers)                                       │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                       ▼
┌──────────────────┐  ┌──────────────────────┐  ┌──────────────────────────┐
│  SYNAPSE SPARK   │  │  SYNAPSE PIPELINES   │  │  SYNAPSE SQL POOL        │
│  POOLS           │  │  (Orchestration)     │  │  (Dedicated / Serverless)│
│                  │  │                      │  │                          │
│  • Bronze ETL    │  │  • Master Pipeline   │  │  • External Tables       │
│  • Silver Clean  │  │  • Trigger Chains    │  │  • Materialized Views    │
│  • Gold Agg      │  │  • Error Handling    │  │  • Prediction Results    │
│  • ML Training   │  │  • Monitoring        │  │  • Aggregated KPIs       │
│  • Prediction    │  │                      │  │                          │
└──────────────────┘  └──────────────────────┘  └──────────┬───────────────┘
                                                             │
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │     POWER BI         │
                                                  │                      │
                                                  │  • Team Performance  │
                                                  │  • Player Analytics  │
                                                  │  • Group Stage Sim   │
                                                  │  • Knockout Bracket  │
                                                  │  • Prediction Model  │
                                                  └──────────────────────┘
```

---

## Technology Stack

| Component | Azure Service | Purpose |
|-----------|--------------|---------|
| Ingestion | **Azure Data Factory** | HTTP, REST, CSV connectors; scheduled triggers |
| Storage | **ADLS Gen2 + Delta Lake** | Medallion architecture (Bronze/Silver/Gold) |
| Batch Processing | **Synapse Spark Pools** | PySpark ETL + MLlib prediction model |
| SQL Analytics | **Synapse Dedicated SQL Pool** | Low-latency reporting layer |
| Serverless Queries | **Synapse Serverless SQL** | Ad-hoc exploration over Delta Lake |
| Operational Analytics | **Synapse Link + Cosmos DB** | Real-time live score integration |
| Orchestration | **Synapse Pipelines** | End-to-end workflow orchestration |
| Visualization | **Power BI** | Interactive dashboards + prediction UI |
| CI/CD | **GitHub Actions** | Deploy ADF/Synapse artifacts |

---

## Project Structure

```
fifa-2026-worldcup-analytics/
├── infrastructure/
│   └── bicep/                        # Azure resource provisioning
│       ├── main.bicep
│       ├── synapse.bicep
│       ├── adls.bicep
│       └── cosmos.bicep
├── adf/
│   ├── pipelines/                    # ADF pipeline JSON definitions
│   ├── datasets/                     # ADF dataset definitions
│   └── linkedservices/               # ADF linked service definitions
├── synapse/
│   ├── notebooks/                    # PySpark notebooks
│   │   ├── 01_bronze_ingestion.ipynb
│   │   ├── 02_silver_transformation.ipynb
│   │   ├── 03_gold_aggregation.ipynb
│   │   └── 04_ml_prediction_model.ipynb
│   ├── sql_scripts/                  # SQL Pool scripts
│   │   ├── 01_create_schema.sql
│   │   ├── 02_external_tables.sql
│   │   ├── 03_create_views.sql
│   │   └── 04_stored_procedures.sql
│   └── pipelines/                    # Synapse pipeline definitions
│       └── master_pipeline.json
├── data/
│   └── sample/                       # Sample CSVs for testing
├── powerbi/
│   └── FIFA2026_Dashboard.md         # Power BI design guide
├── docs/
│   ├── architecture.md
│   ├── setup_guide.md
│   └── data_dictionary.md
└── .github/
    └── workflows/
        └── deploy.yml                # CI/CD pipeline
```

---

## Datasets Used

- [Kaggle: International Football Results (1872–2024)](https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017)
- [Kaggle: FIFA World Rankings](https://www.kaggle.com/datasets/cashncarry/fifaworldranking)
- [Kaggle: FIFA Player Stats](https://www.kaggle.com/datasets/stefanoleone992/fifa-22-complete-player-dataset)
- [FIFA Official API](https://www.fifa.com/tournaments/mens/worldcup/2026canada-mexico-unitedstates)

---

## Quick Start

### Prerequisites
- Azure Subscription
- Azure CLI installed
- Python 3.9+
- Power BI Desktop

### 1. Provision Infrastructure
```bash
az login
az group create --name rg-fifa2026 --location eastus
az deployment group create \
  --resource-group rg-fifa2026 \
  --template-file infrastructure/bicep/main.bicep
```

### 2. Configure ADF Pipelines
```bash
# Deploy ADF artifacts
az datafactory pipeline create \
  --factory-name adf-fifa2026 \
  --resource-group rg-fifa2026 \
  --name IngestRawData \
  --pipeline @adf/pipelines/ingest_raw_data.json
```

### 3. Run Synapse Notebooks
Import notebooks from `synapse/notebooks/` into your Synapse workspace and run in order:
1. `01_bronze_ingestion.ipynb`
2. `02_silver_transformation.ipynb`
3. `03_gold_aggregation.ipynb`
4. `04_ml_prediction_model.ipynb`

### 4. Load SQL Pool
Execute scripts in `synapse/sql_scripts/` in order against your Dedicated SQL Pool.

### 5. Connect Power BI
Open `powerbi/FIFA2026_Dashboard.pbix` and update the SQL Pool connection string.

---

## Prediction Model

The ML model uses **PySpark MLlib GBTClassifier** (Gradient Boosted Trees) to predict match outcomes (Win/Draw/Loss).

**Features used:**
- FIFA ranking differential
- Head-to-head win rate (last 5/10 years)
- Average goals scored/conceded (last 10 matches)
- Home/neutral ground advantage
- Tournament stage (Group/R16/QF/SF/Final)
- Squad average age and value
- Form index (points from last 5 matches)

**Model accuracy:** ~67% on holdout set (comparable to published academic benchmarks)

---

## CI/CD Pipeline

GitHub Actions workflow automatically:
1. Validates ARM/Bicep templates
2. Deploys ADF pipelines on push to `main`
3. Runs SQL script linting
4. Publishes Synapse artifacts

---

## Resume Highlights

This project demonstrates:
- ✅ **Azure Synapse Analytics** — Workspace, Spark Pools, Dedicated SQL Pool
- ✅ **Azure Data Factory** — HTTP connectors, Copy activities, Triggers
- ✅ **Delta Lake** — Medallion architecture with ACID transactions
- ✅ **ADLS Gen2** — Hierarchical namespace, role-based access
- ✅ **Synapse Link** — Cosmos DB analytical store integration
- ✅ **Synapse Pipelines** — Orchestration with error handling
- ✅ **PySpark / MLlib** — Distributed ML model training
- ✅ **Power BI** — DirectQuery to Synapse SQL Pool
- ✅ **CI/CD** — GitHub Actions deployment automation

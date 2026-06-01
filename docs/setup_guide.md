# Setup Guide — FIFA 2026 World Cup Analytics Platform

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Azure CLI | 2.50+ | Resource provisioning |
| Python | 3.9+ | Sample data generation |
| Git | 2.40+ | Source control |
| Power BI Desktop | Latest | Report development |
| VS Code + Synapse extension | Latest | Notebook development |

---

## Step 1 — Azure Resource Provisioning

```bash
# Login
az login
az account set --subscription "<your-subscription-id>"

# Create resource group
az group create --name rg-fifa2026 --location eastus

# Deploy all resources (~10 minutes)
az deployment group create \
  --resource-group rg-fifa2026 \
  --template-file infrastructure/bicep/main.bicep \
  --parameters sqlAdminPassword="YourPassword123!"
```

**Resources created:**
- `adlsfifa2026dev` — ADLS Gen2 storage account with 6 containers
- `synapse-fifa2026-dev` — Synapse workspace
- `sqldw_fifa2026` — Dedicated SQL Pool (DW100c)
- `sparkfifa2026` — Spark Pool (3–10 Medium nodes, auto-pause 15min)
- `adf-fifa2026-dev` — Azure Data Factory
- `cosmos-fifa2026-dev` — Cosmos DB (Synapse Link enabled)
- `kv-fifa2026-dev` — Key Vault

---

## Step 2 — Grant Synapse MSI Access to ADLS

```bash
# Synapse workspace managed identity needs Storage Blob Data Contributor on ADLS
SYNAPSE_PRINCIPAL=$(az synapse workspace show \
  --name synapse-fifa2026-dev \
  --resource-group rg-fifa2026 \
  --query identity.principalId -o tsv)

STORAGE_ID=$(az storage account show \
  --name adlsfifa2026dev \
  --resource-group rg-fifa2026 \
  --query id -o tsv)

az role assignment create \
  --assignee $SYNAPSE_PRINCIPAL \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

---

## Step 3 — Load Secrets into Key Vault

```bash
KV_NAME="kv-fifa2026-dev"

# ADLS account key
az keyvault secret set --vault-name $KV_NAME \
  --name "adls-account-key" \
  --value "$(az storage account keys list --account-name adlsfifa2026dev --query '[0].value' -o tsv)"

# Kaggle credentials (get from kaggle.com/settings)
az keyvault secret set --vault-name $KV_NAME --name "kaggle-username" --value "<your-username>"
az keyvault secret set --vault-name $KV_NAME --name "kaggle-api-key"  --value "<your-api-key>"
```

---

## Step 4 — Download Source Datasets

```bash
pip install kaggle

# Configure Kaggle credentials
mkdir ~/.kaggle
echo '{"username":"<user>","key":"<key>"}' > ~/.kaggle/kaggle.json

# Download datasets
kaggle datasets download -d martj42/international-football-results-from-1872-to-2017 -p data/raw/
kaggle datasets download -d cashncarry/fifaworldranking -p data/raw/
kaggle datasets download -d stefanoleone992/fifa-22-complete-player-dataset -p data/raw/

# Upload to ADLS raw container
az storage blob upload-batch \
  --account-name adlsfifa2026dev \
  --destination raw \
  --source data/raw/ \
  --auth-mode login
```

**Alternative:** Use the sample data generator:
```bash
python data/sample/generate_sample_data.py
# Then upload the sample CSVs to ADLS raw container
```

---

## Step 5 — Import Synapse Notebooks

1. Open Synapse Studio: `https://web.azuresynapse.net`
2. Navigate to **Develop → Notebooks → Import**
3. Import all `.py` files from `synapse/notebooks/`
4. Attach each notebook to `sparkfifa2026` Spark Pool

Or use CLI:
```bash
for notebook in synapse/notebooks/*.py; do
  name=$(basename "$notebook" .py)
  az synapse notebook import \
    --workspace-name synapse-fifa2026-dev \
    --name "$name" \
    --file @"$notebook" \
    --spark-pool-name sparkfifa2026
done
```

---

## Step 6 — Run the Pipeline

**Option A — Synapse Studio (recommended for first run):**
1. Open Synapse Studio → Integrate
2. Import `synapse/pipelines/master_pipeline.json`
3. Click **Add trigger → Trigger now**
4. Monitor in **Monitor → Pipeline runs**

**Option B — CLI:**
```bash
az synapse pipeline create-run \
  --workspace-name synapse-fifa2026-dev \
  --name pl_master_fifa2026
```

**Expected run time:** ~45–90 minutes (first run, training ML model)

---

## Step 7 — Create SQL Pool Tables

Connect to the Dedicated SQL Pool (`sqldw_fifa2026`) using:
- Azure Data Studio or SSMS
- Server: `synapse-fifa2026-dev.sql.azuresynapse.net`
- Database: `sqldw_fifa2026`
- Authentication: SQL Login (sqladmin)

Run scripts in order:
```sql
-- In Azure Data Studio / SSMS
:r synapse/sql_scripts/01_create_schema.sql
:r synapse/sql_scripts/02_external_tables.sql
:r synapse/sql_scripts/03_create_views.sql
:r synapse/sql_scripts/04_stored_procedures.sql
```

---

## Step 8 — Connect Power BI

1. Open Power BI Desktop
2. **Get Data → Azure → Azure Synapse Analytics SQL**
3. Server: `synapse-fifa2026-dev-ondemand.sql.azuresynapse.net`
4. Database: `master`
5. Mode: **DirectQuery**
6. Import all views from schema `reporting`
7. Follow `powerbi/FIFA2026_Dashboard.md` for report design

---

## Step 9 — Configure GitHub Actions (CI/CD)

Add repository secrets in GitHub → Settings → Secrets:

| Secret Name | Value |
|-------------|-------|
| `AZURE_CREDENTIALS` | Output of `az ad sp create-for-rbac --sdk-auth` |
| `SQL_ADMIN_PASSWORD` | Your SQL admin password |

Push to `main` branch to trigger automated deployment.

---

## Cost Estimate (Monthly, Dev tier)

| Resource | SKU | Est. Cost |
|----------|-----|-----------|
| ADLS Gen2 | LRS, ~50GB | ~$5 |
| Synapse Spark | Medium, 15min auto-pause | ~$30 |
| Dedicated SQL Pool | DW100c | ~$150 (pause when not in use!) |
| Cosmos DB | Serverless | ~$5 |
| ADF | ~100 activities/day | ~$10 |
| **Total** | | **~$200/month** |

> **Tip:** Pause the Dedicated SQL Pool when not actively using it to reduce costs by ~70%.
> ```bash
> az synapse sql pool pause --name sqldw_fifa2026 --workspace-name synapse-fifa2026-dev --resource-group rg-fifa2026
> ```

# dab_project — Citibike ETL Lakehouse (Databricks Asset Bundle)

A Databricks Asset Bundle (DAB) project implementing a Bronze-Silver-Gold Medallion
architecture for Citibike trip data using Delta Lake, Delta Live Tables (DLT),
and Databricks Workflows.

---

## Repository Structure

```
dab_project/
├── databricks.yml              # DAB bundle config — targets: dev, test, prod
├── pyproject.toml              # Python project metadata and dev dependencies
├── requiremnts_pyspark.txt     # Local PySpark unit test venv dependencies
├── requiremnts_dbc.txt         # Databricks Connect integration test venv dependencies
├── .coveragerc                 # pytest-cov configuration (source=src, show_missing=True)
│
├── src/                        # Importable Python source code (tested locally)
│   ├── citibike/
│   │   └── citibike_utils.py   # Bronze→Silver→Gold transformation functions
│   └── utils/
│       └── datetime_utils.py   # Shared datetime helpers
│
├── citibike_etl/               # Databricks pipeline code (runs on cluster)
│   ├── dlt/                    # Delta Live Tables pipeline scripts
│   ├── notebooks/              # Jupyter notebooks (Bronze / Silver / Gold)
│   └── scripts/                # Plain Python scripts
│
├── resources/                  # DAB resource definitions
│   ├── jobs/                   # Workflow job YAML configs
│   └── pipelines/              # DLT pipeline YAML configs
│
└── tests/
    ├── conftest.py                             # Shared SparkSession fixture
    ├── test_citibike_utils.py                  # Unit tests — citibike transformations
    ├── test_datetime_utils.py                  # Unit tests — datetime helpers
    ├── test_citibike_interview_scenarios.py    # Unit tests — scenario-based edge cases
    └── test_citibike_catalog_integration.py   # Integration tests — real catalog via Databricks Connect
```

---

## Prerequisites

Before setting up the project, ensure the following are installed:

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10 – 3.12 | Required by `pyproject.toml` |
| Java (JDK) | 17 or later | Required by PySpark local mode (`JAVA_HOME` must be set) |
| Databricks CLI | Latest | For bundle deploy commands — `pip install databricks-cli` |
| VS Code | Latest | Recommended IDE |
| Databricks VS Code Extension | Latest | For workspace connection and notebook execution |

Verify Java is configured correctly:
```powershell
java -version        # should show 17+
echo $env:JAVA_HOME  # should point to your JDK installation
```

---

## Two Virtual Environments

This project uses **two separate Python virtual environments** — one for each testing mode:

| Environment | Purpose | Key packages |
|---|---|---|
| `.venv_pyspark` | Local unit tests using a local PySpark session (no Databricks connection needed) | `pyspark`, `pytest`, `pytest-cov` |
| `.venv_dbc` | Integration tests against the real `citibike_dev` Databricks catalog via Databricks Connect | `databricks-connect`, `databricks-sdk`, `pytest` |

---

## Setup: `.venv_pyspark` (Local Unit Tests)

```powershell
# 1. Create the virtual environment
python -m venv .venv_pyspark

# 2. Activate it
.venv_pyspark\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requiremnts_pyspark.txt

# 4. Install this project in editable mode (so src/ is importable)
pip install -e .
```

Verify setup:
```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Setup: `.venv_dbc` (Databricks Connect Integration Tests)

```powershell
# 1. Create the virtual environment
python -m venv .venv_dbc

# 2. Activate it
.venv_dbc\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requiremnts_dbc.txt

# 4. Install this project in editable mode
pip install -e .
```

### Configure Databricks Authentication

Databricks Connect reads credentials from `~/.databrickscfg`. Configure it once:

```powershell
databricks configure
# Enter: Databricks workspace URL and personal access token
```

Verify connection:
```powershell
databricks auth describe
```

> **Note:** If you have multiple profiles in `~/.databrickscfg` pointing to the same host,
> set the profile explicitly before running tests:
> ```powershell
> $env:DATABRICKS_CONFIG_PROFILE="DEFAULT"
> ```

---

## Running Tests

### Unit Tests (requires `.venv_pyspark`)

```powershell
.venv_pyspark\Scripts\Activate.ps1

# Run all unit tests
pytest tests/test_citibike_utils.py tests/test_datetime_utils.py -vv

# Run with coverage report
pytest tests/test_citibike_utils.py tests/test_datetime_utils.py --cov -vv

# Run with HTML coverage report (open htmlcov/index.html afterwards)
pytest tests/test_citibike_utils.py tests/test_datetime_utils.py --cov --cov-report=html -vv
Invoke-Item htmlcov\index.html
```

### Integration Tests (requires `.venv_dbc` + Databricks auth)

These tests read real tables from the `citibike_dev` catalog via Databricks Connect.
They are **automatically skipped** when running with `.venv_pyspark`.

```powershell
.venv_dbc\Scripts\Activate.ps1
$env:DATABRICKS_CONFIG_PROFILE="DEFAULT"

pytest tests/test_citibike_catalog_integration.py -vv
```

---

## Deploying the Bundle (Databricks CLI)

```powershell
# Authenticate (once)
databricks configure

# Deploy to dev environment
databricks bundle deploy --target dev

# Deploy to test environment
databricks bundle deploy --target test

# Deploy to production
databricks bundle deploy --target prod

# Run a specific job after deploying
databricks bundle run citibike_etl_pipeline --target dev
```

---

## VS Code Setup

1. Install the **Databricks** extension (`ms-databricks.databricks`)
2. Connect to your workspace via the Databricks sidebar
3. Select the Python interpreter for each file type:
   - For `tests/test_citibike_utils.py` → select `.venv_pyspark`
   - For `tests/test_citibike_catalog_integration.py` → select `.venv_dbc`
   - For `citibike_etl/notebooks/*.ipynb` → select `.venv_dbc`

A `.vscode/extensions.json` file is committed to this repo with the recommended
extensions list. VS Code will prompt you to install them when you open the project.

---

## Catalog Structure (citibike_dev)

| Layer | Schema | Table | Description |
|---|---|---|---|
| Bronze | `01_bronze` | `jc_citibike` | Raw ingested trip records |
| Silver | `02_silver` | `jc_citibike` | Cleaned, typed, deduplicated with metadata |
| Gold | `03_gold` | `daily_ride_summary` | Daily aggregated ride KPIs |
| Gold | `03_gold` | `daily_station_performance` | Daily per-station performance metrics |


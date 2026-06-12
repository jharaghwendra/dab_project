# Databricks Lakehouse Projects with DAB (Databricks Asset Bundle)

This repository contains three Databricks deliverables:

- a Citibike Databricks Asset Bundle project with its own catalog and schema
- an iGaming Databricks Asset Bundle project presented under the `gaming_lakehouse` project name
- an iGaming dbt Gold-layer project in `dbt_gold/`

Together they show how I build Databricks projects across ingestion, transformation, orchestration, testing, and Gold-layer modeling.

---

## Project Overview

- Citibike DAB: separate catalog/schema, Bronze-Silver-Gold processing, local and Databricks Connect tests
- iGaming DAB: multi-country Bronze-Silver processing with jobs and pipelines in `resources/jobs/` and `resources/pipelines/`
- iGaming dbt Gold layer: located in `dbt_gold/` with incremental models, tests, snapshots, and CI/CD workflows to support the iGaming BI platform

## Where To Look

| What you want to see | Where to look |
|---|---|
| Citibike DAB project | [Repository Structure](#repository-structure), [Catalog Structure](#catalog-structure-citibike_dev), [Getting Started](#getting-started) |
| iGaming DAB project | [iGaming DAB Project](#igaming-dab-project), especially [resources/jobs](resources/jobs) and [resources/pipelines](resources/pipelines) |
| iGaming dbt Gold layer | [dbt Work](#dbt-work), especially [dbt_gold](dbt_gold) and [fact_payments.sql](dbt_gold/models/facts/fact_payments.sql) |
| CI/CD workflow | [CI/CD Workflow](#cicd-workflow), especially [.github/workflows/ci-workflow.yml](.github/workflows/ci-workflow.yml) and [.github/workflows/cd-workflow.yml](.github/workflows/cd-workflow.yml) |
| Setup and environment | This section below, kept intentionally short |

---

## Repository Structure

```
dab_project/
├── databricks.yml              # DAB bundle config — targets: dev, test, prod
├── pyproject.toml              # Python project metadata and dev dependencies
├── requirements_pyspark.txt    # Local PySpark unit test venv dependencies
├── requirements_dbc.txt        # Databricks Connect integration test venv dependencies
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
├── gaming_lakehouse/           # iGaming DAB project code and scripts
├── resources/                  # DAB resource definitions
│   ├── jobs/                   # Workflow job YAML configs
│   └── pipelines/              # DLT pipeline YAML configs
│
├── dbt_gold/                   # iGaming Gold layer built with dbt
├── docs/                       # Diagrams and lineage images
│
└── tests/
    ├── conftest.py                             # Shared SparkSession fixture
    ├── test_citibike_utils.py                  # Unit tests — citibike transformations
    ├── test_datetime_utils.py                  # Unit tests — datetime helpers
    ├── test_citibike_scenarios.py              # Unit tests — scenario-based edge cases
    └── test_citibike_catalog_integration.py    # Integration tests — real catalog via Databricks Connect
```

---

## dbt Work

The dbt work for the iGaming project lives in [dbt_gold](dbt_gold) and follows a standard analytics-engineering structure.

### Location

```text
dbt_gold/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── facts/
│   ├── dimensions/
│   ├── marts/
│   └── sources/
├── macros/
├── snapshots/
└── tests/
```

### dbt Highlights

- Incremental fact models with merge strategy
- Late-arriving data handling with lookback logic
- Surrogate key generation using `dbt_utils`
- Python model
- Snapshot-based SCD2 history tracking
- Generic tests, singular tests, custom generic tests, source tests, and unit tests
- Source freshness and state-based workflows
- Documentation-ready model structure for analytics consumers

These pieces show how I handle late-arriving data, preserve history, validate models at multiple layers, and keep downstream reporting stable.

### Strong dbt example: `fact_payments`

The [fact_payments.sql](dbt_gold/models/facts/fact_payments.sql) model is a strong example of production-style dbt design:

- incremental `merge` strategy
- 2-day lookback for late-arriving status updates
- surrogate key generation via `dbt_utils`
- business-rule logic for declined deposit retries and first-deposit flags
- timezone-aware date handling
- BI-friendly denormalized output for downstream reporting tools

### Testing and quality checks

- Generic tests in YAML for column integrity
- Singular tests in `tests/` for business rules
- Custom generic tests in `macros/` for reusable validations
- Unit tests in model YAML for isolated SQL logic
- Source tests and source freshness checks for upstream reliability

### Dev and delivery workflow

- Local validation through dbt test commands
- CI/CD automation via GitHub workflows
- Separate dev, test, and prod targets in the dbt profile

---

## iGaming DAB Project

It handles three country datasets - Austria, Germany, and Denmark - with country-specific Bronze and Silver processing, then a unified Gold layer built from the shared Silver tables using `country_id`.

### Folder Structure

```text
gaming_lakehouse/
├── scripts/
│   └── 03_gold/
├── resources/
│   ├── jobs/
│   └── pipelines/
├── docs/
└── dbt_gold/
```

### End-to-End Execution Flow

The pipeline is organized as Bronze, Silver, and Gold processing with Databricks Workflows and DLT.
The DAB jobs and resources for that flow live under `resources/jobs/` and `resources/pipelines/`.

### Bronze and Silver jobs

These jobs process each country separately in the Bronze and Silver layers:

- `TMA Medallion Bronze and Silver AT`
- `TMA Medallion Bronze and Silver DE`
- `TMA Medallion Bronze and Silver DK`

Each Bronze task lands raw country data from the source layer, and the matching Silver task applies the latest-record merge/upsert logic with deduplication and schema enforcement. That gives the job graph a clear Bronze → Silver task lineage for each country.

### Gold jobs

These jobs read from the shared Silver layer, where the Silver tables hold the combined multi-country data using `country_id`:

- `TMA Gold Dims Daily`
- `TMA Gold Facts`
- `TMA Gold Hourly Orchestrator`
- `TMA Gold Marts PC`
- `TMA Gold Snapshots Hourly`

The Gold lineage is dbt-driven: snapshots such as `dim_player`, `dim_tag`, and `dim_userlimit` feed downstream marts, while facts and dimensions are built from the shared Silver layer. Example lineage is `silver.userdata -> snapshot dim_player -> mart_pc_account_signup` and `silver.gameround -> fact_gameround -> mart_gameround_hourly`.

### Gold Layer

- Gold models are built with dbt and Databricks SQL Warehouse, consuming the prepared Silver layer as input.

---

## CI/CD Workflow

The GitHub Actions workflows live in [.github/workflows](.github/workflows).

### CI workflow: [.github/workflows/ci-workflow.yml](.github/workflows/ci-workflow.yml)

- Runs on feature branches and pull requests into `main`
- Sets up Python, installs dependencies, runs pytest, and publishes coverage
- Runs `dbt parse` so SQL and model references are checked before deployment

### CD workflow: [.github/workflows/cd-workflow.yml](.github/workflows/cd-workflow.yml)

- Runs when code lands on `main`
- Deploys the bundle to the test environment first
- Then deploys to prod after the test deploy succeeds and the prod environment gate allows it
- Uses Databricks CLI and bundle deploy commands for the release step

### What this shows

- Pull requests are validated before merge
- Test deployment is automated from the main branch
- Prod deployment is separated from test and can be protected by environment approval
- The same bundle is promoted through the release flow instead of maintaining separate deploy logic per environment

---

## Docs And Lineage Images

The `docs/` folder can hold the visual lineage artifacts for this project.

Recommended image files:

- `docs/igaming_bronze_silver_lineage.png` for the Bronze → Silver task graph across AT, DE, and DK
- `docs/igaming_gold_dbt_lineage.png` for the dbt Gold lineage showing snapshots, facts, dimensions, and marts

These images show the databricks job-task flow and dbt model lineage.

---

## Getting Started

Use two Python environments when you work on the Citibike project:

- `.venv_pyspark` for local unit tests and source development
- `.venv_dbc` for Databricks Connect tests and notebook-style work

Databricks authentication comes from `~/.databrickscfg`, and the Databricks CLI is used for bundle deploy and run commands.

VS Code is most useful here when the Databricks extension is installed and the right interpreter is selected for the file you are editing.

For exact install and run commands, use the test and deploy sections above and below rather than duplicating them here.

---

## Catalog Structure (citibike_dev)

| Layer | Schema | Table | Description |
|---|---|---|---|
| Bronze | `01_bronze` | `jc_citibike` | Raw ingested trip records |
| Silver | `02_silver` | `jc_citibike` | Cleaned, typed, deduplicated with metadata |
| Gold | `03_gold` | `daily_ride_summary` | Daily aggregated ride KPIs |
| Gold | `03_gold` | `daily_station_performance` | Daily per-station performance metrics |

---

## Catalog Structure (igaming_dev)

Raw source data lands from the AWS S3 data lake into the `bronze` schema. Bronze keeps source-aligned tables, Silver applies deduplication and SCD Type 1 merge/upsert logic with tight schema enforcement, and Gold is dbt modeled as a star schema ready for the BI team.

### Bronze layer: raw source tables

| Schema | Table |
|---|---|
| `bronze` | `brandgame` |
| `bronze` | `check` |
| `bronze` | `game` |
| `bronze` | `gameround` |
| `bronze` | `gametransaction` |
| `bronze` | `payment` |
| `bronze` | `tag` |
| `bronze` | `userdata` |
| `bronze` | `userlimit` |

### Silver layer: cleaned and deduplicated tables

Silver tables hold the latest valid version of each record using merge/upsert logic, deduplication, and schema enforcement.

| Schema | Table |
|---|---|
| `silver` | `brandgame` |
| `silver` | `check` |
| `silver` | `game` |
| `silver` | `gameround` |
| `silver` | `gametransaction` |
| `silver` | `payment` |
| `silver` | `silver_gametransaction` |
| `silver` | `tag` |
| `silver` | `userdata` |
| `silver` | `userlimit` |

### dbt Gold schema

The iGaming Gold layer is built with dbt in the `gold` schema.

| Schema | Table Name | Type |
|---|---|---|
| `gold` | `dim_date_spine` | Dimension |
| `gold` | `dim_date_time` | Dimension |
| `gold` | `dim_game` | Dimension |
| `gold` | `dim_payment_method` | Dimension |
| `gold` | `fact_payments` | Fact |
| `gold` | `fact_game_revenue` | Fact |
| `gold` | `fact_gameround` | Fact |
| `gold` | `fact_gametransaction_kpi` | Fact |
| `gold` | `dim_player` | Snapshot / SCD2 |
| `gold` | `dim_tag` | Snapshot / SCD2 |
| `gold` | `dim_userlimit` | Snapshot / SCD2 |
| `gold` | `mart_gameround_hourly` | Mart |
| `gold` | `mart_pc_account_signup` | Mart |


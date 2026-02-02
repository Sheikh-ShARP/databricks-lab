# Supply Chain Analytics - Databricks Lakehouse

## Overview
This project implements a Medallion Architecture (Bronze → Silver → Gold)
on Databricks using Unity Catalog across DEV, TEST, and PROD environments.

## Environments
We use a single codebase with runtime environment selection:
- dev  → supply_dev
- test → supply_test
- prod → supply_prod

The environment is injected via Databricks widget parameter `env`.

## Execution Order
Notebooks must be executed in the following order:
1. 02_bronze_ingestion
2. 03_silver_clean
3. 04_gold_kpis

## How to Run (DEV)
- Open notebook
- Set widget: env = dev
- Run all cells

## How to Run (TEST / PROD)
- Do NOT run manually
- Use Databricks Jobs with env=test or env=prod

## Data Sources
Raw files must exist in:
/Volumes/<env>_supply/bronze/raw_data/

## Outputs
Tables are written to:
<env>_supply.<bronze|silver|gold>

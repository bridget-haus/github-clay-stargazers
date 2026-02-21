# GitHub Stargazer ELT Pipeline

An end-to-end ELT pipeline that extracts GitHub stargazer data via GraphQL, loads it into DuckDB, transforms it with dbt, and generates analytics-ready models and reports.

## 🚀 Features

- Parallel GraphQL extraction across multiple repos
- Incremental loading with per-repo watermarks
- Full backfill mode for reproducible rebuilds
- Merge-based writes to prevent duplicates
- dbt models for analytics-ready star metrics
- Automated daily orchestration with Dagster

---

## 🏗 Architecture

**Extract**
- GitHub GraphQL API
- Parallel repo fetching
- ASC order for backfill, DESC for incremental

**Load**
- DuckDB via dlt
- Merge on `(repo_full_name, user_id)`

**Transform**
- dbt models:
  - `stargazer_by_user`
  - `stargazer_by_month`

**Report**
- Python visualizations + HTML output

---

## 🔄 Pipeline Modes

### Backfill
Rebuilds the dataset from source of truth.

Behavior:
- Deletes DuckDB database file
- Fetches full history (ASC order)
- Recreates tables via merge writes

Use when:
- First run
- Logic changes
- Data drift correction
- Reproducible analytics

```bash
python loader.py --mode backfill
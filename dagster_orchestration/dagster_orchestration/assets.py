from dagster import asset, Field
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@asset(config_schema={"mode": Field(str, default_value="incremental")})
def extract_and_load(context):
    """
    Loads GitHub stargazers into DuckDB via github_stargazers_loader.py.

    Mode is controlled via Dagster Launchpad config:
      - incremental (default)
      - backfill
    """
    mode = context.op_config["mode"]

    result = subprocess.run(
        [sys.executable, "github_stargazers_loader.py", "--mode", mode],
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,    
        text=True,
        check=True,
        cwd=REPO_ROOT,
    )
    # send subprocess output to Dagster logs
    print(result.stdout)


@asset(deps=[extract_and_load])
def dbt_transform():
    subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "config",
        ],
        check=True,
        cwd=REPO_ROOT,
    )


@asset(deps=[dbt_transform])
def generate_reports():
    subprocess.run(
        [sys.executable, "reports/visualizations.py"],
        check=True,
        cwd=REPO_ROOT,
    )
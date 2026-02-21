from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)

from .assets import extract_and_load, dbt_transform, generate_reports


# Job that materializes the full asset graph
github_stargazers_job = define_asset_job(
    name="github_stargazers_job",
    selection=AssetSelection.keys(
        "extract_and_load",
        "dbt_transform",
        "generate_reports",
    ),
)


# 6am daily schedule (incremental mode)
daily_incremental_schedule = ScheduleDefinition(
    job=github_stargazers_job,
    cron_schedule="0 6 * * *",  # 6:00 AM daily
    run_config={
        "ops": {
            "extract_and_load": {
                "config": {
                    "mode": "incremental"
                }
            }
        }
    },
    execution_timezone="America/New_York",  # adjust if needed
)


defs = Definitions(
    assets=[extract_and_load, dbt_transform, generate_reports],
    jobs=[github_stargazers_job],
    schedules=[daily_incremental_schedule],
)
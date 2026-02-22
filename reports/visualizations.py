import base64
from io import BytesIO
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# ----------------------------
# Helpers
# ----------------------------

con = duckdb.connect("data/github_stars.duckdb")
ts = con.execute(f"SELECT MAX(extracted_at) FROM main.raw_github_stargazers").fetchone()[0]

if ts is None:
    refresh_timestamp = "Unknown"
else:
    # If DuckDB returns a naive datetime, assume UTC then convert to NY
    if isinstance(ts, datetime) and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    ts_est = ts.astimezone(ZoneInfo("America/New_York")) if isinstance(ts, datetime) else ts
    refresh_timestamp = ts_est.strftime("%m/%d/%Y %I:%M%p %Z").lstrip("0").replace(" 0", " ")

def fig_to_base64_png(fig, dpi: int = 160) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG string."""
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded


def df_to_html_table(df: pd.DataFrame, table_class: str = "stargazer-table", index: bool = True) -> str:
    """Convert a dataframe to an HTML table string."""
    return df.to_html(index=index, classes=table_class, border=0)


# ----------------------------
# NEW: Build time-series summary table (months as columns, repos as rows)
# ----------------------------
def build_stars_timeseries_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Year-level summary with row + column grand totals.
    """
    df = con.execute("""
        SELECT
            repo_full_name,
            month,
            stars
        FROM main.stargazer_by_month
    """).df()

    df["month_dt"] = pd.to_datetime(df["month"])
    df["year"] = df["month_dt"].dt.year

    pivot = df.pivot_table(
        index="repo_full_name",
        columns="year",
        values="stars",
        fill_value=0,
        aggfunc="sum",
    )

    # Order columns chronologically
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)

    # Order repos by total stars desc
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]

    # ----------------------------
    # Add Grand Totals
    # ----------------------------
    pivot["Total"] = pivot.sum(axis=1)              # row totals
    column_totals = pivot.sum(axis=0)                     # column totals
    pivot.loc["Total"] = column_totals              # bottom row

    # ----------------------------
    # Format with commas
    # ----------------------------
    pivot_display = pivot.astype(int).apply(lambda col: col.map(lambda x: f"{x:,}"))

    pivot_display.index.name = None
    pivot_display.columns.name = None
    return pivot_display

# ----------------------------
# Build: Summary distribution table
# ----------------------------
def build_repos_starred_summary(con: duckdb.DuckDBPyConnection, max_repos: int = 5) -> pd.DataFrame:
    df = con.execute("""
        SELECT repos_starred
        FROM main.stargazer_by_user
    """).df()

    df = df[df["repos_starred"].between(1, max_repos)]

    # Count users per repo bucket
    counts = df["repos_starred"].value_counts().reindex(range(1, max_repos + 1), fill_value=0)
    counts = counts.astype(int)

    total_users = counts.sum()

    # Percentages
    pct = (counts / total_users * 100).round(1)

    # ---- Display formatting ----
    counts_display = counts.apply(lambda x: f"{x:,}")
    pct_display = pct.astype(str) + "%"

    # Add Total column
    counts_display["Total"] = f"{total_users:,}"
    pct_display["Total"] = "100%"

    summary = pd.DataFrame(
        [counts_display.values, pct_display.values],
        index=["Count Users", "%"]
    )

    summary.columns = [
        *(f"{i} repo" if i == 1 else f"{i} repos" for i in range(1, max_repos + 1)),
        "Total"
    ]

    summary.index.name = ""
    summary.columns.name = "# of Repos Starred"

    return summary


# ----------------------------
# Build: Chart (inline in HTML)
# ----------------------------
def build_stars_by_month_chart_base64(con: duckdb.DuckDBPyConnection) -> str:
    """
    Builds the stacked bar chart exactly like your PNG version, but returns base64 PNG
    so it can be embedded directly into HTML.
    """
    df = con.execute("""
        SELECT
            repo_full_name,
            month,
            stars
        FROM main.stargazer_by_month
        ORDER BY month, repo_full_name
    """).df()

    df["month_dt"] = pd.to_datetime(df["month"])
    df["month"] = df["month_dt"].dt.strftime("%Y-%m")

    pivot = (
        df.pivot_table(
            index="month",
            columns="repo_full_name",
            values="stars",
            fill_value=0
        ).sort_index()
    )

    totals = pivot.sum().sort_values(ascending=False)
    pivot = pivot[totals.index]

    fig, ax = plt.subplots(figsize=(14, 7))
    pivot.plot(kind="bar", stacked=True, ax=ax, width=0.85)

    ax.set_xlabel("Year")
    ax.set_ylabel("Stars")

    month_dts = pd.to_datetime(pivot.index + "-01")
    labels = [str(d.year) if d.month == 1 else "" for d in month_dts]
    ax.set_xticklabels(labels, rotation=0)

    plt.tight_layout()

    return fig_to_base64_png(fig, dpi=160)


# ----------------------------
# Build: Detailed stargazers table (individual users)
# ----------------------------
def build_top_stargazers_table(con: duckdb.DuckDBPyConnection, top_n: int = 50) -> pd.DataFrame:
    users = con.execute(f"""
        SELECT
            login,
            repos_starred
        FROM main.stargazer_by_user
        ORDER BY repos_starred DESC, login ASC
    """).df()

    users = users.rename(columns={"login": "User", "repos_starred": "Repos Starred"})
    return users


# ----------------------------
# Save: Full HTML report (time series summary -> chart -> repos summary -> individual users)
# ----------------------------
def save_html_report(con: duckdb.DuckDBPyConnection, top_n: int = 50, max_repos: int = 5) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)

    # NEW ordering:
    # 1) time series summary table
    timeseries_df = build_stars_timeseries_summary(con)

    # 2) bar chart directly underneath
    chart_b64 = build_stars_by_month_chart_base64(con)

    # 3) repos_starred distribution summary beneath chart
    summary_df = build_repos_starred_summary(con, max_repos=max_repos)

    # 4) individual table at the end
    top_users_df = build_top_stargazers_table(con, top_n=top_n)

    timeseries_html = df_to_html_table(timeseries_df, table_class="stargazer-table timeseries-table", index=True)
    summary_html = summary_df.to_html(classes="stargazer-table summary-table", border=0)
    users_html = top_users_df.to_html(index=False, classes="stargazer-table users-table", border=0)

    html = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8" />
    <title>GitHub Stargazer Dashbaord</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
        body {{
        font-family: Arial, sans-serif;
        margin: 32px;
        color: #111;
        }}

        h1 {{
        margin: 0 0 10px 0;
        font-size: 28px;
        }}

        .sub {{
        margin: 0 0 28px 0;
        color: #555;
        }}

        h2 {{
        margin-top: 34px;
        margin-bottom: 12px;
        font-size: 18px;
        }}

        .refresh-timestamp {{
        position: absolute;
        top: 20px;
        right: 32px;
        font-size: 12px;
        color: #888;
        letter-spacing: 0.5px;
        }}

        .card {{
        border: 1px solid #e5e5e5;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 18px;
        background: #fff;
        width: 100%;
        }}

        /* Base table styling */
        table.stargazer-table {{
        border-collapse: collapse;
        width: 100%;
        table-layout: fixed;
        }}

        table.stargazer-table th,
        table.stargazer-table td {{
        padding: 8px 10px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        }}

        table.stargazer-table th {{
        text-align: left;
        border-bottom: none;
        font-weight: 700;
        }}

        table.stargazer-table tr:nth-child(even) {{
        background-color: #f6f6f6;
        }}

        /* ----------------------------
        TIME SERIES TABLE — RESPONSIVE + FULL WIDTH
        ---------------------------- */

        .hscroll {{
        overflow-x: auto;
        border: 1px solid #eee;
        border-radius: 10px;
        width: 100%;
        }}

        table.timeseries-table {{
        width: 100% !important;       /* fill card */
        table-layout: fixed !important;/* distribute columns evenly */
        }}

        table.timeseries-table th,
        table.timeseries-table td {{
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        }}

        /* Repo column gets fixed width */
        table.timeseries-table th:first-child,
        table.timeseries-table td:first-child {{
        width: 150px;
        position: sticky;
        left: 0;
        background: #fff;
        z-index: 2;
        }}

        table.timeseries-table tr:nth-child(even) td:first-child {{
        background: #f6f6f6;
        }}

        /* Right-align numeric columns */
        table.timeseries-table th:not(:first-child),
        table.timeseries-table td:not(:first-child) {{
        text-align: right;
        }}

        /* Summary table tweaks */
        table.summary-table {{
        table-layout: auto;
        }}
        table.summary-table th, table.summary-table td {{
        white-space: normal;
        }}

        /* Users table: scrollable + sticky header */
        .scroll {{
        max-height: 720px;
        overflow: auto;
        border: 1px solid #eee;
        border-radius: 10px;
        }}
        .scroll table {{
        margin: 0;
        }}
        .scroll thead th {{
        position: sticky;
        top: 0;
        background: #fff;
        z-index: 1;
        }}

        /* Chart */
        .chart {{
        width: 100%;
        height: auto;
        display: block;
        }}
    </style>
    </head>
    <body>

    <h1>GitHub Stargazer Dashboard</h1>

    <div class="refresh-timestamp">
    Data Refreshed as of {refresh_timestamp}
    </div>

    <div class="card">
        <h2>Github Stars by Repo Time Series</h2>
        <div class="hscroll">
        {timeseries_html}
        </div>
    </div>
    

    <div class="card">
        <img class="chart" src="data:image/png;base64,{chart_b64}" alt="Stars by Month (Stacked)" />
    </div>

    <div class="card">
        <h2>Distribution of Repos Starred</h2>
        {summary_html}
    </div>

    <div class="card">
        <h2>Individual Stargazers (Repos Starred)</h2>
        <div class="scroll">
        {users_html}
        </div>
    </div>

    </body>
    </html>
    """.strip()

    out_path = Path("reports") / "github_stargazer_dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved {out_path}")


def main():
    con = duckdb.connect("data/github_stars.duckdb")
    save_html_report(con, top_n=50, max_repos=5)


if __name__ == "__main__":
    main()
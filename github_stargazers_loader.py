import os
import json
import argparse
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Iterable, Any, List
import requests
import dlt
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue
import threading
from dotenv import load_dotenv

load_dotenv()

def load_config(path: str = "config/config.json") -> dict:
    with open(path) as f:
        return json.load(f)


def parse_github_ts(ts: str) -> datetime:
    # GitHub returns e.g. "2026-02-19T01:23:45Z"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_repo_watermarks(pipeline) -> Dict[str, datetime]:
    """
    Pure watermark approach: derive per-repo high-water marks from the destination table itself.
    Returns {repo_full_name: max_starred_at_datetime_utc}.
    """
    sql = """
        SELECT
            repo_full_name,
            MAX(CAST(starred_at AS TIMESTAMP)) AS max_starred_at
        FROM main.raw_github_stargazers
        GROUP BY 1
    """

    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(sql)
    except Exception:
        # First run: table doesn't exist yet
        return {}

    out: Dict[str, datetime] = {}
    for repo_full_name, max_starred_at in rows:
        if max_starred_at is None:
            continue
        # DuckDB often returns Python datetime already; handle strings defensively.
        if isinstance(max_starred_at, str):
            dt = parse_github_ts(max_starred_at)
        else:
            dt = max_starred_at
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        out[repo_full_name] = dt

    return out


def fetch_repo_to_queue(
    *,
    repo_slug: str,
    mode: str,
    watermark: Optional[datetime],
    gql_url: str,
    headers: Dict[str, str],
    query: str,
    extracted_at: str,
    out_queue: Queue,
    metrics: Dict[str, Dict[str, int]],
) -> None:
    """
    Worker thread: fetch pages for a single repo and push row dicts into out_queue.
    Updates metrics[repo_full_name] with counts.
    """
    owner, repo = repo_slug.split("/", 1)
    repo_full_name = f"{owner}/{repo}"

    cursor = None
    pages = 0
    yielded = 0
    stop_reason = 0  # 0=exhausted, 1=watermark

    # Optional: each thread can have its own Session (safe + a bit faster)
    session = requests.Session()

    while True:
        payload = {"query": query, "variables": {"owner": owner, "name": repo, "after": cursor}}
        resp = session.post(gql_url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors for {repo_slug}: {data['errors']}")

        repo_data = data.get("data", {}).get("repository")
        if not repo_data:
            break

        sg = repo_data["stargazers"]
        pages += 1

        # In incremental+DESC mode, stop once we hit <= watermark
        for edge in sg["edges"]:
            node = edge["node"]
            starred_at_str = edge["starredAt"]
            starred_at_dt = parse_github_ts(starred_at_str)

            if mode == "incremental" and watermark is not None:
                if starred_at_dt <= watermark:
                    stop_reason = 1
                    # Stop processing this page + all further pages
                    sg = None  # just to avoid accidental use below
                    cursor = None
                    break

            out_queue.put(
                {
                    "repo_full_name": repo_full_name,
                    "login": node["login"],
                    "user_id": node["databaseId"],
                    "starred_at": starred_at_str,
                    "extracted_at": extracted_at,
                }
            )
            yielded += 1

        if stop_reason == 1:
            break

        if sg["pageInfo"]["hasNextPage"]:
            cursor = sg["pageInfo"]["endCursor"]
        else:
            break

    # store simple metrics (thread-safe update via lock)
    metrics[repo_full_name] = {
        "pages": pages,
        "yielded": yielded,
        "stop_reason_watermark": stop_reason,
    }


@dlt.resource(
    name="raw_github_stargazers",
    write_disposition="merge",
    primary_key=["repo_full_name", "user_id"],
)

def github_stargazers(
    mode: str = "backfill",
    repo_watermarks: Optional[Dict[str, datetime]] = None,
    max_workers: Optional[int] = None,
    queue_maxsize: int = 10_000,
) -> Iterable[Dict[str, Any]]:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable not set")

    config = load_config()
    repos: List[str] = config["repos"]

    gql_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    direction = "ASC" if mode == "backfill" else "DESC"
    query = f"""
    query ($owner: String!, $name: String!, $after: String) {{
      repository(owner: $owner, name: $name) {{
        stargazers(first: 100, after: $after, orderBy: {{field: STARRED_AT, direction: {direction}}}) {{
          pageInfo {{ endCursor hasNextPage }}
          edges {{
            starredAt
            node {{ login databaseId }}
          }}
        }}
      }}
    }}
    """

    extracted_at = utc_now_iso()
    repo_watermarks = repo_watermarks or {}

    # Shared queue for streaming rows back to the main generator
    q: Queue = Queue(maxsize=queue_maxsize)

    # Sentinels / coordination
    done_sentinel = object()
    error_sentinel = object()

    # Metrics per repo (written by workers, read by main thread at end)
    metrics: Dict[str, Dict[str, int]] = {}
    metrics_lock = threading.Lock()

    def worker_wrapper(repo_slug: str) -> None:
        owner, repo = repo_slug.split("/", 1)
        repo_full_name = f"{owner}/{repo}"
        watermark = repo_watermarks.get(repo_full_name) if mode == "incremental" else None

        # local metrics holder to avoid partial updates
        local_metrics: Dict[str, Dict[str, int]] = {}

        try:
            fetch_repo_to_queue(
                repo_slug=repo_slug,
                mode=mode,
                watermark=watermark,
                gql_url=gql_url,
                headers=headers,
                query=query,
                extracted_at=extracted_at,
                out_queue=q,
                metrics=local_metrics,
            )
            with metrics_lock:
                metrics.update(local_metrics)
        except Exception as e:
            # Send the exception object to main thread
            q.put((error_sentinel, repo_slug, e))
        finally:
            # Signal this repo is done
            q.put((done_sentinel, repo_slug, None))

    # choose worker count (small is safer for rate limits)
    workers = max_workers or min(len(repos), 5)

    # Launch workers
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures: List[Future] = [executor.submit(worker_wrapper, repo_slug) for repo_slug in repos]

        done_count = 0
        total_repos = len(repos)

        # Consume queue until all repos done
        while done_count < total_repos:
            item = q.get()

            # Handle control messages
            if isinstance(item, tuple) and len(item) == 3 and item[0] is done_sentinel:
                done_count += 1
                continue

            if isinstance(item, tuple) and len(item) == 3 and item[0] is error_sentinel:
                _, repo_slug, exc = item
                # Optionally: cancel remaining work
                for f in futures:
                    f.cancel()
                raise RuntimeError(f"Worker failed for repo {repo_slug}: {exc}") from exc

            # Otherwise it's a normal row dict
            yield item

        # Ensure any exceptions in futures are surfaced
        for f in futures:
            f.result()

    # Optional: print per-repo fetch metrics (will show in stdout before dlt printout)
    # (comment out if you want totally clean output)
    print("\nFetch metrics (parallel):")
    for repo_full_name in sorted(metrics.keys()):
        m = metrics[repo_full_name]
        reason = "watermark" if m.get("stop_reason_watermark") == 1 else "exhausted"
        print(f"  {repo_full_name}: pages={m.get('pages',0)} yielded={m.get('yielded',0)} stop={reason}")


def table_exists(pipeline, full_table_name: str) -> bool:
    schema, table = full_table_name.split(".", 1)
    sql = f"""
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = '{schema}'
        AND table_name = '{table}'
      LIMIT 1
    """
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(sql)
        return len(rows) > 0
    except Exception:
        return False


def get_total_star_rows(pipeline) -> int:
    sql = "SELECT COUNT(*) FROM main.raw_github_stargazers"
    with pipeline.sql_client() as client:
        return int(client.execute_sql(sql)[0][0])


def get_repo_star_rows(pipeline) -> Dict[str, int]:
    sql = """
      SELECT repo_full_name, COUNT(*) AS cnt
      FROM main.raw_github_stargazers
      GROUP BY 1
    """
    with pipeline.sql_client() as client:
        rows = client.execute_sql(sql)
    return {r[0]: int(r[1]) for r in rows}

def main():
    parser = argparse.ArgumentParser(description="GitHub stargazer pipeline")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of concurrent repo fetch workers (default: min(num_repos, 5))",
    )
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name="github_stargazers",
        destination="duckdb",
        dataset_name="main",
    )

    # --- BACKFILL: drop the DuckDB database file (full rebuild) ---
    if args.mode == "backfill":
        db_path = os.path.join("data", "github_stars.duckdb")
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"🧹 BACKFILL selected: deleted DB {db_path}")

    # --- before counts ---
    has_table = table_exists(pipeline, "main.raw_github_stargazers")
    before_total = get_total_star_rows(pipeline) if has_table else 0
    before_by_repo = get_repo_star_rows(pipeline) if has_table else {}

    repo_watermarks = {}
    if args.mode == "incremental":
        repo_watermarks = get_repo_watermarks(pipeline)

    # --- run ---
    start_time = time.perf_counter()

    info = pipeline.run(
        github_stargazers(mode=args.mode, repo_watermarks=repo_watermarks)
    )

    end_time = time.perf_counter()
    duration_seconds = end_time - start_time

    # --- after counts ---
    after_total = get_total_star_rows(pipeline)
    after_by_repo = get_repo_star_rows(pipeline)

    new_rows = after_total - before_total

    print(info)
    print("")
    print("✅ Stars table: main.raw_github_stargazers")
    print(f"✅ Mode: {args.mode}")
    print(f"⏱️  Runtime: {duration_seconds:.2f} seconds")
    print(f"✅ New stars inserted (new rows): {new_rows:,}")
    print(f"✅ Total stars in table: {after_total:,}")

    print("\nPer-repo new stars:")
    for repo_full_name in sorted(after_by_repo.keys()):
        delta = after_by_repo[repo_full_name] - before_by_repo.get(repo_full_name, 0)
        if delta != 0:
            print(f"  +{delta:,}  {repo_full_name}")


if __name__ == "__main__":
    main()
# GitHub Stargazer Pipeline

Dagster-orchestrated pipeline that extracts GitHub stargazers via GraphQL, loads to DuckDB using dlt, runs dbt models, and generates a report.

---

## Quickstart

### 1. Clone the repo
```bash
git clone https://github.com/<YOUR_USERNAME>/github-clay-stargazers.git
cd github-clay-stargazers
```

### 2. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### 3. Configure GitHub token

```bash
cp .env.example .env
```
Edit .env to include:

`GITHUB_TOKEN=your_token_here`

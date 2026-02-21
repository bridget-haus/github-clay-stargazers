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
### 3. Configure GitHub Token

This pipeline uses the GitHub GraphQL API and requires authentication.

#### Step 1 — Create a Personal Access Token

1. Go to: https://github.com/settings/tokens

2. Click **Generate new token** → **Classic token**

3. Configure:
   - **Expiration:** 30–90 days  
   - **Repository access:** Public repositories (read-only)

4. Required permissions:
   - **Metadata:** Read  
   - **Contents:** Read *(optional but safe)*

5. Click **Generate token** and copy it.

---

#### Step 2 — Add token to environment file

```bash
cp .env.example .env
```

Edit .env to include:

`GITHUB_TOKEN=your_token_here`

### 4. Orchestrate Pipeline with Dagster

Start Dagster:

```bash
cd dagster-orechestration
dagster dev
```
Open the UI → http://localhost:3000

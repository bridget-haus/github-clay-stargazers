# GitHub Stargazer Pipeline

Dagster-orchestrated pipeline that extracts GitHub stargazers via GraphQL, loads to DuckDB using dlt, runs dbt models, and generates HTML dashboard.

---

## Quickstart

### 1. Clone the repo


```bash
git clone https://github.com/bridget-haus/github-clay-stargazers.git
cd github-clay-stargazers
```

### 2. Install Python dependencies

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows (PowerShell):

```PowerShell
python -m venv .venv
.venv\Scripts\Activate.ps1
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

#### Step 2 — Copy example env file

macOS / Linux:

```bash
cp .env.example .env
```

Windows (PowerShell):

```PowerShell
copy .env.example .env
```

#### Step 3 — Edit .env file and paste your token

macOS / Linux:

```bash
open .env
```

Windows (PowerShell):

```PowerShell
notepad .env
```

`GITHUB_TOKEN=<YOUR_TOKEN_HERE>`

Save the file

### 4. Orchestrate Pipeline with Dagster

Start Dagster:

```bash
cd dagster_orchestration
dagster dev
```

Open the UI → http://localhost:3000

On the left side panel click 

**Catalog → Select Assets → Materialize**

### 5. View HTML Dashboard

macOS / Linux:

```bash
cd ..
open reports/github_stargazer_dashboard.html
```

Windows (PowerShell):

```PowerShell
cd ..
start reports/github_stargazer_dashboard.html
```


# GitHub Stargazer Pipeline

Dagster-orchestrated pipeline that extracts GitHub stargazers via GraphQL, loads to DuckDB using dlt, runs dbt models, and generates a report.

---

## Quickstart

### 1. Clone the repo


```bash
git clone https://github.com/bridget-haus/github-clay-stargazers.git
cd github-clay-stargazers
```

Only if not logged in to Github already and using Mac:

```bash
brew install gh
gh auth login
```

### 2. Install Python dependencies

```bash
python3 -m venv .venv
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

If you don't see .env

```bash
ls -a
```
shows hidden files

Edit .env file

```bash
open .env
```

`GITHUB_TOKEN=<YOUR_TOKEN_HERE>`

### 4. Orchestrate Pipeline with Dagster

Start Dagster:

```bash
cd dagster-orechestration
dagster dev
```
Open the UI → http://localhost:3000

On the left side panel click 

**Catalog → Select Assets → Materialize**

# CLAUDE CODE INSTRUCTIONS

This is a complete, pre-built Pharmacy Acquisition Intelligence Platform. All source code is already written. Your job is to:

## Step 1: Verify the project structure
Run `find . -type f -name "*.py" -o -name "*.jsx" -o -name "*.yml" -o -name "*.json" | head -50` to confirm all files are present.

## Step 2: Install and run with Docker
```bash
cp .env.example .env  # if .env doesn't exist
docker compose up --build -d
```

Wait for all services to be healthy:
```bash
docker compose ps
docker compose logs backend --tail 20
```

## Step 3: Verify the backend
```bash
curl http://localhost:8000/api/health
```
Should return `{"status":"ok","service":"pharmacy-intel"}`

## Step 4: Verify the frontend
Open http://localhost:3000 in a browser. You should see the login page.

Default credentials: admin@pharma.local / admin123

## Step 5: Run the data pipeline
From the Dashboard, click "Run Pipeline" — OR run it manually:
```bash
docker compose exec backend python scripts/run_pipeline.py
```
**Note**: The first run downloads ~700MB of NPPES data (expands to ~8GB). This takes 30-60 minutes depending on connection speed. Subsequent runs are incremental and much faster.

## Step 6: Fix any issues
If any files have import errors, missing dependencies, or Docker build failures, fix them. The tech stack is:
- Backend: Python 3.11, FastAPI, SQLAlchemy (async), PostgreSQL, asyncpg
- Frontend: React 18, Vite, Tailwind CSS, react-router-dom v6, Leaflet, Recharts
- Infra: Docker Compose with postgres:16-alpine

Common fixes you may need:
- If `npm install` fails, check package.json versions
- If the backend can't connect to DB, check DATABASE_URL in docker-compose.yml
- If NPPES download URL is stale, check https://download.cms.gov/nppes/NPI_Files.html for the latest link and update `backend/app/pipeline/sources/npi.py` NPPES_FULL_URL

## What this platform does
1. Downloads the full U.S. NPI pharmacy registry (~70K pharmacies)
2. Filters for pharmacy taxonomy codes only
3. Normalizes addresses, phone numbers, entity names
4. Flags chains (CVS, Walgreens, Walmart, etc.) and institutional pharmacies
5. Extracts ownership signals for independent pharmacies
6. Optionally enriches with CMS Medicare Part D utilization data
7. Stores everything in PostgreSQL with full-text search
8. Serves a React dashboard with search, filter, map view, export, and change tracking
9. Runs on a weekly schedule to detect changes

## Architecture
- `backend/app/main.py` — FastAPI entry point, creates tables + admin user on startup
- `backend/app/pipeline/orchestrator.py` — Full pipeline runner
- `backend/app/pipeline/sources/npi.py` — NPI download and parse
- `backend/app/pipeline/chain_filter.py` — Chain/independent classification
- `backend/app/api/pharmacies.py` — Main search/filter API
- `frontend/src/pages/Directory.jsx` — Main searchable pharmacy table
- `frontend/src/pages/MapView.jsx` — Leaflet map view
- `frontend/src/pages/Dashboard.jsx` — Stats + pipeline control

# 📡 PokeAPI to Databricks Ingestion

ETL project to extract data from PokeAPI and load it into a Databricks SQL Warehouse.

The project evolved through 3 distinct connection and data loading approaches, each with its own loaders and runners.

## 🏗️ Project Structure

- `src/services/pokeapi.py`: API extraction service.
- `src/parsers/parser.py`: Data transformation and cleaning.
- `src/loaders`: Data loaders to databricks catalog.
- `src/services`: ETL Orchestrators.
- `.env`: Connection configurations (Host, Token, Http Path).

## 🚀 Ingestion Approaches

### 1. SQLAlchemy + Staging Table (Final Solution) ✅
The final and most robust solution, using modern and clean code.

- **Loader**: `src/loaders/databricks_alchemy.py`
- **Runner**: `src/runner_alchemy.py`
- **Execution**: `python main_alchemy.py`
- **Features**:
  - Uses **SQLAlchemy** (via `databricks-sqlalchemy`).
  - **Zero Manual SQL**: Uses Pandas `df.to_sql()`.
  - **Staging Pattern**: Loads to temp table -> Moves to final table.
  - **Idempotent**: Automatic Append if table exists, Create if not.
  - Optimized for **Unity Catalog** (`USE CATALOG`).

---

### 2. SQL Connector (Legacy) ⚠️
The intermediate "rustic" solution, functional but verbose.

- **Loader**: `src/loaders/databricks_sql.py`
- **Runner**: `src/runner_sql.py`
- **Execution**: `python main_sql.py`
- **Features**:
  - Uses `databricks-sql-connector` purely.
  - Builds `INSERT INTO` strings manually.
  - Requires manual cursor and commit management.
  - Useful as a fallback if SQLAlchemy fails.

---

### 3. Databricks Connect (Deprecated) ❌
The first attempt, based on Spark Clusters.

- **Loader**: `src/loaders/databricks.py`
- **Runner**: `src/runner.py`
- **Status**: **Does not work with SQL Warehouse**.
- **Issue**: Databricks Connect requires a traditional Cluster ID for Spark Session, which does not exist in Serverless SQL Warehouse environments.

## 🛠️ How to Run (Final Solution)

```bash
# 1. Setup environment
pip install -r requirements.txt

# 2. Configure .env
# DATABRICKS_SERVER_HOSTNAME=...
# DATABRICKS_HTTP_PATH=...
# DATABRICKS_TOKEN=...
# DATABRICKS_CATALOG=...

# 3. Run Pipeline
python main_alchemy.py
```

## 📊 Result
Data is loaded into the table:
`{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.pokemon`

"""
berka_pipeline
--------------
Orchestrates the Berka bank data warehouse, end to end:

    ingest_raw -> dbt_deps -> dbt_seed -> dbt_staging -> dbt_marts
    (CSV->raw)    (packages)   (lookups)   (3 views +     (star schema
                                            raw tests)     + tests)

Airflow does no transformation itself. It only runs each step in order,
retries it on failure, stops the pipeline if a step fails, and keeps a
history and logs of every run.

Paths are the ones INSIDE the Docker container, not on Windows:
    /usr/local/airflow                 = the project root (Astro's AIRFLOW_HOME)
    /usr/local/airflow/include         = the include/ folder (mounted from Windows)
    /usr/local/airflow/dbt_venv        = the dbt virtualenv built by the Dockerfile
"""

from datetime import datetime, timedelta

# Airflow 3 moved these imports; the fallbacks keep the DAG working on
# Airflow 2 as well, whichever runtime version Astro installed.
try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------------
# Paths inside the container
# ---------------------------------------------------------------------------
AIRFLOW_HOME = "/usr/local/airflow"

# The dbt venv from the Dockerfile. It also contains DuckDB (installed by
# dbt-duckdb), so its Python can run ingest.py too: no extra packages needed
# in Airflow's own environment.
DBT_VENV_BIN = f"{AIRFLOW_HOME}/dbt_venv/bin"

DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/include/berka_dbt"
INGEST_SCRIPT = f"{AIRFLOW_HOME}/include/ingestion_raw/ingest.py"


# ---------------------------------------------------------------------------
# Settings applied to every task
# ---------------------------------------------------------------------------
default_args = {
    "owner": "donia",
    # If a task fails, try again twice, waiting 2 minutes each time.
    # Helps with temporary problems (e.g. a network blip during dbt deps).
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="berka_pipeline",
    description="Load Berka CSVs into DuckDB and build the star schema with dbt",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",      # run once a day, at midnight UTC
    catchup=False,          # don't "catch up" on every missed day since start_date
    tags=["berka", "dbt", "duckdb"],
) as dag:

    # ---------------------------------------------------------------- RAW
    # 1. Extract + Load: the 8 CSV files -> raw schema in DuckDB.
    #    ingest.py exits with code 1 if a row count is wrong; Airflow sees
    #    the non-zero exit code, marks the task as failed and stops here.
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=f"{DBT_VENV_BIN}/python {INGEST_SCRIPT}",
    )

    # 2. Install dbt packages (dbt_utils) listed in packages.yml.
    #    dbt_packages/ is not in Git, so a fresh environment must download
    #    it before any model that calls dbt_utils can compile.
    #    "cd" first: dbt looks for dbt_project.yml and profiles.yml in the
    #    current folder.
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt deps",
    )

    # ----------------------------------------------------------- REFERENCE
    # 3. Load the 4 code -> label lookup tables (seeds/*.csv).
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt seed",
    )

    # ------------------------------------------------------------ STAGING
    # 4. Build the 3 staging views and run their tests.
    #    --select staging = everything in models/staging/, which also
    #    includes the source tests in _sources.yml (unique / not_null on
    #    the raw business keys), so raw data is validated here too.
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt build --select staging",
    )

    # -------------------------------------------------------- WAREHOUSE
    # 5. Build the star schema (4 dimensions + fact) and run their tests,
    #    including the row-count check against raw.trans.
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt build --select marts",
    )

    # The order: each step starts only after the previous one SUCCEEDED.
    (
        ingest_raw
        >> dbt_deps
        >> dbt_seed
        >> dbt_staging
        >> dbt_marts
    )
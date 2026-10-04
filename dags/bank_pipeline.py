
from datetime import datetime, timedelta

# Airflow 3 moved these imports; the fallbacks keep the DAG working on Airflow 2.
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
DBT_VENV_BIN = f"{AIRFLOW_HOME}/dbt_venv/bin"          # dbt, python and duckdb
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/include/berka_dbt"
INGEST_SCRIPT = f"{AIRFLOW_HOME}/include/ingestion_raw/ingest.py"

# Every dbt command runs from the dbt project folder (dbt_project.yml, profiles.yml)
DBT = f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt"


default_args = {
    "owner": "donia",
    "retries": 2,                          # retry twice on failure...
    "retry_delay": timedelta(minutes=2),   # ...two minutes apart
}


with DAG(
    dag_id="berka_pipeline",
    description="Build and test the Berka warehouse, layer by layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["berka", "dbt", "duckdb", "data-quality"],
) as dag:

    # --------------------------------------------------------------- SETUP
    # dbt_utils (dbt_packages/ is not in Git, so it is installed every run)
    install_dbt_packages = BashOperator(
        task_id="install_dbt_packages",
        bash_command=f"{DBT} deps",
    )

    # ----------------------------------------------------------------- RAW
    # 8 CSV files -> raw schema (all text, untouched). The script prints the
    # row count of every file and exits with code 1 if one differs from
    # the dataset documentation.
    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command=f"{DBT_VENV_BIN}/python {INGEST_SCRIPT}",
    )

    # Tests on the raw business keys (unique, not_null).
    # cautious: only tests whose parents are ALL raw sources, so the
    # fact-vs-source reconciliation test waits for the warehouse layer.
    test_raw_data = BashOperator(
        task_id="test_raw_data",
        bash_command=f'{DBT} test --select "source:*" --indirect-selection=cautious',
    )

    # ----------------------------------------------------------- REFERENCE
    # Code -> label lookup tables used by the warehouse dimensions
    load_reference_seeds = BashOperator(
        task_id="load_reference_seeds",
        bash_command=f"{DBT} seed",
    )

    # ------------------------------------------------------------- STAGING
    # The 3 staging views (models only, tests run in the next task)
    build_staging = BashOperator(
        task_id="build_staging",
        bash_command=f"{DBT} run --select staging",
    )

    # Tests on the staging models only (source tests already ran above,
    # so sources are excluded here)
    test_staging = BashOperator(
        task_id="test_staging",
        bash_command=(
            f'{DBT} test --select staging --exclude "source:*" '
            "--indirect-selection=cautious"
        ),
    )

    # ----------------------------------------------------------- WAREHOUSE
    # The star schema: 4 dimensions + fact_transaction (models only)
    build_warehouse = BashOperator(
        task_id="build_warehouse",
        bash_command=f"{DBT} run --select marts",
    )

    # Tests on the star schema, including relationships and the
    # fact-vs-source row-count reconciliation
    test_warehouse = BashOperator(
        task_id="test_warehouse",
        bash_command=f"{DBT} test --select marts",
    )

    # --------------------------------------------------------------- ORDER
    # Each task starts only after the previous one SUCCEEDED.
    # Tasks run one after another: DuckDB allows one writer at a time.
    (
        install_dbt_packages
        >> load_raw_data
        >> test_raw_data
        >> load_reference_seeds
        >> build_staging
        >> test_staging
        >> build_warehouse
        >> test_warehouse
    )
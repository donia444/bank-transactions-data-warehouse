"""
ingest.py
---------
Loads the 8 Berka CSV files into the `raw` schema of the DuckDB warehouse.

Location in the project:
    include/
    ├── data/raw/*.csv          <- input  (8 source files)
    ├── ingestion_raw/ingest.py <- this script
    └── warehouse/berka.duckdb  <- output (created if missing)

Why this script exists
    The raw layer is an exact, untouched copy of the source. dbt reads from
    here, never from the CSV files directly:
        CSV  ->  raw.<table>  ->  stg_<table>  ->  dim_* / fact_*

Design decisions
    1. Paths are computed from THIS file's location, not from the terminal's
       current folder. The script then works from any folder, on Windows,
       and inside the Airflow Docker container.
    2. Every column is loaded as VARCHAR (text). The raw layer must not
       interpret the data; type casting is a transformation and belongs in
       dbt staging, where it is visible, documented and tested.
       (Example: district 69 has '?' in numeric columns. Letting DuckDB
       guess types could fail or mis-type the whole column.)
    3. Tables are re-created on every run (CREATE OR REPLACE), so the script
       is idempotent: running it twice never duplicates rows.
    4. Row counts are checked against the official documentation. A load
       that silently drops rows is the worst kind of bug, because everything
       downstream still "works". The script fails loudly instead.

Run (from any folder):
    python include/ingestion_raw/ingest.py
"""

from pathlib import Path   # OS-independent path handling
import sys                  # sys.exit(1) signals failure to callers (e.g. Airflow)

import duckdb               # embedded analytical database, no server needed


# ---------------------------------------------------------------------------
# Paths (decision 1)
# ---------------------------------------------------------------------------
# __file__            -> .../include/ingestion_raw/ingest.py
# .resolve()          -> absolute version of that path
# .parent             -> .../include/ingestion_raw
# .parent.parent      -> .../include
INCLUDE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = INCLUDE_DIR / "data" / "raw"              # the 8 source CSVs
DB_PATH = INCLUDE_DIR / "warehouse" / "berka.duckdb"     # same file profiles.yml points to




# ---------------------------------------------------------------------------
EXPECTED_ROWS = {
    "account":  4_500,
    "card":     892,
    "client":   5_369,
    "disp":     5_369,
    "district": 77,
    "loan":     682,
    "order":    6_471,
    "trans":    1_056_320,
}


def load_table(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    """Load one CSV file into raw.<table_name> and return its row count."""

    csv_path = RAW_DATA_DIR / f"{table_name}.csv"

    # Fail early with a clear message, instead of a confusing DuckDB error
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing source file: {csv_path}")

    # The table name is wrapped in double quotes: "order" is a reserved SQL
    # word (ORDER BY), so without quotes that one table would fail.
    #
    # read_csv options:
    #   delim=';'         Berka files are semicolon-separated, not comma
    #   header=true       first line holds the column names
    #   all_varchar=true  load every column as text (decision 2)
    #
    # as_posix() converts Windows backslashes (C:\...) to forward slashes
    # (C:/...), which DuckDB accepts on every operating system.
    con.execute(f"""
        CREATE OR REPLACE TABLE raw."{table_name}" AS
        SELECT *
        FROM read_csv(
            '{csv_path.as_posix()}',
            delim = ';',
            header = true,
            all_varchar = true
        )
    """)

    # fetchone() returns a tuple such as (4500,); [0] extracts the number
    return con.execute(f'SELECT count(*) FROM raw."{table_name}"').fetchone()[0]


def main() -> None:
    # Create include/warehouse/ if it doesn't exist yet;
    # DuckDB creates the database file, but not missing folders.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Opens the database file (creates it on the first run)
    con = duckdb.connect(str(DB_PATH))

    # A schema is a "folder" inside the database. Raw data gets its own,
    # separate from staging and marts, which dbt will create later.
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    failures = []   # collect every mismatch, then report them all together

    try:
        for table_name, expected in EXPECTED_ROWS.items():
            actual = load_table(con, table_name)
            status = "OK" if actual == expected else "MISMATCH"
            print(f"{table_name:<10} loaded {actual:>10,} rows  "
                  f"(expected {expected:>10,})  {status}")
            if actual != expected:
                failures.append(table_name)
    finally:
        # Always release the file, even if a load crashed halfway,
        # so dbt (or a rerun) can open the database afterwards.
        con.close()

    # Exit code 1 tells any caller (later: Airflow) that this step failed,
    # so the pipeline stops instead of building models on incomplete data.
    if failures:
        print(f"\nRow count mismatch in: {', '.join(failures)}")
        sys.exit(1)

    print(f"\nAll tables loaded into {DB_PATH}")


# Run main() only when the file is executed directly,
# not when another script imports it.
if __name__ == "__main__":
    main()
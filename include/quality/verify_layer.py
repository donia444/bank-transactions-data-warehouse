

from pathlib import Path
import sys

import duckdb


# include/quality/verify_layer.py -> .parent.parent = include/
INCLUDE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = INCLUDE_DIR / "warehouse" / "berka.duckdb"


# ---------------------------------------------------------------------------
# Checks per layer: (description, SQL returning ONE value, expected value)
# An expected value can itself be SQL (a string starting with "select"):
# then it is computed from the previous layer instead of hard-coded.
# ---------------------------------------------------------------------------
CHECKS = {

    # RAW: every source file landed in full (counts from the documentation)
    "raw": [
        ("raw.account loaded",  "select count(*) from raw.account",   4_500),
        ("raw.card loaded",     "select count(*) from raw.card",      892),
        ("raw.client loaded",   "select count(*) from raw.client",    5_369),
        ("raw.disp loaded",     "select count(*) from raw.disp",      5_369),
        ("raw.district loaded", "select count(*) from raw.district",  77),
        ("raw.loan loaded",     "select count(*) from raw.loan",      682),
        ("raw.order loaded",    'select count(*) from raw."order"',   6_471),
        ("raw.trans loaded",    "select count(*) from raw.trans",     1_056_320),
    ],

    # STAGING: no row lost, and the cleaning rules actually applied
    "staging": [
        ("stg_trans kept every raw row",
         "select count(*) from staging.stg_trans",
         "select count(*) from raw.trans"),
        ("stg_account kept every raw row",
         "select count(*) from staging.stg_account",
         "select count(*) from raw.account"),
        ("stg_district kept every raw row",
         "select count(*) from staging.stg_district",
         "select count(*) from raw.district"),
        ("all transaction dates converted (no NULL dates)",
         "select count(*) from staging.stg_trans where transaction_date is null",
         0),
        ("no blank codes left (all unified to N/A)",
         """select count(*) from staging.stg_trans
            where trim(transaction_type_code) = ''
               or trim(operation_code) = ''
               or trim(k_symbol_code) = ''""",
         0),
        ("only Jesenik has unknown 1995 statistics",
         """select count(*) from staging.stg_district
            where unemployment_rate_1995 is null or crimes_1995 is null""",
         1),
    ],

    # WAREHOUSE: every table has the expected grain and nothing is orphaned
    "warehouse": [
        ("dim_date covers 1993-01-01 to 1998-12-31 (2,191 days)",
         "select count(*) from marts.dim_date", 2_191),
        ("dim_account has one row per account",
         "select count(*) from marts.dim_account",
         "select count(*) from staging.stg_account"),
        ("dim_district has one row per district",
         "select count(*) from marts.dim_district",
         "select count(*) from staging.stg_district"),
        ("dim_transaction_profile has 15 combinations",
         "select count(*) from marts.dim_transaction_profile", 15),
        ("every profile has a known direction",
         """select count(*) from marts.dim_transaction_profile
            where direction not in ('CREDIT', 'DEBIT')""",
         0),
        ("fact_transaction has one row per source transaction",
         "select count(*) from marts.fact_transaction",
         "select count(*) from raw.trans"),
        ("no fact row with a missing dimension key",
         """select count(*) from marts.fact_transaction
            where transaction_date_key is null
               or account_key is null
               or branch_district_key is null
               or transaction_profile_key is null""",
         0),
    ],
}


def value(con, sql_or_value):
    """Run SQL and return its single value, or return a constant as-is."""
    if isinstance(sql_or_value, str) and sql_or_value.strip().lower().startswith("select"):
        return con.execute(sql_or_value).fetchone()[0]
    return sql_or_value


def main() -> None:
    layer = sys.argv[1] if len(sys.argv) > 1 else ""
    if layer not in CHECKS:
        print(f"Usage: python verify_layer.py [{' | '.join(CHECKS)}]")
        sys.exit(2)

    # read_only: confirmation only looks at the data, never changes it
    con = duckdb.connect(str(DB_PATH), read_only=True)
    failed = 0

    print(f"POST-EXECUTION CHECKS: {layer.upper()} LAYER")
    print("-" * 82)
    try:
        for description, check_sql, expected in CHECKS[layer]:
            actual = value(con, check_sql)
            target = value(con, expected)
            ok = actual == target
            failed += 0 if ok else 1
            mark = "PASS" if ok else "FAIL"
            print(f"[{mark}] {description:<56} expected {target:>10,}  got {actual:>10,}")

        # An informative business number for the warehouse layer
        if layer == "warehouse":
            net = con.execute("select sum(signed_amount) from marts.fact_transaction").fetchone()[0]
            print(f"[INFO] net money flow (sum of signed_amount): {net:,.2f}")
    finally:
        con.close()

    print("-" * 82)
    if failed:
        print(f"{failed} check(s) FAILED: the {layer} layer is not as expected.")
        sys.exit(1)
    print(f"All {len(CHECKS[layer])} checks passed: the {layer} layer is confirmed.")


if __name__ == "__main__":
    main()
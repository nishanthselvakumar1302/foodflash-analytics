

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import os
import sys
import time
import math
import argparse
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.errors import DatabaseError, ProgrammingError

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/extract_load.log", mode="a"),
    ],
)
# Silence noisy third-party loggers
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector.cursor").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

log = logging.getLogger("foodflash.etl")


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
load_dotenv()   # loads from .env file if present

# ── PostgreSQL ──────────────────────────────
PG_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     os.getenv("PG_PORT",     "5432"),
    "database": os.getenv("PG_DATABASE", "foodflash_db"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

# ── Snowflake ───────────────────────────────
SF_CONFIG = {
    "account":   os.getenv("SF_ACCOUNT",   ""),    
    "user":      os.getenv("SF_USER",      "FOODFLASH_USER"),
    "password":  os.getenv("SF_PASSWORD",  ""),    
    "database":  os.getenv("SF_DATABASE",  "FOODFLASH_DB"),
    "warehouse": os.getenv("SF_WAREHOUSE", "FOODFLASH_WH"),
    "schema":    os.getenv("SF_SCHEMA",    "RAW"),
    "role":      os.getenv("SF_ROLE",      "FOODFLASH_ROLE"),
}

# ── Load settings ───────────────────────────
CHUNK_SIZE = 10_000    # rows per Snowflake write_pandas call
PG_FETCH_CHUNK = 50_000  # rows per PostgreSQL fetch for very large tables

# ── Table config ────────────────────────────
# Each entry: (table_name, primary_key, expected_min_rows)
TABLES = [
    ("customers",   "customer_id",   1_000),
    ("restaurants", "restaurant_id",   100),
    ("riders",      "rider_id",         50),
    ("orders",      "order_id",      5_000),
    ("order_items", "item_id",      10_000),
]


# ─────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────

def print_banner(text: str) -> None:
    width = 56
    print("\n" + "═" * width)
    print(f"  {text}")
    print("═" * width)


def print_section(text: str) -> None:
    print(f"\n── {text} {'─' * max(0, 50 - len(text))}")


def print_progress(label: str, current: int, total: int) -> None:
    pct   = min(int((current / total) * 38), 38)
    bar   = "█" * pct + "░" * (38 - pct)
    pct_n = int(current / total * 100)
    print(f"\r  [{bar}] {pct_n:>3}%  {current:>8,} / {total:,}  {label}",
          end="", flush=True)


def elapsed_str(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def human_size(num_rows: int, bytes_per_row: int = 200) -> str:
    """Rough estimate of data size."""
    b = num_rows * bytes_per_row
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"~{b:.0f} {unit}"
        b /= 1024
    return f"~{b:.0f} GB"


# ─────────────────────────────────────────────
# POSTGRESQL — EXTRACT
# ─────────────────────────────────────────────

def get_pg_engine():
    """Create and validate a SQLAlchemy engine for PostgreSQL."""
    conn = (
        f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    )
    try:
        engine = create_engine(conn, echo=False, future=True)
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        log.info(
            f"PostgreSQL connected → {PG_CONFIG['host']}:{PG_CONFIG['port']}"
            f"/{PG_CONFIG['database']} as {PG_CONFIG['user']}"
        )
        return engine
    except SQLAlchemyError as e:
        log.error(f"PostgreSQL connection failed:\n  {e}")
        log.error(
            "Fix: check PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD in .env"
        )
        sys.exit(1)


def get_pg_row_count(engine, table: str) -> int:
    """Returns exact row count for a PostgreSQL table."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()


def extract_table(engine, table: str) -> pd.DataFrame:
    """
    Reads an entire table from PostgreSQL into a pandas DataFrame.
    Uses server-side cursor (stream_results=True) for memory efficiency
    on large tables like order_items.
    """
    t0 = time.time()
    row_count = get_pg_row_count(engine, table)
    log.info(f"  Extracting {table} → {row_count:,} rows {human_size(row_count)}")

    if row_count == 0:
        log.warning(f"  Table '{table}' is empty — skipping")
        return pd.DataFrame()

    try:
        # Use chunked reading for large tables to avoid memory spikes
        if row_count > PG_FETCH_CHUNK:
            chunks = []
            chunk_count = math.ceil(row_count / PG_FETCH_CHUNK)
            for i, chunk in enumerate(
                pd.read_sql_table(
                    table,
                    con=engine,
                    chunksize=PG_FETCH_CHUNK,
                )
            ):
                chunks.append(chunk)
                print_progress(
                    f"extracting {table}",
                    min((i + 1) * PG_FETCH_CHUNK, row_count),
                    row_count,
                )
            print()
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_sql_table(table, con=engine)

        elapsed = time.time() - t0
        log.info(
            f"  ✓ {table}: {len(df):,} rows extracted in {elapsed_str(elapsed)}"
            f"  cols={list(df.columns)}"
        )
        return df

    except SQLAlchemyError as e:
        log.error(f"  ✗ Failed to extract '{table}': {e}")
        sys.exit(1)


# ─────────────────────────────────────────────
# DATA PREP — before loading to Snowflake
# ─────────────────────────────────────────────

def prepare_for_snowflake(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Cleans and transforms a DataFrame before loading to Snowflake:
    1. Uppercase all column names (Snowflake convention)
    2. Convert timezone-naive timestamps to UTC strings
    3. Convert boolean columns properly
    4. Add _LOADED_AT audit column
    5. Replace NaN with None (NULL in Snowflake)
    """
    # 1. Uppercase column names — Snowflake stores identifiers in uppercase
    df.columns = [col.upper() for col in df.columns]

    # 2. Handle timestamp columns — convert to strings for safe loading
    timestamp_cols = df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # 3. Handle date columns
    date_cols = [c for c in df.columns if df[c].dtype == "object"
                 and any(k in c.lower() for k in ["_at", "date", "_date", "opened"])]
    # Already handled as strings — no conversion needed for object types

    # 4. Add audit timestamp column
    df["_LOADED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 5. Boolean handling — Snowflake write_pandas handles booleans correctly
    bool_cols = df.select_dtypes(include=["bool"]).columns
    for col in bool_cols:
        df[col] = df[col].astype(bool)

    # 6. Replace NaN/NaT with None so Snowflake stores NULL
    df = df.where(pd.notnull(df), other=None)

    return df


# ─────────────────────────────────────────────
# SNOWFLAKE — LOAD
# ─────────────────────────────────────────────

def get_sf_connection():
    """Create and validate a Snowflake connection."""
    # Validate required config
    missing = [k for k, v in SF_CONFIG.items() if not v]
    if missing:
        log.error(
            f"Missing Snowflake config: {missing}\n"
            "  Add these to your .env file:\n"
            "  SF_ACCOUNT, SF_USER, SF_PASSWORD, SF_DATABASE, SF_WAREHOUSE, SF_ROLE"
        )
        sys.exit(1)

    try:
        conn = snowflake.connector.connect(
            account   = SF_CONFIG["account"],
            user      = SF_CONFIG["user"],
            password  = SF_CONFIG["password"],
            database  = SF_CONFIG["database"],
            warehouse = SF_CONFIG["warehouse"],
            schema    = SF_CONFIG["schema"],
            role      = SF_CONFIG["role"],
            # Performance settings
            session_parameters={
                "QUERY_TAG": "foodflash_elt_pipeline",
                "STATEMENT_TIMEOUT_IN_SECONDS": "3600",
            },
        )
        # Quick test
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        row = cursor.fetchone()
        log.info(
            f"Snowflake connected → user={row[0]}  role={row[1]}  warehouse={row[2]}"
        )
        log.info(
            f"  Target: {SF_CONFIG['database']}.{SF_CONFIG['schema']}"
        )
        return conn

    except DatabaseError as e:
        log.error(f"Snowflake connection failed:\n  {e}")
        log.error(
            "Common fixes:\n"
            "  1. Check SF_ACCOUNT format: abc12345.ap-south-1.aws (no https://)\n"
            "  2. Check SF_USER and SF_PASSWORD are correct\n"
            "  3. Make sure FOODFLASH_WH warehouse is not suspended manually\n"
            "  4. Check your IP is not blocked by Snowflake network policy"
        )
        sys.exit(1)


def get_sf_row_count(conn, table: str) -> int:
    """Returns current row count for a Snowflake table."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {SF_CONFIG['database']}.{SF_CONFIG['schema']}.{table.upper()}"
        )
        return cursor.fetchone()[0]
    except ProgrammingError:
        return 0   # table may not exist yet


def truncate_table(conn, table: str) -> None:
    """Truncates a Snowflake table before full reload."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"TRUNCATE TABLE IF EXISTS "
            f"{SF_CONFIG['database']}.{SF_CONFIG['schema']}.{table.upper()}"
        )
        log.info(f"  Truncated {table.upper()} (replace mode)")
    except ProgrammingError as e:
        log.warning(f"  Could not truncate {table}: {e} — continuing")


def load_table_to_snowflake(
    conn,
    df: pd.DataFrame,
    table: str,
    mode: str = "replace",
    chunk_size: int = CHUNK_SIZE,
) -> dict:
    """
    Loads a DataFrame into a Snowflake table using write_pandas.
    Splits into chunks for large tables and shows a progress bar.

    Args:
        conn       : Snowflake connection object
        df         : DataFrame to load
        table      : Target table name (uppercase in Snowflake)
        mode       : 'replace' (truncate then insert) or 'append'
        chunk_size : Rows per write_pandas call

    Returns:
        dict with load stats: rows_loaded, chunks, elapsed_seconds
    """
    if df.empty:
        log.warning(f"  {table}: DataFrame is empty — skipping load")
        return {"rows_loaded": 0, "chunks": 0, "elapsed_seconds": 0}

    t0 = time.time()
    table_upper = table.upper()
    total_rows  = len(df)
    n_chunks    = math.ceil(total_rows / chunk_size)

    # Prepare DataFrame for Snowflake
    df_clean = prepare_for_snowflake(df.copy(), table)

    # Truncate first if replacing
    if mode == "replace":
        truncate_table(conn, table)

    log.info(
        f"  Loading {table_upper} → {total_rows:,} rows"
        f"  in {n_chunks} chunk(s) of {chunk_size:,}"
    )

    rows_loaded = 0
    errors      = []

    for i in range(n_chunks):
        chunk_start = i * chunk_size
        chunk_end   = min(chunk_start + chunk_size, total_rows)
        chunk_df    = df_clean.iloc[chunk_start:chunk_end]

        try:
            success, n_chunks_sf, n_rows, output = write_pandas(
                conn=conn,
                df=chunk_df,
                table_name=table_upper,
                database=SF_CONFIG["database"],
                schema=SF_CONFIG["schema"],
                auto_create_table=False,   # tables pre-created in setup SQL
                overwrite=False,           # we handle truncate separately
                quote_identifiers=False,
                compression="gzip",        # compress for faster upload
            )

            if success:
                rows_loaded += n_rows
                print_progress(
                    f"uploading {table_upper}",
                    rows_loaded,
                    total_rows,
                )
            else:
                errors.append(f"Chunk {i+1}: write_pandas returned success=False")
                log.error(f"  Chunk {i+1} failed for {table_upper}: {output}")

        except (DatabaseError, ProgrammingError) as e:
            log.error(f"  ✗ Chunk {i+1} error loading {table_upper}: {e}")
            errors.append(str(e))
            # Continue with remaining chunks rather than aborting
            continue

    print()  # newline after progress bar

    elapsed = time.time() - t0

    if errors:
        log.warning(f"  {table_upper}: {len(errors)} chunk error(s) — check logs")

    log.info(
        f"  ✓ {table_upper}: {rows_loaded:,} rows loaded"
        f"  in {elapsed_str(elapsed)}"
        f"  ({rows_loaded / elapsed:.0f} rows/sec)"
    )

    return {
        "rows_loaded":     rows_loaded,
        "chunks":          n_chunks,
        "elapsed_seconds": elapsed,
        "errors":          errors,
    }


# ─────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────

def verify_load(conn, pg_engine, tables: list, results: dict) -> bool:
    """
    Compares PostgreSQL row counts vs Snowflake row counts.
    Prints a summary table. Returns True if all counts match.
    """
    print_section("Verification — PostgreSQL vs Snowflake row counts")

    header = f"  {'Table':<20} {'PostgreSQL':>12} {'Snowflake':>12} {'Match':>8} {'Time':>8}"
    print(header)
    print("  " + "─" * 62)

    all_ok = True

    for table, pk, _ in tables:
        if table not in results:
            continue

        pg_count  = get_pg_row_count(pg_engine, table)
        sf_count  = get_sf_row_count(conn, table)
        match     = "✓" if pg_count == sf_count else "✗ MISMATCH"
        elapsed   = elapsed_str(results[table].get("elapsed_seconds", 0))

        if pg_count != sf_count:
            all_ok = False

        print(
            f"  {table:<20} {pg_count:>12,} {sf_count:>12,} {match:>8} {elapsed:>8}"
        )

    print("  " + "─" * 62)
    return all_ok


def run_snowflake_checks(conn) -> None:
    """Runs basic data quality checks directly in Snowflake."""
    print_section("Data quality spot-checks in Snowflake")

    checks = [
        (
            "Null order_id in RAW.ORDERS",
            "SELECT COUNT(*) FROM FOODFLASH_DB.RAW.ORDERS WHERE ORDER_ID IS NULL",
            0,
        ),
        (
            "Null customer_id in RAW.ORDERS",
            "SELECT COUNT(*) FROM FOODFLASH_DB.RAW.ORDERS WHERE CUSTOMER_ID IS NULL",
            0,
        ),
        (
            "Invalid order status",
            "SELECT COUNT(*) FROM FOODFLASH_DB.RAW.ORDERS "
            "WHERE STATUS NOT IN ('delivered','cancelled','pending')",
            0,
        ),
        (
            "Negative order_amount",
            "SELECT COUNT(*) FROM FOODFLASH_DB.RAW.ORDERS WHERE ORDER_AMOUNT <= 0",
            0,
        ),
        (
            "Orphan order_items (no matching order)",
            "SELECT COUNT(*) FROM FOODFLASH_DB.RAW.ORDER_ITEMS oi "
            "LEFT JOIN FOODFLASH_DB.RAW.ORDERS o ON oi.ORDER_ID = o.ORDER_ID "
            "WHERE o.ORDER_ID IS NULL",
            0,
        ),
    ]

    cursor = conn.cursor()
    all_passed = True

    for check_name, query, expected in checks:
        try:
            cursor.execute(query)
            result = cursor.fetchone()[0]
            passed = result == expected
            icon   = "✓" if passed else "✗"
            if not passed:
                all_passed = False
            print(f"  {icon}  {check_name:<45} → {result:,}")
        except ProgrammingError as e:
            print(f"  ?  {check_name:<45} → ERROR: {e}")

    print()
    if all_passed:
        log.info("  All data quality checks passed ✓")
    else:
        log.warning("  Some checks failed — review the results above")


def log_pipeline_run(conn, run_stats: dict) -> None:
    """Writes pipeline run metadata to Snowflake AUDIT schema."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FOODFLASH_DB.AUDIT.PIPELINE_RUNS (
                RUN_ID       VARCHAR(50),
                RUN_AT       TIMESTAMP_NTZ,
                STATUS       VARCHAR(20),
                TABLES_LOADED INTEGER,
                TOTAL_ROWS   INTEGER,
                ELAPSED_SEC  FLOAT,
                NOTES        VARCHAR(500)
            )
        """)
        cursor.execute("""
            INSERT INTO FOODFLASH_DB.AUDIT.PIPELINE_RUNS
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            run_stats["run_id"],
            run_stats["run_at"],
            run_stats["status"],
            run_stats["tables_loaded"],
            run_stats["total_rows"],
            run_stats["elapsed_seconds"],
            run_stats["notes"],
        ))
        conn.commit()
        log.info("  Pipeline run logged to AUDIT.PIPELINE_RUNS")
    except Exception as e:
        log.warning(f"  Could not write audit log: {e} — continuing")


# ─────────────────────────────────────────────
# CLI ARGUMENT PARSING
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="FoodFlash: Extract from PostgreSQL → Load to Snowflake RAW"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Load only this table (e.g. --table orders). Default: all tables.",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace: truncate then insert (default). append: add new rows only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract from PostgreSQL only — skip Snowflake load. Useful for testing.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Rows per Snowflake write batch (default: {CHUNK_SIZE:,})",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    run_id  = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    run_at  = datetime.utcnow()
    t_total = time.time()

    print_banner("FoodFlash Analytics — Extract & Load to Snowflake")
    log.info(f"Run ID  : {run_id}")
    log.info(f"Mode    : {args.mode}")
    log.info(f"Dry run : {args.dry_run}")
    if args.table:
        log.info(f"Table   : {args.table} (single table mode)")

    # Determine which tables to process
    tables_to_process = (
        [t for t in TABLES if t[0] == args.table]
        if args.table
        else TABLES
    )
    if not tables_to_process:
        log.error(f"Table '{args.table}' not found. Valid: {[t[0] for t in TABLES]}")
        sys.exit(1)

    # ── EXTRACT ──────────────────────────────
    print_section("Phase 1 — Extract from PostgreSQL")
    pg_engine  = get_pg_engine()
    dataframes = {}

    for table, pk, min_rows in tables_to_process:
        df = extract_table(pg_engine, table)
        if len(df) < min_rows and not df.empty:
            log.warning(
                f"  {table}: only {len(df):,} rows — "
                f"expected at least {min_rows:,}. Run generate_data.py first?"
            )
        dataframes[table] = df

    total_extracted = sum(len(df) for df in dataframes.values())
    log.info(f"\n  Extraction complete — {total_extracted:,} total rows across {len(dataframes)} tables")

    if args.dry_run:
        print_section("Dry run — skipping Snowflake load")
        log.info("  Extracted DataFrames are ready. Remove --dry-run to load.")
        for name, df in dataframes.items():
            log.info(f"  {name:<20} {len(df):,} rows  {list(df.columns)}")
        return

    # ── LOAD ─────────────────────────────────
    print_section("Phase 2 — Load to Snowflake RAW schema")
    sf_conn  = get_sf_connection()
    results  = {}

    for table, pk, _ in tables_to_process:
        df = dataframes.get(table, pd.DataFrame())
        if df.empty:
            log.warning(f"  {table}: no data to load — skipping")
            results[table] = {"rows_loaded": 0, "chunks": 0, "elapsed_seconds": 0}
            continue

        log.info(f"\n  ── {table.upper()} ──")
        stats = load_table_to_snowflake(
            conn=sf_conn,
            df=df,
            table=table,
            mode=args.mode,
            chunk_size=args.chunk_size,
        )
        results[table] = stats

    # ── VERIFY ───────────────────────────────
    print_section("Phase 3 — Verification")
    all_ok = verify_load(sf_conn, pg_engine, tables_to_process, results)
    run_snowflake_checks(sf_conn)

    # ── AUDIT ────────────────────────────────
    total_rows_loaded = sum(r.get("rows_loaded", 0) for r in results.values())
    elapsed_total     = time.time() - t_total
    status            = "SUCCESS" if all_ok else "PARTIAL_FAILURE"

    log_pipeline_run(sf_conn, {
        "run_id":          run_id,
        "run_at":          run_at,
        "status":          status,
        "tables_loaded":   len([r for r in results.values() if r.get("rows_loaded", 0) > 0]),
        "total_rows":      total_rows_loaded,
        "elapsed_seconds": elapsed_total,
        "notes":           f"mode={args.mode} tables={[t[0] for t in tables_to_process]}",
    })

    # ── SUMMARY ──────────────────────────────
    print_section("Pipeline summary")
    print(f"\n  Run ID          : {run_id}")
    print(f"  Status          : {status}")
    print(f"  Tables loaded   : {len(results)}")
    print(f"  Total rows      : {total_rows_loaded:,}")
    print(f"  Total time      : {elapsed_str(elapsed_total)}")
    print(f"  Avg throughput  : {total_rows_loaded / elapsed_total:.0f} rows/sec")

    print("\n  Per-table breakdown:")
    for table, stats in results.items():
        rows    = stats.get("rows_loaded", 0)
        secs    = stats.get("elapsed_seconds", 0)
        rate    = f"{rows / secs:.0f} r/s" if secs > 0 else "—"
        err_ct  = len(stats.get("errors", []))
        err_str = f"  ⚠ {err_ct} error(s)" if err_ct else ""
        print(f"  {'✓' if not err_ct else '⚠'}  {table:<20} {rows:>8,} rows"
              f"  {elapsed_str(secs):>7}  {rate:>10}{err_str}")

    if all_ok:
        print("\n  ✓ All row counts match — load successful!")
        print("  Next step: run   dbt run   inside dbt/foodflash_dbt/")
    else:
        print("\n  ⚠ Row count mismatch detected — check logs/extract_load.log")
        sys.exit(1)

    print("═" * 56)

    # Close connections
    sf_conn.close()
    pg_engine.dispose()


if __name__ == "__main__":
    main()
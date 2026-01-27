import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger

DB_URL = "postgresql+psycopg2://demo_user:demo_pass@localhost:5432/lts_demo"
DATA_DIR = Path("data_raw")

def load_csv(engine, path: Path, table: str):
    df = pd.read_csv(path)
    logger.info(f"{table}: read {len(df):,} rows from {path.name}")
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=2000)
    logger.info(f"{table}: inserted {len(df):,} rows")

def load_claims_json(engine, path: Path, table: str):
    claims = json.loads(path.read_text())
    df = pd.DataFrame(claims)
    logger.info(f"{table}: read {len(df):,} rows from {path.name}")
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=2000)
    logger.info(f"{table}: inserted {len(df):,} rows")

def main():
    engine = create_engine(DB_URL)

    # optional: clear tables so re-runs are deterministic
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw_claims, raw_premium_payments, raw_policies;"))
    logger.info("Truncated raw tables")

    load_csv(engine, DATA_DIR / "policies.csv", "raw_policies")
    load_csv(engine, DATA_DIR / "premium_payments.csv", "raw_premium_payments")
    load_claims_json(engine, DATA_DIR / "claims.json", "raw_claims")

    with engine.begin() as conn:
        counts = conn.execute(text("""
            SELECT 'raw_policies' AS table, COUNT(*) AS n FROM raw_policies
            UNION ALL SELECT 'raw_premium_payments', COUNT(*) FROM raw_premium_payments
            UNION ALL SELECT 'raw_claims', COUNT(*) FROM raw_claims
            ORDER BY table;
        """)).fetchall()

    logger.info("Row counts:")
    for t, n in counts:
        logger.info(f"  {t}: {n:,}")

if __name__ == "__main__":
    main()


Goal: Build a local, idempotent ETL that reads two Excel files (doctors and appointments),
cleans/transforms the data, and loads it into a PostgreSQL schema (default: healthtech).

Key requirements satisfied:
- Python-only pipeline with structured logging to console + file.
- Extract and Transform are separate functions (do NOT merge them).
- Idempotent Load (TRUNCATE/LOAD inside a transaction).
- Two destination tables: healthtech.doctors, healthtech.appointments.

Run
----
python etl_pipeline.py \
  --doctors_xlsx "Data Enginner's Doctors Excel - VIP Medical Group (1).xlsx" \
  --appointments_xlsx "Data Engineer's Appointments Excel - VIP Medical Group (1).xlsx" \
  --db_url postgresql+psycopg2://user:password@localhost:5432/postgres \
  --schema healthtech

(You can also set environment variables; see README.)
"""
import argparse
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
from typing import Tuple

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ---------------------- Logging ----------------------

def _setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("healthtech_etl")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers when re-running
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3)
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

# ---------------------- Extract ----------------------

def extract(doctors_xlsx: Path, appointments_xlsx: Path, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read raw Excel files and return raw DataFrames.
    Keep this function limited to I/O only (no transformations here).
    """
    logger.info("Extract: reading Excel files …")
    df_doctors = pd.read_excel(doctors_xlsx, engine="openpyxl")
    df_appts = pd.read_excel(appointments_xlsx, engine="openpyxl")

    # Normalize column names early (still considered extract; no data changes)
    df_doctors.columns = [str(c).strip().lower().replace(' ', '_') for c in df_doctors.columns]
    df_appts.columns = [str(c).strip().lower().replace(' ', '_') for c in df_appts.columns]

    # Drop extra header rows if present
    if 'doctor_id' in df_doctors.columns:
        df_doctors = df_doctors[~(df_doctors['doctor_id'].astype(str).str.lower() == 'doctor_id')]
    if 'booking_id' in df_appts.columns:
        df_appts = df_appts[~(df_appts['booking_id'].astype(str).str.lower().str.contains('booking'))]

    logger.info("Extract: completed. doctors=%d rows, appointments=%d rows", len(df_doctors), len(df_appts))
    return df_doctors, df_appts

# ---------------------- Transform ----------------------

def transform(raw_doctors: pd.DataFrame, raw_appts: pd.DataFrame, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Clean, standardize, and validate both datasets.
    This function must remain separate from extract().
    """
    logger.info("Transform: cleaning doctors …")
    df_doctors = raw_doctors.copy()
    df_doctors['doctor_id'] = pd.to_numeric(df_doctors['doctor_id'], errors='coerce').astype('Int64')
    for col in ['name', 'specialty']:
        if col in df_doctors.columns:
            df_doctors[col] = df_doctors[col].astype(str).str.strip()
    df_doctors = df_doctors.dropna(subset=['doctor_id']).drop_duplicates(subset=['doctor_id'])

    logger.info("Transform: cleaning appointments …")
    df_appts = raw_appts.copy()
    for col in df_appts.columns:
        if df_appts[col].dtype == object:
            df_appts[col] = df_appts[col].astype(str).str.strip()

    df_appts['booking_id'] = pd.to_numeric(df_appts['booking_id'], errors='coerce').astype('Int64')
    df_appts['patient_id'] = pd.to_numeric(df_appts['patient_id'], errors='coerce').astype('Int64')
    df_appts['doctor_id'] = pd.to_numeric(df_appts['doctor_id'], errors='coerce').astype('Int64')

    df_appts['booking_date'] = pd.to_datetime(df_appts['booking_date'], errors='coerce', infer_datetime_format=True)

    status_map = {
        'confirmed': 'confirmed',
        'confirmado': 'confirmed',
        'confirmada': 'confirmed',
        'cancelled': 'cancelled',
        'canceled': 'cancelled',
    }
    df_appts['status'] = df_appts['status'].astype(str).str.lower().str.strip().map(lambda x: status_map.get(x, np.nan))

    required = ['booking_id', 'patient_id', 'doctor_id', 'booking_date', 'status']
    df_appts = df_appts.dropna(subset=required)

    # enforce integer types after NA drop
    for c in ['booking_id', 'patient_id', 'doctor_id']:
        df_appts[c] = df_appts[c].astype('int64')

    if df_appts['booking_id'].duplicated().any():
        logger.warning("Transform: found duplicate booking_id values; keeping earliest occurrence per booking_id.")
        df_appts = (df_appts
                    .sort_values(['booking_id', 'booking_date'])
                    .drop_duplicates(subset=['booking_id'], keep='first'))

    # Enrich doctors set with missing doctor_ids from appointments
    known = set(df_doctors['doctor_id'].dropna().astype(int))
    used = set(df_appts['doctor_id'].dropna().astype(int))
    missing = sorted(list(used - known))
    if missing:
        logger.info("Transform: adding %d unknown doctor_id(s) found in appointments: %s", len(missing), missing)
        add_df = pd.DataFrame({'doctor_id': missing, 'name': ['Unknown']*len(missing), 'specialty': ['Unknown']*len(missing)})
        df_doctors = pd.concat([df_doctors, add_df], ignore_index=True)

    # Standardize column order
    df_doctors = df_doctors[['doctor_id', 'name', 'specialty']].sort_values('doctor_id').reset_index(drop=True)
    df_appts = df_appts[['booking_id','patient_id','doctor_id','booking_date','status']].sort_values(['booking_date','booking_id']).reset_index(drop=True)

    logger.info("Transform: completed. doctors=%d rows, appointments=%d rows", len(df_doctors), len(df_appts))
    return df_doctors, df_appts

# ---------------------- Load ----------------------

def _ensure_schema_and_tables(engine, schema: str, logger: logging.Logger):
    logger.info("Load: ensuring schema '%s' and tables exist …", schema)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        # Create tables with primary keys and basic constraints
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.doctors (
            doctor_id   INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            specialty   TEXT
        );
        """))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.appointments (
            booking_id    BIGINT PRIMARY KEY,
            patient_id    BIGINT NOT NULL,
            doctor_id     INTEGER NOT NULL REFERENCES {schema}.doctors(doctor_id),
            booking_date  DATE NOT NULL,
            status        TEXT NOT NULL CHECK (status IN ('confirmed','cancelled'))
        );
        """))


def load(df_doctors: pd.DataFrame, df_appts: pd.DataFrame, db_url: str, schema: str, logger: logging.Logger):
    """Idempotent load into PostgreSQL (TRUNCATE/LOAD within a transaction)."""
    engine = create_engine(db_url, future=True)
    _ensure_schema_and_tables(engine, schema, logger)

    logger.info("Load: truncating and loading tables …")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.appointments;"))
        conn.execute(text(f"TRUNCATE TABLE {schema}.doctors;"))

    # Use pandas.to_sql to load data
    # Note: we insert doctors first to satisfy FK
    df_doctors.to_sql('doctors', engine, schema=schema, if_exists='append', index=False, method='multi', chunksize=10_000)
    df_appts.to_sql('appointments', engine, schema=schema, if_exists='append', index=False, method='multi', chunksize=10_000)
    logger.info("Load: completed.")

# ---------------------- CLI ----------------------

def main():
    parser = argparse.ArgumentParser(description="Local ETL for HealthTech doctors & appointments")
    parser.add_argument('--doctors_xlsx', type=Path, required=True)
    parser.add_argument('--appointments_xlsx', type=Path, required=True)
    parser.add_argument('--db_url', type=str, required=False, default=os.getenv('DB_URL', 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'))
    parser.add_argument('--schema', type=str, required=False, default=os.getenv('DB_SCHEMA', 'healthtech'))
    parser.add_argument('--output_dir', type=Path, required=False, default=Path('output'))
    parser.add_argument('--log_dir', type=Path, required=False, default=Path('logs'))

    args = parser.parse_args()

    logger = _setup_logging(args.log_dir / 'pipeline.log')

    try:
        raw_doctors, raw_appts = extract(args.doctors_xlsx, args.appointments_xlsx, logger)
        df_doctors, df_appts = transform(raw_doctors, raw_appts, logger)

        # Persist final datasets for auditing/inspection prior to load
        args.output_dir.mkdir(parents=True, exist_ok=True)
        doctors_csv = args.output_dir / 'final_doctors.csv'
        appts_csv = args.output_dir / 'final_appointments.csv'
        df_doctors.to_csv(doctors_csv, index=False)
        df_appts.to_csv(appts_csv, index=False, date_format='%Y-%m-%d')
        logger.info("Wrote final datasets: %s, %s", doctors_csv, appts_csv)

        # Load to PostgreSQL
        load(df_doctors, df_appts, args.db_url, args.schema, logger)
        logger.info("Pipeline completed successfully.")
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        raise

if __name__ == '__main__':
    main()

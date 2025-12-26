# HealthTech ETL — VIP Medical Group

**Purpose**
A local, idempotent ETL pipeline that ingests *doctors* and *appointments* from Excel files, cleans/standardizes the data, and loads it into a local PostgreSQL schema (`healthtech`). The resulting dataset enables Operations to analyze **doctor productivity** and **appointment trends**.

## What this pipeline is for (short context)
This pipeline prepares consistent, analytics-ready tables so the business can track **confirmed vs. cancelled appointments**, see **who the top-performing doctors are**, and slice by date, doctor, and patient segments. It is designed to be easily re-run without creating duplicates.

## Repository layout (delivered here as files)
```
./etl_pipeline.py            # The Python ETL script (extract, transform, load are separate)
./final_doctors.csv          # Cleaned doctors dataset (pre-load artifact)
./final_appointments.csv     # Cleaned appointments dataset (pre-load artifact)
./queries.sql                # SQL to answer business questions (+ short answers inline)
./logs/                      # Pipeline logs (created on run)
./output/                    # Final CSVs when running from CLI
```

## Setup Instructions

### 1) Prerequisites
- **Python 3.10+**
- **PostgreSQL 13+** running locally.
- Network access to `localhost:5432`.

### 2) Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
```

### 3) Install dependencies
```bash
pip install pandas openpyxl SQLAlchemy psycopg2-binary python-dotenv
```

### 4) Prepare the input files
Place the two Excel files at the project root (filenames used by default):
- `Data Enginner's Doctors Excel - VIP Medical Group (1).xlsx`
- `Data Engineer's Appointments Excel - VIP Medical Group (1).xlsx`

### 5) Set your database connection
Option A — environment variables:
```bash
export DB_URL="postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
export DB_SCHEMA="healthtech"
```

Option B — pass as CLI flags to the script (see next step).

### 6) Run the pipeline
```bash
python etl_pipeline.py \
  --doctors_xlsx "Data Enginner's Doctors Excel - VIP Medical Group (1).xlsx" \
  --appointments_xlsx "Data Engineer's Appointments Excel - VIP Medical Group (1).xlsx" \
  --db_url "$DB_URL" \
  --schema "$DB_SCHEMA"
```
The script writes cleaned CSVs to `./output/` and loads both tables into PostgreSQL. Logs are written to `./logs/pipeline.log` and echoed to the console.

### Idempotency strategy
- The **Load** step executes `TRUNCATE TABLE` on `healthtech.appointments` and `healthtech.doctors` inside a transaction, then reloads from the cleaned DataFrames. You can re-run safely without duplicates.

## Pipeline Explanation

### Extract (I/O only)
- Reads both Excel files (doctors, appointments) using **openpyxl** without altering the data.
- Normalizes column names and removes stray header rows that sometimes appear inside the sheets.

### Transform (data quality)
- **Type coercions:** `doctor_id`, `patient_id`, and `booking_id` → integers; `booking_date` → date.
- **Status standardization:** Lowercase; map variants like `canceled` → `cancelled` and `Confirmed` → `confirmed`.
- **Row filtering:** Drop rows missing any required field or with unparseable dates.
- **Deduplication:** If duplicate `booking_id` rows exist, keep the earliest.
- **Referential integrity:** If an appointment references an unknown `doctor_id` (e.g., 105), the pipeline **adds a placeholder row** to the doctors table with `name='Unknown'` and `specialty='Unknown'` to preserve all appointments.

### Load (PostgreSQL)
- Creates schema `healthtech` if it does not exist.
- Ensures both tables exist with primary keys and constraints.
- **Truncates** both tables and reloads (idempotent).

### Target schema
```sql
CREATE SCHEMA IF NOT EXISTS healthtech;

CREATE TABLE IF NOT EXISTS healthtech.doctors (
  doctor_id   INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  specialty   TEXT
);

CREATE TABLE IF NOT EXISTS healthtech.appointments (
  booking_id    BIGINT PRIMARY KEY,
  patient_id    BIGINT NOT NULL,
  doctor_id     INTEGER NOT NULL REFERENCES healthtech.doctors(doctor_id),
  booking_date  DATE NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('confirmed','cancelled'))
);
```

## Business Questions (short answers)
From the cleaned dataset in this repo:
1. **Doctor with most confirmed appointments:** `doctor_id {q1_doc_id}` ("{q1_name}") with **{q1_count}** confirmed.
2. **Confirmed appointments for patient 34:** **{q2}**.
3. **Cancelled appointments between 2025-10-21 and 2025-10-24:** **{q3}**.
4. **Confirmed by doctor:** see `queries.sql` or `confirmed_by_doctor.csv` for the full distribution.

## AWS Architecture Proposal (production)
> Objective: A simple, scalable, serverless-first stack to ingest appointment and doctor data, transform it into analytics-ready tables, and make it available to business users.

- **Ingestion & Storage:**
  - **Amazon S3** as the data lake landing zone for Excel/CSV drops (versioned buckets, lifecycle policies).
  - **AWS Glue Crawler** to catalog the files into the **AWS Glue Data Catalog** for schema discovery.

- **Transform:**
  - **AWS Glue ETL (PySpark)** jobs for scalable batch transformations (type coercion, standardization, dedupe), writing partitioned datasets (e.g., by date) back to S3 in Parquet.
  - **AWS Step Functions** to orchestrate end-to-end flows (ingest → transform → load), with **Amazon EventBridge** schedules or file-drop triggers.
  - **AWS Secrets Manager** to handle credentials for downstream warehouses / RDS securely.

- **Load (serving layer):**
  - **Amazon Redshift Serverless** (or **Amazon RDS for PostgreSQL** if you prefer RDBMS) to host dimensional/serving tables for BI. Use **COPY** from S3 for efficient loads.

- **Analytics / Dashboards:**
  - **Amazon QuickSight** for interactive dashboards on top of Redshift or Athena (querying data in S3). *Use QuickSight here — do not use CloudWatch for this purpose.*

- **Security & Governance:**
  - **AWS IAM** for least-privilege access; **S3 bucket policies**; **KMS** for encryption at rest; **TLS** for in-flight.

This design keeps costs low, scales automatically, and allows analysts to self-serve insights in **QuickSight** with minimal ops overhead.

# Raw Ingestion Pipeline

## Introduction

In this exercise you'll build a small but realistic lakehouse ingestion pipeline. The exercise data spans two "data intervals" sitting in S3 — the second interval introduces evolved schemas on existing tables and a brand-new table that didn't exist in the first run. Your job is to complete the Airflow DAG that triggers a Glue job per landed table, and the PySpark Glue script that reads and writes the data into Apache Iceberg tables.

The exercise targets three concepts:

- **Multi-format raw ingestion.** Landed files arrive as either JSON or CSV, and the Glue script dispatches on a `--source_format` argument to pick the right reader. Much of this code is completed for you.
- **Schema evolution.** When a new run brings columns that didn't exist before, the Glue script issues `ALTER TABLE ADD COLUMN` against the Iceberg table; when expected columns are missing from the new data, it backfills typed NULLs so the writer schema still matches the table.
- **Dynamic mapped tasks in Airflow.** The DAG discovers the landed tables at runtime by inspecting S3, then uses `GlueJobOperator.partial(...).expand_kwargs(...)` to launch one Glue job per discovered table in parallel.

You'll work in two files, `dag.py` and `glue_script.py`. Both contain `#### YOUR CODE HERE` placeholders sitting directly under the comments that describe what each missing line should do — read the comment above the placeholder, then write the line.

## Instructions

### Prerequisites

- Airflow connection: Amazon Web Services
- Airflow variable: The name of your S3 bucket

### 1. Stage the exercise data

- In the Airflow UI, trigger the `setup_s3_data` DAG. It pushes both data intervals into your S3 bucket under `s3://<your-bucket>/landing/`.
- Wait for the run to finish before moving on. Once it's complete, the bucket should contain:

```
landing/
  2026-01-01/
    dataset_a/   (JSON, initial schema)
    dataset_b/   (JSON, initial schema)
  2026-01-02/
    dataset_a/   (JSON, with new session_id and device columns)
    dataset_b/   (JSON, with new category and in_stock columns)
    dataset_c/   (CSV,  brand-new dataset)
```

### 2. Complete `dag.py`

Open `dag.py` and fill in every `#### YOUR CODE HERE` block.

### 3. Complete `glue_script.py`

Open `glue_script.py` and fill in every `#### YOUR CODE HERE` block. The comment directly above each placeholder describes what the missing line should do.

### 4. Run the first interval

Trigger the DAG with `data_interval=2026-01-01`. Both `dataset_a` and `dataset_b` should land as new Iceberg tables under the `raw` namespace. Verify with a quick query in the Athena console.

```sql
SELECT * FROM raw.dataset_a LIMIT 10;
SELECT * FROM raw.dataset_b LIMIT 10;
```

### 5. Run the second interval

Trigger the DAG again with `data_interval=2026-01-02`. Three things should happen on this run:

- `dataset_a` and `dataset_b` get new columns added via `ALTER TABLE`. Older rows from the first interval keep NULLs in those columns.
- `dataset_c` is created fresh as a new table

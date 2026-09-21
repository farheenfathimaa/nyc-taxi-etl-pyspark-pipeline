"""NYC TLC Trip Record ETL - Airflow DAG.

Orchestrates: download raw Parquet -> download zone lookup -> clean -> aggregate
        -> QA validate, as five separate Airflow tasks with retries.

Run locally via ``docker compose up`` (see README.md). Each failing step surfaces
its full logs in the Airflow UI ("Logs" tab on the failed task instance).

Environment variables read by this DAG:
    TAXI_DATASET (yellow|green|fhvhv)   - which trip-record family to process
    TAXI_MONTHS  ("YYYY-MM" or comma-sep list) - which month(s) to ingest
    DATA_ROOT    - host-mounted data dir mounted at /opt/airflow/data

--------------------------------------------------------------------------------
WHY IT SCALES TO A REAL CLUSTER (Dataproc / EMR) - ALSO IN README.md §6
--------------------------------------------------------------------------------
* Every compute step already runs PySpark, so the code is identical.
  - Locally:   SparkSession.builder.master("local[*]")  (set SPARK_MASTER_URL to override)
  - Cloud:     omit master -> the session connects to YARN/K8s on Dataproc/EMR.
* Storage swaps from the POSIX mount (--data-root /opt/airflow/data) to
  gs://bucket/tlc or s3://bucket/tlc; Parquet + date/borough partition pruning
  work unchanged, so scans stay cheap as volumes grow.
* Airflow tasks stay the same shape. Swap the BashOperators below for cluster-aware
  operators, e.g.:
      DataprocCreateClusterOperator >> DataprocSubmitJobOperator
      EmrCreateJobFlowOperator     >> EmrAddStepsOperator
  or submit the exact same script to Dataproc Serverless / EMR Serverless and keep
  Airflow as the scheduler that merely awaits completion.
* Ingestion stays a thin HTTP pull; in prod you'd point it at GCS/S3 restaged files.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------- configuration
DATA_ROOT = os.environ.get("DATA_ROOT", "/opt/airflow/data")
DATASET = os.environ.get("TAXI_DATASET", "yellow")
MONTHS = os.environ.get("TAXI_MONTHS", "2025-01")
SCRIPTS_DIR = "/opt/airflow/scripts"
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"
ZONE_BUNDLE = "/opt/airflow/spark/data/taxi_zone_lookup.csv"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=15),
}

DOC_MD = """
### NYC TLC Trip Record ETL (yellow/green/fhvhv)

```
                    ┌──────────────────────────────────────────────┐
                    │   Airflow  (docker compose, LocalExecutor)   │
                    │                                              │
  internet         │  ┌──────────────┐   ┌──────────────────────┐  │
  (no auth)        │  │ download_raw │   │ download_zone_lookup │  │
  TLC parquet  ───►│  └──────┬───────┘   └──────────┬───────────┘  │
  TLC zone csv ───►│         └──────────┬───────────┘              │
                    │              ┌─────▼─────┐                   │
                    │              │ clean_trips (PySpark)         │
                    │              └─────┬─────┘                   │
                    │              ┌─────▼─────┐                   │
                    │              │ aggregate_trips (PySpark)     │
                    │              └─────┬─────┘                   │
                    │              ┌─────▼──────┐                  │
                    │              │ validate_outputs (QA)         │
                    │              └────────────┘                  │
                    └──────────────────────────────────────────────┘
                                   │ writes Parquet
                                   ▼
              data/raw -> data/processed/{cleaned,aggregations}/
              (date+borough partitioned)
```

* **download_raw_trips**       - ingest.py: HTTP GET parquet(s) into `data/raw/`
* **download_zone_lookup**     - ingest.py: zone lookup CSV into `data/dimensions/`
* **clean_trips**              - taxi_etl.py `--stage clean`: drop invalid rows, derive
                                 duration/speed/fare-per-mile, write `data/processed/cleaned/`
* **aggregate_trips**          - taxi_etl.py `--stage aggregate`: join zone names, build
                                 daily + hourly pickup-zone aggregations
* **validate_outputs**         - QA step: every aggregation table must be non-empty

Every task retries twice with exponential backoff; failures are visible in the
Airflow UI Logs tab. See README.md for full details including scaling guidance.
"""


def _validate_outputs() -> int:
    """Read the aggregates back with Spark and assert they are non-empty."""
    import sys

    sys.path.insert(0, SPARK_JOBS_DIR)
    import taxi_etl

    data_root = Path(DATA_ROOT)
    daily = data_root / "processed" / "aggregations" / "daily_pickup_zone_metrics"
    hourly = data_root / "processed" / "aggregations" / "hourly_pickup_ride_counts"
    return taxi_etl.validate_outputs(data_root, daily, hourly)


with DAG(
    dag_id="nyc_taxi_trip_etl",
    description="Batch ETL: NYC TLC trip records -> cleaned + aggregated Parquet",
    default_args=DEFAULT_ARGS,
    schedule_interval="@monthly",
    start_date=dt.datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    doc_md=DOC_MD,
    tags=["nyc-tlc", "pyspark", "etl", "parquet"],
) as dag:

    download_raw_trips = BashOperator(
        task_id="download_raw_trips",
        bash_command=(
            f"python {SCRIPTS_DIR}/ingest.py --dataset {DATASET} "
            f'--months "{MONTHS}" --data-dir {DATA_ROOT} '
            f"--bundle-zone-lookup {ZONE_BUNDLE}"
        ),
        do_xcom_push=True,  # last stdout line (raw parquet path) -> XCom
        doc_md="HTTP GET of TLC trip-record Parquet file(s) into `data/raw/`.",
    )

    download_zone_lookup = BashOperator(
        task_id="download_zone_lookup",
        bash_command=(
            f"python {SCRIPTS_DIR}/ingest.py --zone-lookup-only "
            f"--data-dir {DATA_ROOT} --bundle-zone-lookup {ZONE_BUNDLE}"
        ),
        do_xcom_push=True,  # zone lookkup CSV path -> XCom
        doc_md="Download the NYC taxi zone lookup CSV into `data/dimensions/`.",
    )

    clean_trips = BashOperator(
        task_id="clean_trips",
        bash_command=(
            f"python {SPARK_JOBS_DIR}/taxi_etl.py --stage clean "
            '--input "{{ ti.xcom_pull(task_ids="download_raw_trips") }}" '
            '--zone-lookup "{{ ti.xcom_pull(task_ids="download_zone_lookup") }}" '
            f"--data-root {DATA_ROOT} --service {DATASET}"
        ),
        do_xcom_push=True,  # cleaned parquet dir path -> XCom
        doc_md="PySpark `--stage clean`: drop nulls/invalid rows, derive "
               "duration / avg speed / fare-per-mile, write partitioned Parquet.",
    )

    aggregate_trips = BashOperator(
        task_id="aggregate_trips",
        bash_command=(
            f"python {SPARK_JOBS_DIR}/taxi_etl.py --stage aggregate "
            '--input "{{ ti.xcom_pull(task_ids="clean_trips") }}" '
            '--zone-lookup "{{ ti.xcom_pull(task_ids="download_zone_lookup") }}" '
            f"--data-root {DATA_ROOT}"
        ),
        do_xcom_push=True,  # daily aggregation dir path -> XCom
        doc_md="PySpark `--stage aggregate`: join zone lookup (borough/zone names), "
               "build daily + hourly pickup-zone aggregations, write partitioned Parquet.",
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=_validate_outputs,
        doc_md="QA: read the aggregation tables back with Spark and verify each has "
               "rows; raise a FATAL log/py-exception if empty.",
    )

    # Dependencies: both downloads must finish before cleaning; aggregation depends
    # on the zone lookup (join) as well as on the clean output.
    [download_raw_trips, download_zone_lookup] >> clean_trips
    clean_trips >> aggregate_trips >> validate_outputs
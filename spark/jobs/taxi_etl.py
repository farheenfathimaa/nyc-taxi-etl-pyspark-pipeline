#!/usr/bin/env python3
"""
NYC TLC Trip Record Data - batch ETL (PySpark, local[*] mode).

Runs as a standalone script (``python spark/jobs/taxi_etl.py``) in a container that
has a JVM + PySpark installed, or via ``spark-submit``. No cluster required.

Stages
------
clean      - Read raw Parquet -> canonicalize columns -> drop nulls/invalid rows ->
             derive metrics (trip_duration_minutes, avg_speed_mph, fare_per_mile) ->
             write validated trips as Parquet partitioned by trip_date.
aggregate  - Read cleaned trips -> join taxi zone lookup (LocationID -> Borough, Zone,
             service_zone) -> build daily + hourly pick-up aggregation tables partitioned
             by trip_date (and borough) -> write as Parquet.
all        - clean followed by aggregate (handy for manual runs).

Cleaning rules applied (bad rows logged & removed):
  * nulls in any critical column (pickup/dropoff timestamps, zones, fare, distance)
  * fare_amount <= 0, trip_distance <= 0, passenger_count < 1
  * impossible durations: < 1 minute or > 24 hours
  * implausible speed: average speed > 120 mph
  * exact duplicate trips

--------------------------------------------------------------------------------
Scaling to a real cluster (Dataproc / EMR) - what changes
--------------------------------------------------------------------------------
EVERYTHING below already runs on Spark, so porting is mostly configuration:

  1. SparkSession: in local mode we hard-code ``master("local[*]")``. On Dataproc/EMR
     simply call ``SparkSession.builder.appName(...).getOrCreate()`` and it connects
     to the cluster's resource manager automatically. This script does exactly that
     when SPARK_MASTER_URL is set (e.g. "yarn", "k8s://...", "spark://...").

  2. Storage: raw/processed paths live under ``--data-root`` (a POSIX mount locally).
     In the cloud, point ``--data-root`` at object storage: gs://bucket/tlc (Dataproc)
     or s3://bucket/tlc (EMR). Spark's Parquet reader/writer reads those URLs natively,
     and the date/borough partitioning keeps scans cheap (partition pruning).

  3. Orchestration: the Airflow tasks remain, but the BashOperator that runs
     ``python taxi_etl.py`` is swapped for cluster-aware operators, e.g.
     - DataprocCreateClusterOperator -> DataprocSubmitJobOperator, or
     - EmrCreateJobFlowOperator / EmrAddStepsOperator, or
     - run the job as a single Dataproc Serverless / EMR Serverless batch,
       which is the cheapest way to scale without reserving a cluster.

  4. Zone lookup + ingestion: the ingest download step is unchanged (thin HTTP pull);
     in prod you would pre-load the lookup CSV once to GCS/S3 and only download new
     trip files, e.g. with an Airflow "run_after" schedule or a Cloud Function trigger.

  5. Resources: increase shuffle partitions / driver+executor memory via spark confs
     instead of the laptop defaults. File output grows by month count - repartition()
     or Azure-AdLS/FileOutputCommitter tuning only matters at TB scale.
"""

import argparse
import logging
import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("taxi_etl")

# Canonical column mapping per dataset (schemas differ across yellow/green/fhvhv).
# The mappings are also used to guess the service if --service is not given.
SCHEMA_BY_SERVICE = {
    "yellow": {
        "pickup": "tpep_pickup_datetime",
        "dropoff": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type",
    },
    "green": {
        "pickup": "lpep_pickup_datetime",
        "dropoff": "lpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type",
    },
    "fhvhv": {
        "pickup": "pickup_datetime",
        "dropoff": "dropoff_datetime",
        "passenger_count": "",  # FHV trips carry no passenger count
        "trip_distance": "trip_miles",
        "fare_amount": "base_passenger_fare",
        "tip_amount": "tips",
        "total_amount": "base_passenger_fare",
        "payment_type": "",
    },
}

# Sanity thresholds (documented in the docstring above).
MIN_DURATION_MINUTES = 1.0
MAX_DURATION_MINUTES = 24 * 60.0  # 24 h
MAX_AVG_SPEED_MPH = 120.0

# Column names that survive to the cleaned output (derived + canonicalized).
CLEAN_COLUMNS = [
    "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type",
    "pickup_location_id", "dropoff_location_id", "service_type",
    "trip_duration_minutes", "avg_speed_mph", "fare_per_mile", "revenue",
    "trip_date", "trip_hour",
]


def _ensure_java() -> None:
    """Make sure PySpark can find a JVM.

    Works when JAVA_HOME is set, or falls back to locating the ``java`` binary on
    PATH (likely /usr/bin/java -> /usr/lib/jvm/java-17-openjdk-<arch>/bin/java).
    """
    if os.environ.get("JAVA_HOME"):
        return
    java = shutil.which("java")
    if not java:
        logger.warning("No JAVA_HOME and no java on PATH - PySpark will fail to start"
                       " unless one becomes available.")
        return
    resolved = Path(java).resolve()
    java_home = resolved.parent.parent  # <...>/bin/java -> <...>
    os.environ["JAVA_HOME"] = str(java_home)
    logger.info("JAVA_HOME not set; derived from PATH -> %s", java_home)


def build_spark(app_name: str = "nyc-taxi-etl") -> SparkSession:
    """Create a SparkSession (local[*] unless SPARK_MASTER_URL overrides it)."""
    _ensure_java()
    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master)
               .config("spark.ui.enabled", "false")          # no UI inside the scheduler
               .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
               .config("spark.sql.shuffle.partitions", "8")  # laptop-friendly
               .config("spark.sql.warehouse.dir",
                       os.environ.get("SPARK_WAREHOUSE_DIR",
                                      str(Path.cwd() / "spark-warehouse"))))
    if master.startswith("local"):
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
    logger.info("Starting SparkSession: master=%s", master)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def detect_service(df: DataFrame, explicit: str | None = None) -> str:
    """Guess the dataset family from the raw columns, unless explicitly provided."""
    if explicit:
        if explicit not in SCHEMA_BY_SERVICE:
            raise SystemExit(f"Unknown dataset/service: {explicit}")
        return explicit
    cols = {c.lower() for c in df.columns}
    if "lpep_pickup_datetime" in cols:
        return "green"
    if "tpep_pickup_datetime" in cols:
        return "yellow"
    if "hvfhs_license_num" in cols:
        return "fhvhv"
    raise SystemExit(
        "Could not auto-detect the dataset from columns. Pass --service yellow|green|fhvhv."
        f"\nColumns seen: {sorted(df.columns)}"
    )


def canonicalize(df: DataFrame, service: str) -> DataFrame:
    """Map each dataset's schema onto a shared set of column names + types."""
    s = SCHEMA_BY_SERVICE[service]

    def num(col: str, alias: str):
        return (F.col(col).cast("double").alias(alias)) if col else F.lit(None).cast("double").alias(alias)

    def txt(col: str, alias: str):
        return (F.col(col).cast("string").alias(alias)) if col else F.lit(None).cast("string").alias(alias)

    def integ(col: str, alias: str):
        return (F.col(col).cast("int").alias(alias)) if col else F.lit(None).cast("int").alias(alias)

    return df.select(
        F.col(s["pickup"]).cast("timestamp").alias("pickup_datetime"),
        F.col(s["dropoff"]).cast("timestamp").alias("dropoff_datetime"),
        integ(s["passenger_count"], "passenger_count"),
        num(s["trip_distance"], "trip_distance"),
        num(s["fare_amount"], "fare_amount"),
        num(s["tip_amount"], "tip_amount"),
        num(s["total_amount"], "total_amount"),
        txt(s["payment_type"], "payment_type"),
        integ("PULocationID", "pickup_location_id"),
        integ("DOLocationID", "dropoff_location_id"),
        F.lit(service).alias("service_type"),
    )


def _log_count(stage: str, count: int) -> None:
    logger.info("Row count (%-30s) : %s", stage, f"{count:,}")


def clean_trips(df: DataFrame) -> DataFrame:
    """Drop invalid rows + derive the trip-level metrics. Each decision is logged."""
    total = df.count()
    _log_count("raw input", total)

    critical = [
        "pickup_datetime", "dropoff_datetime",
        "pickup_location_id", "dropoff_location_id",
        "fare_amount", "trip_distance",
    ]
    df = df.filter(F.col("pickup_datetime").isNotNull() &
                   F.col("dropoff_datetime").isNotNull() &
                   F.col("pickup_location_id").isNotNull() &
                   F.col("dropoff_location_id").isNotNull() &
                   F.col("fare_amount").isNotNull() &
                   F.col("trip_distance").isNotNull())
    _log_count("dropping critical nulls", df.count())

    df = df.withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(0)))
    df = df.filter((F.col("passenger_count") > 0) &
                   (F.col("fare_amount") > 0) &
                   (F.col("trip_distance") > 0))
    _log_count("dropping non-positive pax/fare/distance", df.count())

    # duration in seconds from the pickup->dropoff timestamp diff, converted to minutes
    df = (df.withColumn("trip_duration_minutes",
                        (F.col("dropoff_datetime").cast("long") -
                         F.col("pickup_datetime").cast("long")) / 60.0)
            .filter((F.col("trip_duration_minutes") >= MIN_DURATION_MINUTES) &
                    (F.col("trip_duration_minutes") <= MAX_DURATION_MINUTES)))
    _log_count("dropping impossible durations", df.count())

    df = (df.withColumn("trip_duration_hours", F.col("trip_duration_minutes") / 60.0)
            .withColumn("avg_speed_mph",
                        F.round(F.col("trip_distance") / F.col("trip_duration_hours"), 2))
            .filter(F.col("avg_speed_mph") <= MAX_AVG_SPEED_MPH))
    _log_count(f"dropping implausible speeds (> {MAX_AVG_SPEED_MPH:.0f} mph)", df.count())

    df = (df.withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
            .withColumn("revenue", F.coalesce(F.col("total_amount"), F.col("fare_amount")))
            .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
            .withColumn("trip_hour", F.hour(F.col("pickup_datetime"))))

    df = df.dropDuplicates(["pickup_datetime", "dropoff_datetime",
                            "pickup_location_id", "dropoff_location_id",
                            "trip_distance", "fare_amount", "passenger_count"])
    kept = df.count()
    _log_count("after dedupe", kept)

    dropped = total - kept
    pct = (dropped / total * 100.0) if total else 0.0
    logger.info("Cleaning summary: kept %s / %s rows (dropped %.2f%%)",
                f"{kept:,}", f"{total:,}", pct)
    return df.select(*CLEAN_COLUMNS)


def write_cleaned(df: DataFrame, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Writing cleaned trips to %s (partitioned by trip_date)", out_dir)
    (df.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("trip_date")
        .parquet(str(out_dir)))
    logger.info("Cleaned output written: %s", out_dir)
    return out_dir.resolve()


def make_aggregations(spark: SparkSession, cleaned: DataFrame, zone_lookup: str,
                      out_daily: Path, out_hourly: Path) -> tuple[int, int]:
    """Join zone names + aggregate daily/hourly pickup metrics by pickup zone."""
    if not Path(zone_lookup).exists():
        raise FileNotFoundError(f"Zone lookup CSV not found: {zone_lookup}")
    zone = (spark.read.option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(zone_lookup)
            .select(F.col("LocationID").cast("int").alias("zone_id"),
                    F.col("Borough").cast("string"),
                    F.col("Zone").cast("string"),
                    F.col("service_zone").cast("string")))

    enriched = cleaned.join(zone, cleaned["pickup_location_id"] == zone["zone_id"], "left")
    enriched = (enriched.withColumn(
                    "Borough",
                    F.when(F.col("Borough").isin("N/A", ""), "Unknown")
                     .otherwise(F.coalesce(F.col("Borough"), F.lit("Unknown"))))
                        .withColumn(
                    "Zone",
                    F.when(F.col("Zone").isin("N/A", ""), "Unmapped")
                     .otherwise(F.coalesce(F.col("Zone"), F.lit("Unmapped")))))

    logger.info("Daily aggregation: group by (trip_date, borough, pickup zone)")
    daily = (enriched.groupBy("trip_date", "Borough", "pickup_location_id", "Zone",
                              "service_type")
             .agg(
                 F.count("*").alias("total_trips"),
                 F.sum("passenger_count").alias("total_passengers"),
                 F.round(F.sum("trip_distance"), 2).alias("total_distance_miles"),
                 F.round(F.sum("revenue"), 2).alias("total_fare_revenue"),
                 F.round(F.sum("tip_amount"), 2).alias("total_tips"),
                 F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                 F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
                 F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
             )
             .orderBy("trip_date", "Borough", "pickup_location_id"))

    out_daily.mkdir(parents=True, exist_ok=True)
    (daily.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("trip_date", "Borough")
        .parquet(str(out_daily)))
    daily_count = daily.count()
    logger.info("Wrote daily aggregation : %s (%s rows)", out_daily, f"{daily_count:,}")

    logger.info("Hourly aggregation: group by (trip_date, hour, pickup zone)")
    hourly = (enriched.groupBy("trip_date", "trip_hour", "Borough",
                               "pickup_location_id", "Zone")
              .agg(F.count("*").alias("ride_count"))
              .orderBy("trip_date", "trip_hour", "Borough", "pickup_location_id"))

    out_hourly.mkdir(parents=True, exist_ok=True)
    (hourly.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("trip_date", "Borough")
        .parquet(str(out_hourly)))
    hourly_count = hourly.count()
    logger.info("Wrote hourly aggregation : %s (%s rows)", out_hourly, f"{hourly_count:,}")
    return daily_count, hourly_count


def validate_outputs(data_root: Path, daily_dir: Path, hourly_dir: Path) -> int:
    """Independent QA check: every aggregation table has rows and date partitions."""
    spark = build_spark("nyc-taxi-etl-qa")
    try:
        new_partitions = None
        for name, path in (("daily_pickup_zone_metrics", daily_dir),
                           ("hourly_pickup_ride_counts", hourly_dir)):
            if not path.exists():
                raise SystemExit(f"FATAL: missing output directory for {name}: {path}")
            df = spark.read.parquet(str(path))
            n = df.count()
            logger.info("QA %-28s : %s rows", name, f"{n:,}")
            if n == 0:
                raise SystemExit(f"FATAL: {name} is empty - pipeline produced no data")
            if "trip_date" in df.columns:
                new_partitions = df.select(F.max("trip_date").alias("max")).collect()[0]["max"]
        logger.info("QA passed. Latest trip_date present: %s", new_partitions)
        return 0
    finally:
        spark.stop()


def run_stage(args) -> int:
    start = time.time()
    spark = build_spark()
    service = None
    artifact_line: str = ""
    try:
        data_root = Path(args.data_root).expanduser().resolve()
        processed = data_root / "processed"

        raw_input = Path(args.input).expanduser().resolve()
        if not raw_input.exists():
            raise SystemExit(f"FATAL: input path does not exist: {raw_input}")

        primary_out: Path | None = None

        if args.stage in ("clean", "all"):
            logger.info("=== STAGE: clean ===")
            raw = spark.read.parquet(str(raw_input))
            if args.limit and int(args.limit) > 0:
                raw = raw.limit(int(args.limit))
                logger.info("Limited to %s rows for a smoke test", args.limit)
            service = detect_service(raw, args.service)
            logger.info("Detected service: %s", service)
            canon = canonicalize(raw, service)
            cleaned = clean_trips(canon)
            clean_out = processed / "cleaned" / service
            primary_out = write_cleaned(cleaned, clean_out)

        if args.stage in ("aggregate", "all"):
            logger.info("=== STAGE: aggregate ===")
            cleaned_input = (Path(args.input).expanduser().resolve()
                             if args.stage == "aggregate" else primary_out)
            # If this is a standalone aggregate run, recover the service from the
            # cleaned table itself (it is stored as a column).
            if args.stage == "aggregate":
                service = spark.read.parquet(str(cleaned_input)).select(
                    F.collect_set("service_type").alias("s")).collect()[0]["s"]
                service = service[0] if service else (args.service or "yellow")
            cleaned = spark.read.parquet(str(cleaned_input))
            if args.limit and int(args.limit) > 0:
                cleaned = cleaned.limit(int(args.limit))
            daily_out = processed / "aggregations" / "daily_pickup_zone_metrics"
            hourly_out = processed / "aggregations" / "hourly_pickup_ride_counts"
            make_aggregations(spark, cleaned, args.zone_lookup, daily_out, hourly_out)
            primary_out = daily_out

        elapsed = time.time() - start
        logger.info("Pipeline finished in %.1f s. Stage=%s", elapsed, args.stage)
        if args.stage == "all":
            artifact_line = (
                f"clean={processed / 'cleaned' / (service or 'yellow')}; "
                f"aggregate={processed / 'aggregations' / 'daily_pickup_zone_metrics'}"
            )
        elif primary_out is not None:
            artifact_line = str(primary_out)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - surface a clean, greppable error
        logger.exception("FATAL: unexpected failure in stage %s", args.stage)
        raise SystemExit(f"FATAL: {args.stage} failed: {exc}") from exc
    finally:
        spark.stop()
        # IMPORTANT: spark.stop() emits a py4j "Closing down clientserver connection"
        # INFO line. Airflow's BashOperator merges stderr into stdout and pushes the
        # LAST line to XCom, so the artifact path must be printed here - after the
        # session is stopped, with nothing logged after it.
    if artifact_line:
        print(artifact_line)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="NYC TLC batch ETL (PySpark). clean -> aggregate stages.")
    parser.add_argument("--stage", choices=["clean", "aggregate", "all"], default="clean")
    parser.add_argument("--input", required=True,
                        help="Raw trip parquet (clean/all) or cleaned parquet dir (aggregate).")
    parser.add_argument("--zone-lookup",
                        default="data/dimensions/taxi_zone_lookup.csv",
                        help="NYC taxi zone lookup CSV.")
    parser.add_argument("--data-root", default="data",
                        help="Root output dir; data/processed is created inside it.")
    parser.add_argument("--service", choices=["yellow", "green", "fhvhv"], default=None,
                        help="Dataset family (auto-detected from columns if omitted).")
    parser.add_argument("--limit", default="0",
                        help="If >0, only process this many rows (smoke test).")
    return parser


def main() -> int:
    # py4j's client-server shut-down chatter is not useful in Airflow task logs
    # and can otherwise interfere with stdout-parsing (XCom capture).
    for noisy in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    args = build_parser().parse_args()
    return run_stage(args)


if __name__ == "__main__":
    sys.exit(main())
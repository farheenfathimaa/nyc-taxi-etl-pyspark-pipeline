#!/usr/bin/env python3
"""
NYC TLC Trip Record Data - ingestion script.

Downloads raw trip-record Parquet files for the requested services/months and the
taxi zone lookup CSV into a local data directory. No API key, no auth, no rate
limit - the NYC TLC publishes these files as public static assets.

Trip data URL pattern (parquet):     {BASE_URL}/trip-data/{service}_tripdata_YYYY-MM.parquet
Zone lookup URL (CSV):               {BASE_URL}/misc/taxi+_zone_lookup.csv
  where BASE_URL = https://d37ci6vzurychx.cloudfront.net   (-is the bucket behind
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Usage examples:
    python scripts/ingest.py --dataset yellow --months 2025-01,2025-02
    python scripts/ingest.py --dataset green  --months 2025-01 --data-dir C:/taxi/data
    python scripts/ingest.py --zone-lookup-only
    python scripts/ingest.py --dataset fhvhv --months 2025-01

Designed to be called from Airflow (BashOperator) or directly from a terminal.
The last line printed to stdout is the absolute path of the primary artifact, so
the task can push it to XCom for downstream tasks.
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
import urllib.request
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest")

# Public cloudfront bucket that backs the TLC Trip Record Data page.
DEFAULT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_LOOKUP_PATH = "misc/taxi+_zone_lookup.csv"
TRIPS_PATH = "trip-data/{service}_tripdata_{month}.parquet"

# A copy of the zone lookup is bundled in the repo (spark/data/) as a fallback so
# the pipeline keeps working even if the TLC object URL ever changes.
DEFAULT_BUNDLE = "spark/data/taxi_zone_lookup.csv"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (nyc-taxi-etl-pyspark-pipeline; local ETL)",
    "Accept": "application/octet-stream, text/csv, */*",
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download NYC TLC trip-record data.")
    parser.add_argument("--dataset", default="yellow",
                        choices=["yellow", "green", "fhvhv"],
                        help="Trip-record dataset to download (default: yellow).")
    parser.add_argument("--months", default="",
                        help="Comma-separated list of months, e.g. 2025-01,2025-02. "
                             "Empty means --zone-lookup-only mode.")
    parser.add_argument("--data-dir", default="data",
                        help="Root data directory (raw/ and dimensions/ are created inside).")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help="Base URL for TLC trip-record assets.")
    parser.add_argument("--bundle-zone-lookup", default=DEFAULT_BUNDLE,
                        help="Local fallback zone-lookup CSV if the remote URL fails.")
    parser.add_argument("--zone-lookup-only", action="store_true",
                        help="Only (re)download the taxi zone lookup CSV then exit.")
    parser.add_argument("--force", action="store_true",
                        help="Re-download files even if they already exist.")
    return parser


def download_file(url: str, dest: Path, retries: int = 3, backoff: float = 5.0,
                  chunk_size: int = 1024 * 1024) -> bool:
    """Download ``url`` to ``dest`` with a simple retry/backoff loop.

    Returns True on success, False if all attempts failed.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
            logger.info("Downloading %s -> %s (attempt %d/%d)",
                        url, dest, attempt, retries)
            with urllib.request.urlopen(request, timeout=120) as resp, open(dest, "wb") as fh:
                total = 0
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    fh.write(chunk)
                    total += len(chunk)
            if total == 0:
                raise OSError("empty response body")
            logger.info("Downloaded %s (%d bytes)", dest.name, total)
            return True
        except Exception as exc:  # noqa: BLE001 - log + retry on any network error
            last_err = exc
            logger.warning("Attempt %d failed for %s: %s", attempt, url, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
    logger.error("Giving up on %s after %d attempts: %s", url, retries, last_err)
    return False


def download_zone_lookup(data_dir: Path, base_url: str, bundle: str,
                         force: bool = False, manifest: dict | None = None) -> Path:
    """Fetch the NYC taxi zone lookup CSV into ``data_dir/dimensions/``.

    Falls back to the bundled copy (spark/data/taxi_zone_lookup.csv) if the remote
    request fails, so the pipeline is never blocked by a dead link.
    """
    dest = data_dir / "dimensions" / "taxi_zone_lookup.csv"
    if dest.exists() and not force:
        logger.info("Zone lookup already present, skipping: %s", dest)
    else:
        url = f"{base_url}/{ZONE_LOOKUP_PATH}"
        ok = download_file(url, dest)
        if not ok:
            bundle_path = Path(bundle)
            if bundle_path.exists():
                shutil.copyfile(bundle_path, dest)
                logger.info("Used bundled fallback zone lookup: %s", bundle_path)
            else:
                raise SystemExit(
                    "FATAL: zone lookup download failed AND no bundle fallback found at "
                    f"{bundle_path}"
                )
    if manifest is not None:
        manifest["zone_lookup"] = {"path": str(dest), "rows": _count_csv_rows(dest)}
    return dest.resolve()


def _count_csv_rows(path: Path) -> int:
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            return sum(1 for _ in fh) - 1  # minus header
    except OSError:
        return -1


def main() -> int:
    args = _build_parser().parse_args()
    data_dir = Path(args.data_dir).expanduser().resolve()

    manifest: dict = {
        "dataset": args.dataset,
        "base_url": args.base_url,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "files": [],
    }

    zone_lookup = download_zone_lookup(data_dir, args.base_url, args.bundle_zone_lookup,
                                       args.force, manifest)

    if args.zone_lookup_only or not args.months:
        manifest_path = data_dir / "raw" / "_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        logger.info("Zone lookup ready: %s", zone_lookup)
        print(zone_lookup)
        return 0

    months = [m.strip() for m in args.months.split(",") if m.strip()]
    if not months:
        logger.error("--months is required unless --zone-lookup-only is set")
        return 1

    download_ok = True
    primary_path = None
    for month in months:
        if len(month) != 7 or month[4] != "-":
            logger.error("Month %r is not in YYYY-MM format", month)
            return 1
        url = f"{args.base_url}/{TRIPS_PATH.format(service=args.dataset, month=month)}"
        dest = data_dir / "raw" / args.dataset / f"{args.dataset}_tripdata_{month}.parquet"
        if dest.exists() and not args.force:
            logger.info("Raw file already present, skipping: %s", dest)
            ok = True
        else:
            ok = download_file(url, dest)
        manifest["files"].append({"month": month, "url": url, "path": str(dest),
                                  "size_bytes": dest.stat().st_size if dest.exists() else 0})
        if primary_path is None:
            primary_path = dest
        download_ok = download_ok and ok

    manifest_path = data_dir / "raw" / "_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Wrote manifest: %s", manifest_path)

    if not download_ok:
        logger.error("At least one download failed. Inspect the log above for "
                     "per-file errors. raw/ files that already existed are kept.")
        return 1

    if primary_path is None:
        logger.error("No files downloaded - nothing to do.")
        return 1
    # Last stdout line is the raw parquet path (consumed by Airflow XCom).
    print(primary_path.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
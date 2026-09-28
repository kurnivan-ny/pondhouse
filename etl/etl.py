"""Medallion ETL: ingestion -> bronze -> silver -> gold -> mart -> serve.

Usage:
  python etl.py --init            # seed pos, then ingest -> bronze -> transform -> serve
  python etl.py                   # ingest -> bronze -> transform -> serve
  python etl.py --ingest custom   # use custom ingestion instead of Sling
"""
import argparse
import os
import sys
import time

import db
import ingest
import bronze
import transform
import serve

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_init():
    db.execute_file(os.path.join(SQL_DIR, "00_init.sql"))


def run_pipeline(ingest_method="auto", ingest_mode="overwrite", do_serve=True):
    if ingest_method == "auto":
        # Sling runs in its own container and is driven over the docker CLI, which
        # is absent inside the etl image; fall back to the equivalent Ibis path.
        ingest_method = "sling" if ingest.sling_available() else "custom"

    try:
        t0 = time.time()
        if ingest_method == "sling":
            ingest.run_sling()
        else:
            ingest.run_custom(ingest_mode)
        print(f"ingestion ({ingest_method}) in {time.time() - t0:.3f}s")
    except Exception as e:
        print(f"ERROR in ingestion step: {e}", file=sys.stderr)
        raise

    try:
        t0 = time.time()
        bronze.run_bronze(source_format="delta" if ingest_method == "custom" else "parquet")
        print(f"bronze (delta) in {time.time() - t0:.3f}s")
    except Exception as e:
        print(f"ERROR in bronze step: {e}", file=sys.stderr)
        raise

    try:
        t0 = time.time()
        transform.run_transform()
        print(f"transform (silver/gold/mart delta) in {time.time() - t0:.3f}s")
    except Exception as e:
        print(f"ERROR in transform step: {e}", file=sys.stderr)
        raise

    if do_serve:
        try:
            t0 = time.time()
            serve.run_serve()
            print(f"serve (clickhouse marts) in {time.time() - t0:.3f}s")
        except Exception as e:
            print(f"ERROR in serve step: {e}", file=sys.stderr)
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="create pos schema + seed sample data")
    parser.add_argument("--ingest", choices=["auto", "sling", "custom"], default="auto",
                        help="ingestion method (default: auto — Sling when the "
                             "docker CLI is available, otherwise the Ibis path)")
    parser.add_argument("--mode", choices=["overwrite", "append", "merge"], default="overwrite",
                        help="custom ingestion mode (default: overwrite)")
    parser.add_argument("--no-serve", action="store_true",
                        help="skip creating ClickHouse marts tables")
    args = parser.parse_args()

    if args.init:
        run_init()
    run_pipeline(ingest_method=args.ingest, ingest_mode=args.mode,
                 do_serve=not args.no_serve)


if __name__ == "__main__":
    main()

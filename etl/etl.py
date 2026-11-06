"""Medallion ETL: ingestion -> bronze -> silver -> gold -> mart -> serve.

Usage:
  python etl.py --init      # seed pos, then ingest -> bronze -> transform -> serve
  python etl.py             # ingest -> bronze -> transform -> serve
  python etl.py --no-serve  # stop after the lake layers
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


def _step(name, run):
    t0 = time.time()
    try:
        run()
    except Exception as e:
        print(f"ERROR in {name} step: {e}", file=sys.stderr)
        raise
    print(f"{name} in {time.time() - t0:.3f}s")


def run_pipeline(do_serve=True):
    _step("ingestion (sling)", ingest.run_sling)
    _step("bronze (delta)", bronze.run_bronze)
    _step("transform (silver/gold/mart delta)", transform.run_transform)
    if do_serve:
        _step("serve (clickhouse marts)", serve.run_serve)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="create pos schema + seed sample data")
    parser.add_argument("--no-serve", action="store_true",
                        help="skip loading the ClickHouse marts tables")
    args = parser.parse_args()

    if args.init:
        run_init()
    run_pipeline(do_serve=not args.no_serve)


if __name__ == "__main__":
    main()

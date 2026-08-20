"""Medallion ETL: Postgres(oltp) --Sling--> bronze (RustFS) --Ibis--> silver/gold/mart.

Usage:
  python etl.py           # migrate (sling) -> transform (ibis)
  python etl.py --init    # create oltp schema + seed sample data first, then run
"""
import argparse
import os
import time

import db
import migrate
import transform

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_init():
    db.execute_file(os.path.join(SQL_DIR, "00_init.sql"))


def run_pipeline():
    t0 = time.time()
    migrate.run_migrate()
    print(f"migrate (oltp -> rustfs bronze, sling) in {time.time() - t0:.3f}s")

    t0 = time.time()
    transform.run_transform()
    print(f"transform (ibis silver/gold/mart) in {time.time() - t0:.3f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="create oltp schema + seed sample data")
    args = parser.parse_args()
    if args.init:
        run_init()
    run_pipeline()


if __name__ == "__main__":
    main()

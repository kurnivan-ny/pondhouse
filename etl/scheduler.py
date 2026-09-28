"""Scheduler: generates sample data + runs the medallion ETL on a cron.

Each cycle inserts one new order (manual entry simulation) then runs
ingest -> bronze -> silver -> gold -> mart. Cadence is ETL_SCHEDULE_CRON
(5-field cron), default "*/5 * * * *".

Usage:
  python scheduler.py          # run forever on the configured schedule
  python scheduler.py --once   # run a single cycle and exit
"""
import argparse
import logging

from apscheduler.schedulers.blocking import BlockingScheduler

import db
import etl
import generate_data
from config import SCHEDULE_CRON

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def cycle():
    conn = db.get_conn()
    try:
        customer_ids, store_ids, products = generate_data.load_lookup(conn)
        transaction_id = generate_data.insert_transaction(conn, customer_ids, store_ids, products)
        conn.commit()
        logging.info("generated transaction %s", transaction_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    etl.run_pipeline()
    logging.info("ETL cycle complete")


def _cron_kwargs(cron):
    """Parse 5-field cron expression (minute hour day month day_of_week)."""
    parts = cron.strip().split()
    if len(parts) != 5:
        raise ValueError(
            f"Invalid cron expression (expected 5 fields, got {len(parts)}): {cron}"
        )
    minute, hour, day, month, dow = parts
    return dict(minute=minute, hour=hour, day=day, month=month, day_of_week=dow)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="run a single cycle and exit")
    args = parser.parse_args()

    if args.once:
        cycle()
        return

    try:
        cron_kwargs = _cron_kwargs(SCHEDULE_CRON)
    except ValueError as e:
        logging.error("Invalid cron configuration: %s", e)
        return

    scheduler = BlockingScheduler()
    scheduler.add_job(cycle, "cron", **cron_kwargs)
    logging.info("scheduler started with cron %s", SCHEDULE_CRON)
    scheduler.start()


if __name__ == "__main__":
    main()

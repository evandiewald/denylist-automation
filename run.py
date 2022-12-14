import datetime

import sqlalchemy.exc

from api import get_issues, get_entries, get_pulls
import connection
from sqlalchemy.engine import create_engine, Engine
from models.migrations import migrate
from queries import *
from dotenv import load_dotenv
import os
import logging
from aws import upload_dict
import boto3


# how far back (in days) should we go when generating reports? on successive passes, only new entries will be processed
N_DAYS = 90

session = boto3.Session(
    profile_name="default"
)

MIGRATE = False
logging.basicConfig(level=logging.INFO)

load_dotenv()

etl_engine = connection.connect()
denylist_engine = create_engine(os.getenv("DENYLIST_DB_CONNECTION_STRING"), pool_recycle=3600)

s3 = session.resource("s3")
bucket = s3.Bucket(os.getenv('S3_BUCKET'))

# run migrations
if MIGRATE is True:
    logging.info("Running migrations...")
    migrate()
    logging.info("Migrations complete.")


def get_new_issues(etl_engine: Engine, denylist_engine: Engine):
    since: datetime.datetime = get_max_issue_timestamp(denylist_engine)
    try:
        since_iso = since.isoformat()
    except:
        since_iso = None

    logging.info(f"Processing new denylist issues since {since_iso}")

    logging.debug("Getting gateway_inventory from ETL")
    gateway_inventory = get_gateway_inventory(etl_engine)
    logging.debug("Getting issues from Github API")
    issues = get_issues(since=since_iso)
    logging.debug("Parsing issues for individual hotspot entries")
    entries = get_entries(issues, gateway_inventory)

    insert_records(denylist_engine, issues, entries)


def update_issues(denylist_engine: Engine):
    logging.info(f"Checking for updates in denylist issues")
    logging.info("Getting issues from Github API")
    issues = get_issues(since=None)

    upsert_issues(denylist_engine, issues)


def update_entries(etl_engine: Engine, denylist_engine: Engine):
    logging.info("Getting gateway_inventory from ETL")
    gateway_inventory = get_gateway_inventory(etl_engine)
    logging.info("Looking for unparsed issues to process for entries")
    unparsed_issues = get_unparsed_issues(denylist_engine)
    logging.info("Parsing issues for individual hotspot entries")
    entries = get_entries(unparsed_issues, gateway_inventory)

    upsert_entries(denylist_engine, entries)


def update_pulls(denylist_engine: Engine):
    logging.info("Checking for new or updated PR's")
    pulls, issue_joins = get_pulls()
    upsert_pulls(denylist_engine, pulls, issue_joins)


def generate_reports(etl_engine: Engine, denylist_engine: Engine):
    since = datetime.datetime.now() - datetime.timedelta(days=N_DAYS)
    pending_issues = get_issues_without_reports(denylist_engine, since.date().isoformat())

    for issue in pending_issues:
        logging.info(f"Processing issue {issue}")
        issue_details = get_issue_details(denylist_engine, issue, with_body=False)
        addresses = get_entries_for_issue(denylist_engine, issue)
        max_block = get_height_for_timestamp(etl_engine, issue_details["created_at"])
        upload_dict(bucket, issue_details, f"issues/{issue}/issue_details")
        for address in addresses:
            try:
                logging.info(f"Processing address {address} in issue {issue}")
                # get json datasets
                distance_vs_rssi = get_distance_vs_rssi(etl_engine, address, max_block=max_block)
                witnessed_makers = get_witnessed_makers(etl_engine, address, max_block=max_block)
                hotspot_details = get_hotspot_details(etl_engine, address)
                witness_graph = get_witness_graph(etl_engine, address, max_block=max_block)
                rssi_vs_snr = get_rssi_vs_snr(etl_engine, address, max_block=max_block)

                # upload to S3
                upload_dict(bucket, distance_vs_rssi, f"issues/{issue}/entries/{address}/distance_vs_rssi")
                upload_dict(bucket, witnessed_makers, f"issues/{issue}/entries/{address}/witnessed_makers")
                upload_dict(bucket, hotspot_details, f"issues/{issue}/entries/{address}/hotspot_details")
                upload_dict(bucket, witness_graph, f"issues/{issue}/entries/{address}/witness_graph")
                upload_dict(bucket, rssi_vs_snr, f"issues/{issue}/entries/{address}/rssi_vs_snr")

                mark_entry_report_as_complete(denylist_engine, address, issue)
            except sqlalchemy.exc.NoResultFound:
                continue
        mark_issue_report_as_complete(denylist_engine, issue)


update_issues(denylist_engine)
update_entries(etl_engine, denylist_engine)
update_pulls(denylist_engine)
generate_reports(etl_engine, denylist_engine)
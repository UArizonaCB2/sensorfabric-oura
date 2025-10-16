#!/usr/bin/env python3

import os
import argparse
import logging
from Ingestor import ingestor

def controller(directory:str, s3_path:str, database:str,
               last_sync_date:str=None, pid:str=None):
    """
    Main controller that scans through all the folders,
    with a device specific folder export structure.

    Paramters
    1. directory - Path to the device export directory.
    2. s3_path - AWS S3 path for data upload.
    3. database - AWS Glue database name.
    4. last_sync_date - Date after which (not inclusive) device data should be
    synced with the cloud platform. Default None, sync everything.
    5. pid - PID of the specific participants that we want to ingest.
    Defaults to ingesting all participants.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if not os.path.isdir(directory):
        logger.exception(f"{directory} must be a directory path.")

    for instance in os.listdir(directory):
        path = os.path.join(directory, instance)
        if not os.path.isdir(path):
            logger.warning(f"Skipping {path} as it is not a directory")
            continue

        # Let's pass this to the ingestor for it to take care of.
        ingestor(path, s3_path, database, pid)

if __name__ == "__main__":
    cmdparser = argparse.ArgumentParser(description='Controller to ingest Oura v2 files.')
    cmdparser.add_argument('-d', '--directory', type=str, help='Path to the participant_data/ directory', required=True)
    cmdparser.add_argument('-p', '--s3_path', type=str, help='Path to where the parquet files will be uploaded in S3', required=True)
    cmdparser.add_argument('-db', '--database', type=str, help='AWS Glue database name', required=True)
    cmdparser.add_argument('-s', '--last_sync_date', type=str, help='Enter the last date (YYYY-MM-DD) till which the controller has uploaded data to remote S3. If not provided everything is uploaded to AWS If not provided everything is uploaded to AWS.', required=False)
    cmdparser.add_argument('-pid', '--pid', type=str, help='Only upload data for the specific PID', required=False)
    args = cmdparser.parse_args()

    directory = args.directory
    s3_path = args.s3_path
    database = args.database
    last_sync_date = args.last_sync_date
    pid = args.pid

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.info('Starting Device Controller')

    """
    The ingestor assumes that there is a local file named "whitelist.txt" that contains
    the names of the tables / metrics that need to be ingested. One table name per line.
    If this file is not there, we will just self-destruct here.
    """
    if not os.path.isfile('whitelist.txt'):
        logger.exception('Cannot find whitelist.txt in the local directory')

    controller(directory, s3_path, database, last_sync_date, pid)

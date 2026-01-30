#!/usr/bin/env python3

import os
import sys
import argparse
import logging
from Ingestor import ingestor
import clickhouse_connect
from dotenv import load_dotenv
from utils import get_pids_from_clickhouse, get_pids_from_directory, createTableWhitelist

# Take it all in!
load_dotenv()

def controller(directory:str, s3_path:str, database:str,
               last_sync_date:str=None, pid:str=None,
               storage:str='athena', clickhouse_config:dict={}, update_mode:bool=False,
               config_options:list=None):
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
    6. storage - Storage backend: 'athena' or 'clickhouse'
    7. clickhouse_config - Configuration dict for ClickHouse connection
    8. update_mode - If True, only process PIDs not already in database (ClickHouse only)
    9. config_options - List of command line arguments the program was called with
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if not os.path.isdir(directory):
        logger.exception(f"{directory} must be a directory path.")

    # Display run configuration
    logger.info("="*60)
    logger.info("Your current run configuration is")
    logger.info("="*60)
    logger.info(f"Storage backend: {storage}")
    logger.info(f"Database/schema name: {database}")
    logger.info(f"Current data directory: {directory}")
    logger.info(f"Update mode: {'yes' if update_mode else 'no'}")

    # Display whitelisted tables
    try:
        table_whitelist = createTableWhitelist()
        logger.info(f"Tables to ingest: {', '.join(table_whitelist)}")
    except Exception as e:
        logger.warning(f"Could not read whitelist.txt: {e}")

    if config_options:
        logger.info(f"Config options: {' '.join(config_options)}")
    logger.info("="*60)

    # If the user has requested to use clickhouse drivers for storage, set things up here.
    clickhouse_client = None
    if storage == 'clickhouse':
        clickhouse_client = clickhouse_connect.get_client(host=clickhouse_config['host'],
                                                          port=clickhouse_config['port'],
                                                          username=clickhouse_config['user'],
                                                          password=clickhouse_config['pass'],
                                                          database=clickhouse_config['db'])

    # Handle update mode: find new PIDs to process
    pids_to_process = None  # None means process all PIDs
    if update_mode:
        logger.info("Update mode enabled: scanning for new PIDs...")

        # Ask user for the master table to query for PIDs
        master_table = input("Enter the master table name to look up PIDs from [temperature]: ").strip()
        if not master_table:
            master_table = "temperature"
        logger.info(f"Using '{master_table}' as the master table for PID lookup")

        # Get PIDs from ClickHouse database
        db_pids = get_pids_from_clickhouse(clickhouse_client, clickhouse_config['db'], master_table)

        # Get PIDs from directory structure
        dir_pids = get_pids_from_directory(directory)

        # Find new PIDs (in directory but not in database)
        new_pids = dir_pids - db_pids

        if len(new_pids) == 0:
            logger.info("No new PIDs found. Database is up to date.")
            return

        # Display new PIDs and get user confirmation
        sorted_new_pids = sorted(new_pids)
        logger.info(f"Found {len(new_pids)} new PID(s) to process:")
        logger.info("New PIDs to be ingested:")
        for pid in sorted_new_pids:
            logger.info(f"  - {pid}")

        # Prompt for confirmation (default is Yes)
        response = input("\nIngest the listed new pids[Y|n]? ").strip().lower()

        if response == 'n' or response == 'no':
            logger.info("Exiting without doing anything. No new PIDs have been ingested")
            return

        logger.info(f"Proceeding with ingestion of {len(new_pids)} new PID(s)")
        pids_to_process = new_pids

    # Track successful and failed PIDs
    successful_pids = set()
    failed_pids = set()

    for instance in os.listdir(directory):
        path = os.path.join(directory, instance)
        if not os.path.isdir(path):
            logger.warning(f"Skipping {path} as it is not a directory")
            continue

        # In update mode, process only new PIDs
        # In regular mode with a specific PID, process only that PID
        # Otherwise, process all PIDs
        if update_mode and pids_to_process is not None:
            # Process each new PID separately
            for new_pid in pids_to_process:
                result = ingestor(path, database, s3_path, str(new_pid), storage, clickhouse_client)
                if result:
                    successful_pids.add(new_pid)
                else:
                    failed_pids.add(new_pid)
        else:
            # Regular mode: let ingestor handle PID filtering (or process all)
            result = ingestor(path, database, s3_path, pid, storage, clickhouse_client)
            # In regular mode without update, we don't track individual PIDs since we might be processing all

    # Display end-of-program summary
    logger.info("="*60)
    logger.info("End of program summary")
    logger.info("="*60)

    if update_mode and pids_to_process is not None:
        # Show detailed PID-level results for update mode
        if successful_pids:
            logger.info(f"Successfully ingested PIDs ({len(successful_pids)}): {', '.join(map(str, sorted(successful_pids)))}")
        else:
            logger.info("Successfully ingested PIDs (0): None")

        if failed_pids:
            logger.error(f"Failed to ingest PIDs ({len(failed_pids)}): {', '.join(map(str, sorted(failed_pids)))}")
        else:
            logger.info("Failed to ingest PIDs (0): None")
    else:
        # For regular mode, just indicate completion
        logger.info("Ingestion process completed")

    logger.info("="*60)

if __name__ == "__main__":
    # Configure logging with both console and file handlers
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('controller.logs'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info('Starting Device Controller')

    cmdparser = argparse.ArgumentParser(description='Controller to ingest Oura v2 files.')
    cmdparser.add_argument('-d', '--directory', type=str, help='Path to the participant_data/ directory', required=True)
    cmdparser.add_argument('-db', '--database', type=str, help='AWS Glue database name', required=True)
    cmdparser.add_argument('-p', '--s3_path', type=str, help='Path to where the parquet files will be uploaded in S3', required=False)
    cmdparser.add_argument('-s', '--last_sync_date', type=str, help='Enter the last date (YYYY-MM-DD) till which the controller has uploaded data to remote S3. If not provided everything is uploaded to AWS If not provided everything is uploaded to AWS.', required=False)
    cmdparser.add_argument('-pid', '--pid', type=str, help='Only upload data for the specific PID', required=False)
    cmdparser.add_argument('--storage', type=str, help='Either athena or clickhouse for data storage [default:athena].', required=False, default='athena')
    cmdparser.add_argument('--clickhouse_host', type=str, help='Clickhouse server host [default:localhost].', required=False, default='localhost')
    cmdparser.add_argument('--clickhouse_port', type=int, help='Clickhouse server HTTP port for connection [default:8123].', required=False, default=8123)
    cmdparser.add_argument('--update', action='store_true', help='Update mode: only ingest data for new PIDs not already in the database. Only supported with ClickHouse backend.', required=False)
    args = cmdparser.parse_args()

    directory = args.directory
    s3_path = args.s3_path
    database = args.database
    last_sync_date = args.last_sync_date
    pid = args.pid

    # If clickhouse server was requested as backend fill out the clickhouse config
    clickhouse_config = {}
    if args.storage and args.storage == 'clickhouse':
        clickhouse_config['host'] = args.clickhouse_host
        clickhouse_config['port'] = args.clickhouse_port

        # Get the clickhouse user and password from environment or throw errors.
        if not os.getenv('CLICKHOUSE_USER'):
            logger.exception("Environment variable CLICKHOUSE_USER must be set when using clickhouse")
            raise Exception("Username for clickhouse not provided")
        if not os.getenv('CLICKHOUSE_PASS'):
            logger.exception("Environment variable CLICKHOUSE_PASS must be set when using clickhouse")
            raise Exception("Password for clickhouse not provided")
        if not os.getenv('CLICKHOUSE_DB'):
            logger.exception("Environment variable CLICKHOUSE_DB must be set when using clickhouse")
            raise Exception("Database for clickhouse not provided")
    
        clickhouse_config['user'] = os.getenv('CLICKHOUSE_USER')
        clickhouse_config['pass'] = os.getenv('CLICKHOUSE_PASS')
        clickhouse_config['db'] = os.getenv('CLICKHOUSE_DB')
    elif args.storage and args.storage == 'athena':
        # We need to make sure that s3 option is provided if it is using the athena storage backend.
        if not args.s3_path:
            logger.exception("S3 path must be provided if using athena storage driver")
            raise Exception("S3 path option for athena not provided")
    elif args.storage:
        logger.exception(f"{args.storage} is an unsupported storage driver. Either select \"athena\" or \"clickhouse\" ")
        raise Exception("Unsupported storage driver requested")

    # Validate that --update is only used with ClickHouse backend
    if args.update and args.storage != 'clickhouse':
        logger.exception("--update mode is only supported with ClickHouse storage backend")
        raise Exception("--update mode requires --storage clickhouse")

    """
    The ingestor assumes that there is a local file named "whitelist.txt" that contains
    the names of the tables / metrics that need to be ingested. One table name per line.
    If this file is not there, we will just self-destruct here.
    """
    if not os.path.isfile('whitelist.txt'):
        logger.exception('Cannot find whitelist.txt in the local directory')

    controller(directory, s3_path, database, last_sync_date, pid, args.storage, clickhouse_config, args.update, sys.argv)

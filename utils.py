"""
Author: Shravan Aras <shravanars@arizona.edu>
Organization: University of Arizona

Description:
Utility function files that are shared across the whole Empatica ingestion head.
"""

import os
import awswrangler as wr
import logging
import re
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

def createTableWhitelist():
    """
    Looks for a local file called "whitelist.txt" that should contain the table names to ingest.
    Parses it with table name per line, and returns a list with those names.
    """
    if not os.path.isfile('whitelist.txt'):
        raise Exception('File whitelist.txt not found in local directory.')
    f = open('whitelist.txt', 'r')
    tables = []
    for line in f.readlines():
        if len(line) > 0:
            line = line.strip()
            tables.append(line)

    return tables

def uploadToAWS(df, s3_path, database, table_name, partition_key):
    """
    Method which uploads the dataframes to the AWS infrastructure.

    Parameters:
    df(pandas.DataFrame) : Pandas DataFrame to ingest.
    s3_path(str) : S3 URI where raw parquet files will be stored. eg: s3://bucket/object/
    database(str) : The AWS Glue database for this data
    table_name(str) : The AWS Glue table to this data
    partition_key(str) : Key on which to partition the data in Glue

    Returns:
    (bool, error message) : (true, None) on success and (false, err message) on failure.
    """
    # Create a table path that has the modality / table-name attached to it.
    table_path = f"{s3_path}/{table_name}/"
    if s3_path[-1] == '/': # we don't add additional / if s3_path variable had one at the end.
        table_path = f"{s3_path}{table_name}/"

    parquet_success = False
    err = None
    try:
        # Save the DataFrame to S3 in parquet format
        wr.s3.to_parquet(
            df=df,
            path=table_path,
            dataset=True,
            database=database,
            table=table_name,
            partition_cols=[partition_key]
        )
        parquet_success = True

    except Exception as e:
        err = f"ERROR : in writing parquet files: {repr(e)}"

    return parquet_success, err

def uploadToClickHouse(df, client, table_name):
    """
    Method which uploads DataFrames to ClickHouse database.

    Parameters:
    df(pandas.DataFrame) : Pandas DataFrame to ingest.
    client(clickhouse_connect.driver.Client) : ClickHouse client connection
    table_name(str) : The ClickHouse table name to insert data into

    Returns:
    (bool, error message) : (True, None) on success and (False, err message) on failure.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    insert_success = False
    err = None

    try:
        # Insert the DataFrame into ClickHouse table
        client.insert_df(table_name, df)
        insert_success = True
        logger.debug(f"Successfully inserted {len(df)} rows into {table_name}")

    except Exception as e:
        err = f"ERROR: in inserting data to ClickHouse: {repr(e)}"
        logger.error(err)

    return insert_success, err

def get_pids_from_clickhouse(clickhouse_client, database:str, table_name:str):
    """
    Query ClickHouse database to get all distinct PIDs from a master table.

    Parameters:
    clickhouse_client - ClickHouse client connection
    database (str) - Database name to query
    table_name (str) - Master table name to query for PIDs

    Returns:
    set - Set of all PIDs found in the specified table

    Raises:
    Exception - If the table does not exist in the database
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    all_pids = set()

    try:
        # First check if the table exists
        check_query = f"EXISTS TABLE {database}.{table_name}"
        exists_result = clickhouse_client.command(check_query)

        if exists_result != 1:
            error_msg = f"The given table to look up PIDs from '{table_name}' does not exist in the database. Do you have the right storage configuration?"
            logger.error(error_msg)
            logger.error(f"{Fore.RED}Exiting due to errors{Style.RESET_ALL}")
            raise Exception(error_msg)

        # Query to get distinct PIDs from this table
        query = f"SELECT DISTINCT pid FROM {database}.{table_name}"
        result = clickhouse_client.query(query)

        # Add PIDs from this table to the set
        for row in result.result_rows:
            all_pids.add(int(row[0]))

        logger.info(f"Found {len(all_pids)} distinct PIDs in table '{table_name}'")

    except Exception as e:
        # If we already logged the error above, just re-raise
        if "does not exist in the database" in str(e):
            raise
        # Otherwise this is a different error
        error_msg = f"Error querying table '{table_name}': {repr(e)}"
        logger.error(error_msg)
        logger.error(f"{Fore.RED}Exiting due to errors{Style.RESET_ALL}")
        raise Exception(error_msg)

    return all_pids

def get_pids_from_directory(directory:str):
    """
    Scan directory structure to find all PIDs from CSV filenames.

    Parameters:
    directory (str) - Top level directory containing participant folders

    Returns:
    set - Set of all PIDs found in the directory structure
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    all_pids = set()
    exp = r"^([a-z0-9]+)_(\d+)_([a-z]+)_(\d+)\.csv$"

    if not os.path.isdir(directory):
        logger.error(f"{directory} is not a valid directory")
        return all_pids

    # Scan through all subdirectories
    for instance in os.listdir(directory):
        path = os.path.join(directory, instance)
        if not os.path.isdir(path):
            continue

        # Scan CSV files in this directory
        for filename in os.listdir(path):
            match = re.match(exp, filename, re.IGNORECASE)

            if match and len(match.groups()) == 4:
                pid = int(match.group(4))
                all_pids.add(pid)

    logger.info(f"Found {len(all_pids)} total distinct PIDs in directory structure")
    return all_pids

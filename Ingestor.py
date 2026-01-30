#!/usr/bin/env python3

import pandas as pd
import os
from utils import createTableWhitelist, uploadToAWS, uploadToClickHouse
import re
import logging
from datetime import datetime
import numpy as np
from typing import Tuple, Optional

"""
Pandas to Hadoop datatype mapper.
---------------------------------
The hadoop data type names are different from the pandas datatype names.
This map does a "best-effort" job to provide a conversion for some of them.
Add more to these as we see newer datatypes.
"""
dtype_mapper = {
    'integer': 'bigint',
    'floating': 'double',
    'datetime64[ns]': 'timestamp',
}

# Global cache for ClickHouse table schemas
# Key: table_name, Value: schema dictionary from DESCRIBE TABLE
clickhouse_schema_cache = {}

def _activity_modifier(df: pd.DataFrame()) -> pd.DataFrame():
    """Modifier mehtod for the activity table"""

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Extract the timezone offset from `day_start`. This is the only place
    # where I currently see this offset avaliable.
    day_start = df['day_start'].values
    tz_offset = []  # Store the timezone offsets here.
    for ds in day_start:
        try:
            dt = datetime.fromisoformat(ds)
            tzname = dt.tzname()  # Example UTC-04:00
            tzoffset = tzname[3:]  # Isolates -04:00
            buff = tzoffset.split(':')
            hrs = int(buff[0])
            mins = int(buff[1])
            tz_offset.append(hrs*60 + mins)

        except ValueError:
            logger.warn(f"Failed to convert {ds} into datetime for timezone extraction")
            tz_offset.append(np.nan)

    # Change some of the fields to datetime64 at to utc timestamp.
    fields = ['summary_date', 'day_start', 'day_end']
    for field in fields:
        try:
            df[field] = pd.to_datetime(df[field], utc=True)
        except (ParserError, ValueError):
            logger.error('Unable to convert timestamp field to datetime64.')
            logger.error(f"Sample value that failed conversion - {df['timestamp'][0]}")
            return None

    # Rename some of the columns for better readability and understanding
    df = df.rename(columns={'summary_date': 'summary_date_utc',
                            'day_start': 'day_start_utc',
                            'day_end': 'day_end_utc'})

    # Finally add the timezone column to all of this.
    df['tzoffset'] = tz_offset

    return df

def _temp_modifier(df: pd.DataFrame()) -> pd.DataFrame():
    """Modifier method for the temperature table"""

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Drop the email column and also the group column from the frame.
    df = df.drop(['email', 'group', 'name', 'participant_id'], axis=1)

    # Converting `timestamp` datatype from object to datetime64
    if (df['timestamp'].dtype != 'datetime64[ns]'):
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        except (ParserError, ValueError):
            logger.error('Unable to convert timestamp field to datetime64.')
            logger.error(f"Sample value that failed conversion - {df['timestamp'][0]}")
            return None

    # Also want to rename `timestamp` to be a bit a more explicit about it.
    df = df.rename(columns={'timestamp': 'timestamp_utc'})

    return df

def removeSensitive(df: pd.DataFrame) -> pd.DataFrame:
    """Remove any sensitive identifiers"""

    default_values = {
        'email': 'foo@bar.org',
        'group': 'g1',
        'name': 'john doe',
        'participant_id': '000-0000',
    }

    # Replace the sensitive columns with default values.
    cols = df.columns
    for col in cols:
        if col in default_values:
            df[col] = default_values[col]

    return df

# Modifier functions for specific tables. If a table has one then they will be called.
MODIFIERS = {
}

def ingestor(directory, database, s3_path:str='', specific_pid=None, storage='athena', clickhouse_client=None):
    """
    Function to correctly parse the directory structure, extract the data from the CSV files and then upload them to the correct storage backend.

    Parameters:
    directory(str) - Top level date folder path
    database (str) - AWS Glue/Athena database name or ClickHouse database name
    s3_path (str) - S3 path where the parquet files will be stored (for Athena backend)
    specific_pid (str) - Only ingest data for this specific participant ID
    storage (str) - Storage backend to use: 'athena' or 'clickhouse' (default: 'athena')
    clickhouse_client - ClickHouse client connection (required if storage='clickhouse')
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # The names of tables to ingest.
    table_whitelist = createTableWhitelist()

    # Each file inside this directory is a table with Oura data information
    # that needs to be ingested.
    exp = r"^([a-z0-9]+)_(\d+)_([a-z]+)_(\d+)\.csv$"
    for filename in os.listdir(directory):
        match = re.match(exp, filename, re.IGNORECASE)

        pid = None
        table_name = None

        if match and len(match.groups()) == 4:
            table_name = match.group(1)
            table_name = table_name.replace('-', '_')
            pid = match.group(4)
        else:
            logger.warning(f'Skipping {os.path.join(directory, filename)} as name is of incorrect format.')
            continue

        if table_name not in table_whitelist:
            # Silently skip this table name as its not in the whitelist
            continue

        # If we have provided a specific PID then we only want to ingest that one.
        if specific_pid is not None and not(pid == specific_pid):
            continue

        # Open this CSV file and get the information as a dataframe.
        df = pd.read_csv(os.path.join(directory, filename))

        # Attach a new pid column and fill that in.
        df['pid'] = pid
        df['pid'] = df['pid'].astype('int64')

        # If there is a table modifier attached to this we will call it.
        if table_name in MODIFIERS:
            df = MODIFIERS[table_name](df)
            if df is None:
                logger.error(f"Failed to ingest {os.path.join(directory, filename)}")

        df = removeSensitive(df)

        status = False
        err = None

        # We are now ready to push this up to the storage backend.
        # Only push data if we are not running in dry run mode.
        if os.getenv('ENVIRONMENT', 'PRODUCTION') == 'PRODUCTION':
            if storage == 'athena':
                status, err = _backend_athena(df, database, table_name, s3_path)
            elif storage == 'clickhouse':
                status, err = _backend_clickhouse(df, table_name, clickhouse_client)
        else:
            logger.info(f'Running in non-production mode (dry run), nothing will be uploaded')
            status = True
            err = None

        if not status:
            logger.error(f'Could not write file {os.path.join(directory, filename)} into table {table_name}')
            logger.error(err)

    return True

def _backend_athena(df: pd.DataFrame, database: str, table_name: str, s3_path: str) -> Tuple[bool, Optional[str]]:
    """
    Upload data to AWS Athena/Glue storage backend.

    Parameters:
    df (pd.DataFrame): Pandas DataFrame containing the data to upload
    database (str): AWS Glue database name
    table_name (str): AWS Glue table name
    s3_path (str): S3 URI where parquet files will be stored

    Returns:
    Tuple[bool, Optional[str]]: (True, None) on success, (False, error_message) on failure

    Notes:
    Clickhouse automatically converts DateTime64[ns] to epoch values internally using the timezone
    information present in the datetime string. Hence it is always best to pass it timezone aware
    stirng only. If a timezone naive string is passed then it will interepret it as being the systems local
    timezone and cause all sorts of confusion. 
    """
    status, err = uploadToAWS(df, s3_path, database, table_name, 'pid')

    return status, err

def _backend_clickhouse(df: pd.DataFrame, table_name: str, clickhouse_client: Optional[object]) -> Tuple[bool, Optional[str]]:
    """
    Upload data to ClickHouse storage backend.

    Parameters:
    df (pd.DataFrame): Pandas DataFrame containing the data to upload
    table_name (str): ClickHouse table name
    clickhouse_client (Optional[object]): ClickHouse client connection object (required for upload)

    Returns:
    Tuple[bool, Optional[str]]: (True, None) on success, (False, error_message) on failure
    """
    global clickhouse_schema_cache

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if clickhouse_client is None:
        logger.error('ClickHouse client is required when using clickhouse storage backend')
        status = False
        err = 'ClickHouse client not provided'
        return status, err

    # Check if table schema is cached
    if table_name not in clickhouse_schema_cache:
        logger.debug(f"Table {table_name} not in schema cache, querying ClickHouse...")

        try:
            # Query ClickHouse to check if table exists and get its schema
            describe_result = clickhouse_client.query(f"DESCRIBE TABLE {table_name}")

            # If we get here, table exists. Build schema from result
            schema = {}
            for row in describe_result.result_rows:
                # Each row is (name, type, default_type, default_expression, comment, codec_expression, ttl_expression)
                column_name = row[0]
                column_type = row[1]
                schema[column_name] = column_type

            # Cache the schema
            clickhouse_schema_cache[table_name] = schema
            logger.info(f"Cached schema for table {table_name} with {len(schema)} columns")

        except Exception as e:
            # Table doesn't exist or query failed
            logger.error(f"Table {table_name} not found in ClickHouse database. Ignoring this data. Error: {repr(e)}")
            status = False
            err = f"Table {table_name} does not exist in ClickHouse"
            return status, err

    # Process DataFrame to match ClickHouse schema
    df = df.copy()  # Create a copy to avoid modifying the original
    schema = clickhouse_schema_cache[table_name]

    # Process DateTime/Date fields
    for column_name, column_type in schema.items():
        # Check if this is a DateTime or Date field in ClickHouse
        if column_name not in df.columns:
            logger.error(f"Mismatch between database schema and dataframe provided. Table: {table_name}, Missing column: {column_name}")
            status = False
            err = f"Column '{column_name}' exists in ClickHouse table '{table_name}' schema but not in provided DataFrame"
            return status, err

        # Handle DateTime fields
        if 'DateTime' in column_type or 'Date' in column_type:
            # Convert to pandas datetime if not already
            if df[column_name].dtype != 'datetime64[ns]':
                try:
                    df[column_name] = pd.to_datetime(df[column_name])
                except Exception as e:
                    logger.error(f"Failed to convert column '{column_name}' to datetime for table '{table_name}'. ClickHouse expects DateTime but conversion failed: {repr(e)}")
                    status = False
                    err = f"Cannot convert column '{column_name}' to datetime as required by ClickHouse schema for table '{table_name}'"
                    return status, err

            # Check if datetime is timezone-aware by sampling first non-null element
            sample_value = df[column_name].dropna().iloc[0] if len(df[column_name].dropna()) > 0 else None

            if sample_value is not None and hasattr(sample_value, 'tzinfo') and sample_value.tzinfo is not None:
                logger.debug(f"Column {column_name} has timezone-aware data, extracting offset...")

                # Extract timezone offset for all rows and convert to minutes
                tzoffset_minutes = []
                for val in df[column_name]:
                    if pd.isna(val):
                        tzoffset_minutes.append(None)
                    else:
                        # Get UTC offset in seconds, convert to minutes
                        offset_seconds = val.utcoffset().total_seconds()
                        offset_minutes = int(offset_seconds / 60)
                        tzoffset_minutes.append(offset_minutes)

                # Add tzoffset column if it exists in schema
                if 'tzoffset' in schema:
                    df['tzoffset'] = tzoffset_minutes
                    logger.debug(f"Added tzoffset column with values for {column_name}")

        # Handle String fields - fill NaN values with empty string
        elif 'String' in column_type:
            # Replace NaN values with empty string
            if df[column_name].isna().any():
                null_count = df[column_name].isna().sum()
                df[column_name] = df[column_name].fillna('')
                logger.debug(f"Filled {null_count} NaN values with empty string in column '{column_name}'")

    # Proceed with upload
    status, err = uploadToClickHouse(df, clickhouse_client, table_name)

    return status, err

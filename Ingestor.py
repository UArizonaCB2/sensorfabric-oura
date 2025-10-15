#!/usr/bin/env python3

import pandas as pd
import awswrangler as wr
import boto3
import os
from utils import createTableWhitelist, uploadToAWS
import re
import logging
from datetime import datetime
import numpy as np

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

def removeSensitive(df: pd.DataFrame()) -> pd.DataFrame():
    """Remove any sensitive identifiers"""

    df = df.drop(['email', 'group', 'name', 'participant_id'], axis=1)

    return df

# Modifier functions for specific tables. If a table has one then they will be called.
MODIFIERS = {
}

def ingestor(directory, s3_path, database, mode='append'):
    """
    Function to correctly parse the directory structure, extract the data from the CSV files and then upload them to the correct AWS table.

    Parameters:
    directory(str) - Top level date folder path
    s3_path (str) - S3 path where the parquet files will be stored
    database (str) - AWS Glue/Athena database name
    mode (str) - (Default) "append" to keep any possible existing table or  "overwrite" to recreate any possible existing table
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

        # We are now ready to push this up to the SF backend.
        status, err = uploadToAWS(df, s3_path, database,
                                  table_name, 'pid')

        if not status:
            logger.error(f'Could not write file {os.path.join(directory, filename)} into table {table_name}')
            logger.error(err)

    return True

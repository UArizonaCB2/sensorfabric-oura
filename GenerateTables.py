import clickhouse_connect
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Load environment variables from .env file
load_dotenv()

# Set up logging with colors
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{Style.RESET_ALL}"
        record.msg = f"{log_color}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

def setup_logging(log_file='tables.logs'):
    """Set up dual logging to console and file"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers = []

    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # File handler without colors
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def generate_create_table_sql(table_name, schema_fields, order_by_fields, database_name):
    """
    Generate ClickHouse CREATE TABLE SQL statement from schema data.

    Args:
        table_name: Name of the table to create
        schema_fields: List of field definitions from JSON schema
        order_by_fields: List of field names for ORDER BY clause
        database_name: Name of the database

    Returns:
        SQL CREATE TABLE statement as string
    """
    fields = []

    for field in schema_fields:
        field_name = field['field_name']
        field_type = field['suggested_clickhouse_type']

        # Build field definition with optional DEFAULT clause
        field_def = f"    `{field_name}` {field_type}"

        # Add DEFAULT clause if specified in schema
        if 'default' in field and field['default'] is not None:
            default_value = field['default']
            # Format default value based on type
            if isinstance(default_value, str):
                # Check if it's the '-inf' special value for Float64
                if default_value == '-inf':
                    field_def += f" DEFAULT -inf"
                else:
                    # String default - use single quotes (empty string)
                    field_def += f" DEFAULT ''"
            elif isinstance(default_value, int):
                # Integer default
                field_def += f" DEFAULT {default_value}"

        fields.append(field_def)

    fields_sql = ",\n".join(fields)

    # Generate ORDER BY clause
    if order_by_fields and len(order_by_fields) > 0:
        order_by_clause = ", ".join([f"`{field}`" for field in order_by_fields])
        order_by_sql = f"ORDER BY ({order_by_clause})"
    else:
        order_by_sql = "ORDER BY tuple()"

    sql = f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
(
{fields_sql}
)
ENGINE = MergeTree()
{order_by_sql}"""

    return sql

def create_tables_from_schemas(schema_folder, clickhouse_host, clickhouse_port, database_name):
    """
    Read schema JSON files and create tables in ClickHouse.

    Args:
        schema_folder: Path to folder containing schema JSON files
        clickhouse_host: ClickHouse server host
        clickhouse_port: ClickHouse server port
        database_name: Target database name
    """
    logger = setup_logging()

    # Get credentials from environment variables
    username = os.getenv('CLICKHOUSE_USER')
    password = os.getenv('CLICKHOUSE_PASS')

    if not username or not password:
        logger.error("CLICKHOUSE_USER and CLICKHOUSE_PASS environment variables must be set")
        logger.error("Please create a .env file with these variables or set them in your environment")
        return

    logger.info(f"Connecting to ClickHouse at {clickhouse_host}:{clickhouse_port}")

    try:
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=username,
            password=password
        )

        logger.info(f"✓ Connected to ClickHouse successfully")

        # Create database if it doesn't exist
        logger.info(f"Creating database '{database_name}' if it doesn't exist")
        client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        logger.info(f"✓ Database '{database_name}' ready")

    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        return

    # Find all schema JSON files
    schema_path = Path(schema_folder)
    if not schema_path.exists():
        logger.error(f"Schema folder not found: {schema_folder}")
        return

    schema_files = list(schema_path.glob("*_schema.json"))

    if not schema_files:
        logger.warning(f"No schema files found in {schema_folder}")
        return

    logger.info(f"Found {len(schema_files)} schema file(s)")
    logger.info("=" * 80)

    # Process each schema file
    tables_created = 0
    tables_failed = 0

    for schema_file in schema_files:
        # Extract table name from filename (remove _schema.json suffix)
        table_name = schema_file.stem.replace('_schema', '')

        logger.info(f"Processing table: {table_name}")

        try:
            # Read schema JSON
            with open(schema_file, 'r') as f:
                schema_json = json.load(f)

            # Handle both old and new schema formats
            if isinstance(schema_json, dict) and 'fields' in schema_json:
                # New format with orderby
                schema_fields = schema_json['fields']
                order_by_fields = schema_json.get('orderby', [])
            else:
                # Old format (just a list of fields)
                schema_fields = schema_json
                order_by_fields = []

            logger.info(f"  Read schema: {len(schema_fields)} fields")

            if order_by_fields:
                logger.info(f"  ORDER BY: {', '.join(order_by_fields)}")

            # Generate CREATE TABLE SQL
            create_sql = generate_create_table_sql(table_name, schema_fields, order_by_fields, database_name)

            logger.info(f"  Generated CREATE TABLE statement:")
            # Log the SQL in a visually distinct way
            for line in create_sql.split('\n'):
                logger.info(f"    {Fore.CYAN}{line}{Style.RESET_ALL}")

            # Execute CREATE TABLE
            client.command(create_sql)
            logger.info(f"  ✓ Table '{table_name}' created successfully")
            tables_created += 1

        except Exception as e:
            logger.error(f"  ✗ Failed to create table '{table_name}': {e}")
            tables_failed += 1

        logger.info("-" * 80)

    # Summary
    logger.info("=" * 80)
    logger.info(f"Table creation complete!")
    logger.info(f"  ✓ Successfully created: {tables_created} table(s)")
    if tables_failed > 0:
        logger.warning(f"  ✗ Failed: {tables_failed} table(s)")
    logger.info(f"Logs saved to: tables.logs")

    # Close connection
    client.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Create ClickHouse tables from schema JSON files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python GenerateTables.py my_database schema/
  python GenerateTables.py my_database schema/ --host 192.168.1.100
  python GenerateTables.py my_database schema/ --host localhost --port 9000

Environment Variables:
  CLICKHOUSE_USER - ClickHouse username (required)
  CLICKHOUSE_PASS - ClickHouse password (required)

  These can be set in a .env file in the current directory.
        """
    )

    parser.add_argument('database_name', type=str, help='Name of the ClickHouse database')
    parser.add_argument('schema_folder', type=str, help='Path to folder containing schema JSON files')
    parser.add_argument('--host', type=str, default='localhost', help='ClickHouse host (default: localhost)')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse port (default: 8123)')

    args = parser.parse_args()

    create_tables_from_schemas(
        schema_folder=args.schema_folder,
        clickhouse_host=args.host,
        clickhouse_port=args.port,
        database_name=args.database_name
    )

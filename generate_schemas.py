import pandas as pd
import os
from pathlib import Path
from collections import defaultdict
import json
import logging
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

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

def setup_logging():
    """Set up dual logging to console and file"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # File handler without colors
    file_handler = logging.FileHandler('schema.logs', mode='w')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def infer_clickhouse_type(series, check_timezone=False):
    """
    Infer ClickHouse data type from pandas Series.
    Returns tuple of (type, has_timezone) if check_timezone is True, otherwise just type.
    """
    # Get non-null values for type inference
    non_null = series.dropna()

    if len(non_null) == 0:
        return ("Nullable(String)", False) if check_timezone else "Nullable(String)"

    dtype = series.dtype
    has_timezone = False

    # Numeric types
    if pd.api.types.is_integer_dtype(dtype):
        min_val = non_null.min()
        max_val = non_null.max()

        # Note - Since it is really hard to know all the different integer values that
        # the sensor data will have, and if it will be signed or unsigned we are giving
        # up space optimization for convinience here and just saying that all number types
        # are going to be Int32. The limitation offcourse if that if we have a number that
        # is greater than 2147483647 then we might have an issue. But in that case the
        # DB insert will fail and we can adapt it.
        base_type = "Int32"

        return (base_type, False) if check_timezone else base_type

    elif pd.api.types.is_float_dtype(dtype):
        return ("Float64", False) if check_timezone else "Float64"

    elif pd.api.types.is_bool_dtype(dtype):
        return ("UInt8", False) if check_timezone else "UInt8"

    elif pd.api.types.is_datetime64_any_dtype(dtype):
        # Check if timezone aware
        if hasattr(dtype, 'tz') and dtype.tz is not None:
            has_timezone = True
        return ("DateTime", has_timezone) if check_timezone else "DateTime"

    else:
        # String type - check if it could be a datetime with timezone
        try:
            parsed_dates = pd.to_datetime(non_null.head(100), errors='raise')
            # Check if the parsed dates have timezone info
            if hasattr(parsed_dates.dtype, 'tz') and parsed_dates.dtype.tz is not None:
                has_timezone = True
            else:
                # Check string format for timezone indicators (+, -, Z, UTC)
                sample_str = str(non_null.iloc[0])
                if any(tz_indicator in sample_str for tz_indicator in ['+', 'Z', 'UTC', '-']) and ':' in sample_str:
                    # More detailed check for timezone pattern
                    if 'T' in sample_str or ' ' in sample_str:
                        has_timezone = True

            return ("DateTime", has_timezone) if check_timezone else "DateTime"
        except:
            pass

        return ("String", False) if check_timezone else "String"

def analyze_csv_files(folder_path, schema_output_dir):
    """
    Read all CSV files in folder and generate schema information.
    """
    # Set up logging
    logger = setup_logging()

    folder_path = Path(folder_path)
    schema_dir = Path(schema_output_dir)

    # Group files by table name
    table_files = defaultdict(list)

    # Find all CSV files
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {folder_path}")
        return

    logger.info(f"Found {len(csv_files)} CSV files")

    # Parse filenames and group by table
    for csv_file in csv_files:
        filename = csv_file.stem
        parts = filename.split('_')

        if len(parts) >= 1:
            table_name = parts[0]
            table_files[table_name].append(csv_file)
        else:
            logger.warning(f"Skipping {csv_file.name}: doesn't match expected pattern")

    # Create schema directory
    schema_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Schema directory: {schema_dir}")

    # Process each table
    for table_name, files in table_files.items():
        logger.info(f"Processing table: {table_name} ({len(files)} file(s))")

        # Read all files for this table to get comprehensive schema
        all_columns = {}
        has_timezone_fields = False

        for csv_file in files:
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"  Read {csv_file.name}: {len(df)} rows, {len(df.columns)} columns")

                # Analyze each column
                for col in df.columns:
                    if col not in all_columns:
                        all_columns[col] = {
                            'example_values': [],
                            'has_nulls': df[col].isnull().any(),
                            'null_count': df[col].isnull().sum(),
                            'total_count': len(df[col]),
                            'series_list': [df[col]]
                        }
                    else:
                        # Update existing column info
                        all_columns[col]['has_nulls'] = all_columns[col]['has_nulls'] or df[col].isnull().any()
                        all_columns[col]['null_count'] += df[col].isnull().sum()
                        all_columns[col]['total_count'] += len(df[col])
                        all_columns[col]['series_list'].append(df[col])

                    # Collect example values (non-null)
                    non_null_values = df[col].dropna()
                    if len(non_null_values) > 0:
                        all_columns[col]['example_values'].extend(
                            non_null_values.head(3).astype(str).tolist()
                        )

            except Exception as e:
                logger.error(f"  ERROR reading {csv_file.name}: {e}")

        # Generate schema file
        schema_data = []
        datetime_field = None

        for col_name, col_info in all_columns.items():
            # Combine all series for this column to infer type
            combined_series = pd.concat(col_info['series_list'], ignore_index=True)
            base_type, has_tz = infer_clickhouse_type(combined_series, check_timezone=True)

            # Track if we found timezone-aware fields
            if has_tz:
                has_timezone_fields = True

            # Track the first DateTime field for ORDER BY
            if base_type == "DateTime" and datetime_field is None:
                datetime_field = col_name

            # Add Nullable wrapper if column has nulls
            if col_info['has_nulls']:
                if not base_type.startswith('Nullable'):
                    clickhouse_type = f"Nullable({base_type})"
                else:
                    clickhouse_type = base_type
            else:
                clickhouse_type = base_type

            # Get unique example values (limit to 3)
            example_values = list(set(col_info['example_values']))[:3]

            # Determine default value based on type
            default_value = None
            if 'String' in clickhouse_type:
                default_value = ''
            elif 'Int32' in clickhouse_type:
                default_value = -2147483648
            elif 'Float64' in clickhouse_type:
                default_value = '-inf'

            schema_data.append({
                'field_name': col_name,
                'example_values': example_values,
                'has_nulls': col_info['has_nulls'],
                'null_percentage': round(col_info['null_count'] / col_info['total_count'] * 100, 2),
                'suggested_clickhouse_type': clickhouse_type,
                'default': default_value
            })

        # Add pid field (always required)
        schema_data.append({
            'field_name': 'pid',
            'example_values': ['1', '2', '100'],
            'has_nulls': False,
            'null_percentage': 0.0,
            'suggested_clickhouse_type': 'UInt16',
            'default': None
        })
        logger.info(f"  ✓ Added 'pid' field (UInt16) for participant ID")

        # Add tzoffset field if we found timezone-aware datetime fields
        if has_timezone_fields:
            schema_data.append({
                'field_name': 'tzoffset',
                'example_values': ['0', '+60', '-300'],
                'has_nulls': False,
                'null_percentage': 0.0,
                'suggested_clickhouse_type': 'Int16',
                'default': None
            })
            logger.warning(f"  ⚠️  Table '{table_name}' contains timezone-aware datetime fields")
            logger.info(f"  ✓ Added 'tzoffset' field (Int16) to store timezone offset in minutes")

        # Determine ORDER BY fields
        order_by_fields = ['pid']
        if datetime_field:
            order_by_fields.append(datetime_field)
            logger.info(f"  ✓ ORDER BY fields: pid, {datetime_field}")
        else:
            logger.info(f"  ✓ ORDER BY fields: pid")

        # Write schema file with orderby metadata
        schema_output = {
            'orderby': order_by_fields,
            'fields': schema_data
        }

        schema_file = schema_dir / f"{table_name}_schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema_output, f, indent=2, default=str)

        logger.info(f"  ✓ Schema written to {schema_file.name}")

        # Also create a human-readable text version
        text_file = schema_dir / f"{table_name}_schema.txt"
        with open(text_file, 'w') as f:
            f.write(f"Schema for table: {table_name}\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"ORDER BY: {', '.join(order_by_fields)}\n")
            f.write("-" * 80 + "\n\n")

            for field in schema_data:
                f.write(f"Field: {field['field_name']}\n")
                f.write(f"  Type: {field['suggested_clickhouse_type']}\n")
                f.write(f"  Has Nulls: {field['has_nulls']} ({field['null_percentage']}%)\n")
                f.write(f"  Examples: {', '.join(map(str, field['example_values']))}\n")
                if field['default'] is not None:
                    f.write(f"  Default: {field['default']}\n")
                f.write("\n")

        logger.info(f"  ✓ Human-readable schema written to {text_file.name}")

    logger.info("\n" + "=" * 80)
    logger.info("Schema generation complete!")
    logger.info(f"Logs saved to: schema.logs")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Analyze CSV files and generate ClickHouse schema definitions')
    parser.add_argument('csv_folder', type=str, help='Path to folder containing CSV files')
    parser.add_argument('schema_output', type=str, help='Path to output folder for schema files')

    args = parser.parse_args()

    analyze_csv_files(args.csv_folder, args.schema_output)

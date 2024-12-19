import os
import argparse
import re
import logging
import pandas as pd
import psycopg2
from psycopg2 import sql
from dateutil.parser import parse as parse_datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_file(file_path):
    """Read data from a CSV or Excel file."""
    logger.info(f"Reading file: {file_path}")
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path, dtype=str)
    elif file_path.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file_path, dtype=str)
    else:
        logger.error("Unsupported file type. Use CSV or Excel.")
        raise ValueError("Unsupported file type. Use CSV or Excel.")


def infer_pg_type(value):
    """Infer PostgreSQL data type for a value."""
    if pd.isna(value) or (isinstance(value, str) and value.strip() == ''):
        return None

    value_str = str(value)
    
    if value_str.startswith('0') or (
        len(value_str) >= 8 and not re.match(r'^\d{4}-\d{2}-\d{2}', value_str)
    ):
        return 'TEXT'

    try:
        numeric_value = float(value_str.strip())
        if '.' in value_str or 'e' in value_str.lower():
            return 'FLOAT'
        elif numeric_value.is_integer():
            return 'INTEGER'
        return 'FLOAT'
    except (ValueError, AttributeError):
        pass

    try:
        parsed_date = parse_datetime(value_str, fuzzy=False)
        if 1900 <= parsed_date.year <= 2100:
            return 'TIMESTAMP'
    except (ValueError, TypeError):
        pass

    return 'TEXT'


def infer_column_type(series):
    """Infer PostgreSQL column type from a data series."""
    sample = series.dropna()
    sample_size = min(500, len(sample))
    sample = sample.sample(n=sample_size, random_state=42)

    inferred_types = {infer_pg_type(value) for value in sample if infer_pg_type(value) is not None}

    if not inferred_types:
        return 'TEXT'

    type_priority = ['TEXT', 'INTEGER', 'FLOAT', 'TIMESTAMP']
    for data_type in type_priority:
        if data_type in inferred_types:
            return data_type

    return 'TEXT'


def connect_db(dbname):
    """Connect to the specified PostgreSQL database."""
    logger.info(f"Connecting to the database '{dbname}'...")
    return psycopg2.connect(
        dbname=dbname,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD
    )


def ensure_database_and_schema(dbname, schema):
    """Ensure the database and schema exist, creating them if necessary."""
    try:
        logger.info(f"Ensuring database '{dbname}' exists...")
        conn = connect_db("postgres")
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the database exists using execute_query
        check_db_query = "SELECT 1 FROM pg_database WHERE datname = %s;"
        execute_query(cursor, check_db_query, (dbname,))

        if not cursor.fetchone():
            logger.info(f"Database '{dbname}' does not exist. Creating it...")
            create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
            execute_query(cursor, create_db_query)
            logger.info(f"Database '{dbname}' created successfully.")
        else:
            logger.info(f"Database '{dbname}' already exists.")

        cursor.close()
        conn.close()

        conn = connect_db(dbname)
        conn.autocommit = True
        ensure_schema_exists(conn, schema)
        return conn, conn.cursor()

    except Exception as e:
        logger.error(f"Error ensuring database and schema existence: {e}")
        raise


def ensure_schema_exists(conn, schema):
    """Ensure the schema exists in the connected database."""
    try:
        cursor = conn.cursor()

        # Check if the schema exists using execute_query
        check_schema_query = "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s;"
        execute_query(cursor, check_schema_query, (schema,))

        if not cursor.fetchone():
            logger.info(f"Schema '{schema}' does not exist. Creating it...")
            create_schema_query = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            execute_query(cursor, create_schema_query)
            logger.info(f"Schema '{schema}' created successfully.")
        else:
            logger.info(f"Schema '{schema}' already exists.")
        
        cursor.close()
    except Exception as e:
        logger.error(f"Error ensuring schema existence: {e}")
        raise


def ensure_table_exists(cursor, schema, table, column_types):
    """Ensure the table exists, creating it if necessary."""
    try:
        logger.info(f"Ensuring table '{schema}.{table}' exists...")

        # Check if the table exists using execute_query
        check_query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s;
        """
        execute_query(cursor, check_query, (schema, table))

        if not cursor.fetchone():
            logger.info(f"Table '{schema}.{table}' does not exist. Creating it...")

            # Construct the CREATE TABLE query
            columns_definitions = [f"idpk SERIAL PRIMARY KEY"] + [
                f"{col} {dtype}" for col, dtype in column_types.items()
            ]
            create_table_query = sql.SQL(
                "CREATE TABLE {}.{} ({})"
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ".join(columns_definitions))
            )

            # Execute the CREATE TABLE query
            execute_query(cursor, create_table_query)
            logger.info(f"Table '{schema}.{table}' created successfully.")
        else:
            logger.info(f"Table '{schema}.{table}' already exists.")
    except Exception as e:
        logger.error(f"Error ensuring table existence: {e}")
        raise


def execute_query(cursor, query, params=None):
    """Execute an SQL query with optional parameters."""
    query_str = cursor.mogrify(query, params).decode() if params else query
    if args.show_sql:
        logger.info(f"Executing SQL: {query_str}")
    cursor.execute(query, params)


def table_exists(cursor, schema, table_name):
    """Check if a table exists in the specified schema."""
    query = """
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s;
    """
    execute_query(cursor, query, (schema, table_name))
    return cursor.fetchone() is not None


def handle_delete_mode(cursor, schema, table_name, column_types, table_existed):
    """Handle the 'delete' mode: drop the table if it existed, then recreate it."""
    if table_existed:
        logger.info(f"Mode set to 'delete'. Dropping table {table_name}.")
        drop_query = sql.SQL(
            "DROP TABLE {}.{};"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        execute_query(cursor, drop_query)

    logger.info(f"Recreating table {table_name}.")
    ensure_table_exists(cursor, schema, table_name, column_types)


def handle_replace_mode(cursor, schema, table_name, column_types, table_existed):
    """Handle the 'replace' mode: delete all data if the table existed."""
    if table_existed:
        logger.info(f"Mode set to 'replace'. Deleting all data from table {table_name}.")
        delete_query = sql.SQL(
            "DELETE FROM {}.{};"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        execute_query(cursor, delete_query)
    else:
        logger.info(f"Table {table_name} did not exist. Creating it.")
        ensure_table_exists(cursor, schema, table_name, column_types)


def handle_update_mode(cursor, schema, table_name, column_types, table_existed):
    """Handle the 'update' mode: create the table if it does not exist."""
    if not table_existed:
        logger.info(f"Table {table_name} did not exist. Creating it.")
        ensure_table_exists(cursor, schema, table_name, column_types)


def insert_data(cursor, schema, table_name, data):
    """Insert data into the table."""
    insert_query = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({})"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table_name),
        sql.SQL(", ".join([col for col in data.columns])),
        sql.SQL(", ".join(["%s"] * len(data.columns)))
    )

    logger.info(f"Executing INSERTs in table {table_name}.")
    for row in data.itertuples(index=False):
        # Convert empty strings to None
        row_data = [value if value != '' else None for value in row]
        execute_query(cursor, insert_query, row_data)


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Create or update a PostgreSQL table from an Excel or CSV file."
)
parser.add_argument("-f", "--file", required=True, help="Path to the Excel or CSV file.")
parser.add_argument("-s", "--schema", default="public", help="Database schema (default: public).")
parser.add_argument("-d", "--database", default="python_scripts", help="Database name.")
parser.add_argument("-t", "--table", help="Table name (generated from file name if not provided).")
parser.add_argument("-m", "--mode", choices=['delete', 'replace', 'update'], default='update',
    help="Mode: 'delete' to drop the table, 'replace' to truncate data, 'update' to overwrite (default: update)."
)
parser.add_argument("--show-sql", action='store_true', help="Show the executed SQL queries.")
args = parser.parse_args()

# Constants for database connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_USER or not DB_PASSWORD:
    logger.error("Environment variables DB_USER and DB_PASSWORD must be set.")
    raise EnvironmentError("Environment variables DB_USER and DB_PASSWORD must be set.")

try:
    file_name = os.path.basename(args.file)
    default_table_name = re.sub(r"[^a-zA-Z0-9]+", "_", os.path.splitext(file_name)[0].lower())
    table_name = args.table or default_table_name

    logger.info(f"Using table name: {table_name}")

    data = read_file(args.file)
    data.fillna('', inplace=True)
    logger.info("File read successfully.")

    data.columns = [re.sub(r"[^a-zA-Z0-9]+", "_", col.lower()) for col in data.columns]
    logger.info(f"Sanitized column names: {list(data.columns)}")

    column_types = {col: infer_column_type(data[col]) for col in data.columns}
    logger.info(f"Inferred column types: {column_types}")

    # Ensure the database and schema exist
    conn, cursor = ensure_database_and_schema(args.database, args.schema)

    try:
        # Ensure the schema exists
        ensure_schema_exists(conn, args.schema)

        # Check if the table exists and store the result
        table_existed = table_exists(cursor, args.schema, table_name)

        # Handle the mode
        if args.mode == 'delete':
            handle_delete_mode(cursor, args.schema, table_name, column_types, table_existed)
        elif args.mode == 'replace':
            handle_replace_mode(cursor, args.schema, table_name, column_types, table_existed)
        elif args.mode == 'update':
            handle_update_mode(cursor, args.schema, table_name, column_types, table_existed)

        # Insert data into the table
        insert_data(cursor, args.schema, table_name, data)
        logger.info("Process completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Connection closed.")

except Exception as e:
    logger.error(f"An error occurred: {e}")
    raise

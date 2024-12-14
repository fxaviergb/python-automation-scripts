import os
import argparse
import re
import pandas as pd
import psycopg2
from psycopg2 import sql
from dateutil.parser import parse as parse_datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Helper function to infer PostgreSQL data types
def infer_pg_type(value):
    value_str = str(value) if not pd.isna(value) else ''
    if pd.isna(value) or value_str.strip() == '':
        return 'TEXT'

    if value_str.startswith('0') or (len(value_str) > 8 and not re.match(r'^\d{4}-\d{2}-\d{2}', value_str)):
        return 'TEXT'

    try:
        numeric_value = float(value_str.strip())
        if '.' in value_str or 'e' in value_str.lower():
            return 'FLOAT'
        elif numeric_value.is_integer():
            return 'INTEGER'
        else:
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
    sample = series.dropna().sample(n=10, random_state=42)
    inferred_types = [infer_pg_type(value) for value in sample]
    unique_types = set(inferred_types)
    if len(unique_types) == 1:
        return list(unique_types)[0]

    if 'INTEGER' in unique_types:
        return 'INTEGER'
    if 'FLOAT' in unique_types:
        return 'FLOAT'
    if 'TIMESTAMP' in unique_types:
        return 'TIMESTAMP'

    return 'TEXT'

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Create or update a PostgreSQL table from an Excel or CSV file.")
parser.add_argument("-f", "--file", required=True, help="Path to the Excel or CSV file.")
parser.add_argument("-s", "--schema", default="public", help="Database schema (default: public).")
parser.add_argument("-d", "--database", default="python_scripts", help="Database name (default: python_scripts).")
parser.add_argument("-t", "--table", help="Table name (generated from file name if not provided).")
parser.add_argument("-m", "--mode", choices=['replace', 'update'], default='update',
                    help="Mode of operation: 'replace' to delete all data and reload, 'update' to add or overwrite (default: update).")
parser.add_argument("--show-sql", action='store_true', help="Show the executed SQL queries.")
args = parser.parse_args()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_USER or not DB_PASSWORD:
    logger.error("Environment variables DB_USER and DB_PASSWORD must be set.")
    raise EnvironmentError("Environment variables DB_USER and DB_PASSWORD must be set.")

file_name = os.path.basename(args.file)
default_table_name = re.sub(r"[^a-zA-Z0-9]+", "_", os.path.splitext(file_name)[0].lower())
table_name = args.table or default_table_name

logger.info(f"Using table name: {table_name}")

def read_file(file_path):
    logger.info(f"Reading file: {file_path}")
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path, dtype=str)
    elif file_path.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file_path, dtype=str)
    else:
        logger.error("Unsupported file type. Use CSV or Excel.")
        raise ValueError("Unsupported file type. Use CSV or Excel.")

try:
    data = read_file(args.file)
    data.fillna('', inplace=True)
    logger.info("File read successfully.")
except Exception as e:
    logger.error(f"Error reading file: {e}")
    raise RuntimeError(f"Error reading file: {e}")

data.columns = [re.sub(r"[^a-zA-Z0-9]+", "_", col.lower()) for col in data.columns]
logger.info(f"Sanitized column names: {list(data.columns)}")

column_types = {col: infer_column_type(data[col]) for col in data.columns}
logger.info(f"Inferred column types: {column_types}")

for col, dtype in column_types.items():
    if dtype == 'TIMESTAMP':
        try:
            pd.to_datetime(data[col], errors='coerce')
        except Exception as e:
            logger.error(f"Error validating column '{col}' as TIMESTAMP: {e}")
            column_types[col] = 'TEXT'

def connect_db(dbname):
    logger.info(f"Connecting to the database '{dbname}'...")
    return psycopg2.connect(
        dbname=dbname, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
    )

def execute_query(cursor, query, params=None):
    query_str = cursor.mogrify(query, params).decode() if params else query
    if args.show_sql:
        logger.info(f"Executing SQL: {query_str}")
    cursor.execute(query, params)

try:
    conn = connect_db(args.database)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        execute_query(cursor, sql.SQL("CREATE SCHEMA IF NOT EXISTS {};" ).format(sql.Identifier(args.schema)))

        columns_definitions = [f"idpk SERIAL PRIMARY KEY"] + [f"{col} {dtype}" for col, dtype in column_types.items()]
        columns_sql = ", ".join(columns_definitions)
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {}.{} ({})"
        ).format(sql.Identifier(args.schema), sql.Identifier(table_name), sql.SQL(columns_sql))

        execute_query(cursor, create_table_query)

        if args.mode == 'replace':
            logger.info(f"Mode set to 'replace'. Deleting all data from table {table_name}.")
            delete_query = sql.SQL("DELETE FROM {}.{};").format(sql.Identifier(args.schema), sql.Identifier(table_name))
            execute_query(cursor, delete_query)

        insert_query = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES ({})"
        ).format(
            sql.Identifier(args.schema),
            sql.Identifier(table_name),
            sql.SQL(", ".join([col for col in data.columns])),
            sql.SQL(", ".join(["%s"] * len(data.columns)))
        )

        logger.info(f"Executing INSERTs in table {table_name}.")
        for row in data.itertuples(index=False):
            execute_query(cursor, insert_query, row)
        logger.info(f"Done!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Connection closed.")
        logger.info("Process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred: {e}")
    raise

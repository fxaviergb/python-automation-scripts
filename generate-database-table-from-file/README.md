# README

## Description
This script creates a table in a PostgreSQL database from the structure of a CSV or Excel file. It allows automatic data loading into the generated table, inferring column data types from the file and configuring the columns appropriately in PostgreSQL.

## Prerequisites
Before running the script, ensure you meet the following requirements:

1. Update your system and install the required dependencies by running:
   ```bash
   sudo apt update
   sudo apt install python3.12-dev
   sudo apt install libpq-dev
   ```
2. Install the necessary Python libraries:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up the required environment variables for PostgreSQL connection:
   ```bash
   export DB_USER="{your_user}"
   export DB_PASSWORD="{your_password}"
   export DB_HOST="{host}"                  # e.g., localhost
   export DB_PORT="{port}"                  # e.g., 5432
   ```

## Usage
The script accepts several command-line arguments to customize its execution. The steps to run it are described below:

### General Syntax
```bash
python3 generate-database-table-from-file.py -f <file_path> [-s <schema>] [-d <database>] [-t <table_name>] [-m <mode>] [--show-sql]
```

### Arguments

- `-f`, `--file`: **(Required)** Path to the CSV or Excel file containing the data.
- `-s`, `--schema`: Database schema where the table will be created. Default is `public`.
- `-d`, `--database`: Name of the database. Default is `python_scripts`.
- `-t`, `--table`: Name of the table to create. If not specified, the name is generated from the file name.
- `-m`, `--mode`: Operation mode:
  - `delete`: Deletes the entire table and recreates it with the updated structure.
  - `replace`: Removes all existing data in the table before loading new data.
  - `update` (default): Adds or updates data in the table.
- `--show-sql`: Displays the SQL queries executed during the process.

### Usage Examples

#### Create a table with a name derived from the file
```bash
python3 generate-database-table-from-file.py -f data.csv
```

#### Create a table in a specific schema
```bash
python3 generate-database-table-from-file.py -f data.xlsx -s analytics
```

#### Create a table with a custom name
```bash
python3 generate-database-table-from-file.py -f data.csv -t sales
```

#### Replace existing data in the table
```bash
python3 generate-database-table-from-file.py -f data.xlsx -m replace
```

#### Display the executed SQL queries
```bash
python3 generate-database-table-from-file.py -f data.csv --show-sql
```

## Outcome
1. Once the script is executed, a table will be created in the specified database.
2. The data from the file will be loaded into the table.
3. To verify the result, connect to the database and query the created table using a tool like `psql` or a graphical client like DBeaver.

```sql
SELECT * FROM <schema>.<table_name>;
```

## Logs
The script generates detailed logs during its execution. These logs include:
- Information about reading the file.
- Inferred column types.
- Executed SQL queries (if `--show-sql` is enabled).
- Success or error messages during the process.

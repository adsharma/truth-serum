# https://github.com/dr5hn/countries-states-cities-database
# imports world.sqlite3 to world.db (duckdb)

import re
import sqlite3

import duckdb
import inflect
import pandas as pd

p = inflect.engine()


def migrate_with_global_sequence(
    sqlite_path="your_sqlite_db.db", duck_path="your_duckdb.db"
):
    # Connect to databases
    sqlite_conn = sqlite3.connect(sqlite_path)
    duck_conn = duckdb.connect(duck_path)

    duck_conn.execute("CREATE SEQUENCE global_sequence")

    # Create a sequence to track global IDs
    current_id = 1

    # Tables to migrate
    tables = ["regions", "subregions", "countries", "states", "cities"]

    # Dictionary to store old_id -> new_id mappings for each table
    id_mappings = {table: {} for table in tables}

    # First pass: Read data and create mappings
    for table in tables:
        # Read data from SQLite using pandas
        df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)

        # Create mapping for this table
        for old_id in df["id"]:
            id_mappings[table][old_id] = current_id
            current_id += 1

    # Second pass: Create new tables and insert data with updated IDs
    for table in tables:
        # Read schema from SQLite
        cursor = sqlite_conn.cursor()
        cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
        )
        create_stmt = cursor.fetchone()[0]

        # Create table in DuckDB
        create_stmt = create_stmt.replace(
            "AUTOINCREMENT", "DEFAULT nextval('global_sequence')"
        )
        create_stmt = create_stmt.replace("MEDIUMINT", "INTEGER")
        print(create_stmt)
        duck_conn.execute(create_stmt)

        # Read data from SQLite using pandas
        df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)

        # Update IDs in the dataframe
        df["id"] = df["id"].map(id_mappings[table])

        # update timestamps. sqlite -> duckdb timestamp errors out
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()

        # Update foreign key references if they exist
        for col in df.columns:
            if col.endswith("_id"):
                ref_table = p.plural(col[:-3])
                if ref_table in tables:
                    df[col] = df[col].map(id_mappings[ref_table])

        # Insert data into DuckDB
        duck_conn.execute(
            f"""
            INSERT INTO {table}
            SELECT * FROM df
        """
        )

        print(f"Migrated {table} with {len(df)} rows")

    # Run the query to get index definitions
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index'")
    index_defs = cursor.fetchall()

    # Create indices in DuckDB
    for index_name, index_sql in index_defs:
        # Extract table name and column names from index SQL
        match = re.search(r"CREATE INDEX .* ON (.*) \((.*)\)", index_sql)
        table_name = match.group(1)

        # Extract column names from index SQL
        column_names = [
            col.strip() for col in index_sql.split("(")[1].split(")")[0].split(",")
        ]

        # Create index in DuckDB
        duck_conn.execute(
            f"CREATE INDEX {index_name} ON {table_name} ({', '.join(column_names)})"
        )

    # Commit changes and close connections
    duck_conn.commit()
    sqlite_conn.close()
    duck_conn.close()


if __name__ == "__main__":
    migrate_with_global_sequence("world.sqlite3", "world.db")

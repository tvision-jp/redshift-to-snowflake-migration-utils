import psycopg2
from pathlib import Path
import sys
import os

host = os.environ['REDSHIFT_HOST']
port = os.environ['REDSHIFT_PORT']
dbname = os.environ['REDSHIFT_DATABASE']
user = os.environ['REDSHIFT_USER']
password = os.environ['REDSHIFT_PASSWORD']


def main(args):
    """
    Export DDL for the specified tables from RedShift to the specified directory.

    args[1]: Input: A file list containing "schema"."table_name"
    args[2]: Output: Directory for outputting DDL files
    args[3]: Optional: Excluded table list file
    
    The output files will be named "schema"."table_name".sql for each table.
    tables listed in the excluded table list file will not have DDL generated.
    tables that could not be output will be listed in a "table.err" file in the output directory.
    """

    # Get the output directory from command-line arguments
    output_root = args[2]

    # Connect to the Redshift database
    error_list = []
    try:
        # Redshift connection string
        conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"

        conn = psycopg2.connect(conn_string)
        conn.autocommit = False
        cursor = conn.cursor()

        # Initialize the set of tables to ignore
        ignore_table = set()

        # If there are more than 4 arguments, read the ignore_table file
        if len(args) > 3:
            with open(args[3], 'r') as f:
                ignore_table = {line.rstrip('\n').lower() for line in f}

        # Read the table list file
        with open(args[1], 'r') as table_list_file:
            for table in table_list_file:
                table = table.rstrip('\n')

                # Process only tables that are not in the ignore_table set
                if table not in ignore_table:
                    print(f'{table=}')
                    try:
                        # Execute a "show table" query for the current table
                        cursor.execute(f"show table {dbname}.{table}")
                        result = cursor.fetchone()

                        # Write the table's DDL to a file
                        os.makedirs(Path(output_root), exist_ok=True)
                        with open(Path(output_root, f"{table}.sql"), 'w') as ddl_file:
                            sql = result[0]
                            ddl_file.write(f"create table {table} as\n")
                            ddl_file.write(sql)
                            ddl_file.write("\n")
                    except Exception as e:
                        # If an error occurs, log the error and continue with the next table
                        print(f"can not write {table} ddl! continue.", file=sys.stderr)
                        error_list.append(f"can not write {table} ddl! continue.\n")
                        print(e, file=sys.stderr)
                        conn.rollback()

    # Handle connection errors
    except Exception as e:
        print("can not open Redshift database.", file=sys.stderr)
        print(e, file=sys.stderr)
        error_list.append(e + "\n")

    # Write the list of errors to a file
    if error_list:
        with open(Path(output_root, "table.err"), 'w') as error_file:
            error_file.writelines(error_list)

# Run the main function if this script is executed
if __name__ == "__main__":
    main(sys.argv)

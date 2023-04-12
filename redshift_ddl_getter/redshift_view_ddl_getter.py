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
    Export DDL for the specified views from RedShift to the specified directory.

    args[1]: Input: A file list containing "schema"."view_name"
    args[2]: Output: Directory for outputting DDL files
    args[3]: Optional: Excluded view list file
    
    The output files will be named "schema"."view_name".sql for each view.
    Views listed in the excluded view list file will not have DDL generated.
    Views that could not be output will be listed in a "view.err" file in the output directory.
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

        # Initialize the set of views to ignore
        ignore_view = set()

        # If there are more than 4 arguments, read the ignore_view file
        if len(args) > 3:
            with open(args[3], 'r') as f:
                ignore_view = {line.rstrip('\n').lower() for line in f}

        # Read the view list file
        with open(args[1], 'r') as view_list_file:
            for view in view_list_file:
                view = view.rstrip('\n')

                # Process only views that are not in the ignore_view set
                if view not in ignore_view:
                    print(f'{view=}')
                    try:
                        # Execute a "show view" query for the current view
                        cursor.execute(f"show view {dbname}.{view}")
                        result = cursor.fetchone()

                        # Write the view's DDL to a file
                        os.makedirs(Path(output_root), exist_ok=True)
                        with open(Path(output_root, f"{view}.sql"), 'w') as ddl_file:
                            sql = result[0]
                            # Write a "create view" statement if it's not a materialized view
                            if 'MATERIALIZED' not in sql.upper():
                                ddl_file.write(f"create view {view} as\n")
                            ddl_file.write(sql)
                            ddl_file.write("\n")
                    except Exception as e:
                        # If an error occurs, log the error and continue with the next view
                        print(f"can not write {view} ddl! continue.", file=sys.stderr)
                        error_list.append(f"can not write {view} ddl! continue.\n")
                        print(e, file=sys.stderr)
                        conn.rollback()

    # Handle connection errors
    except Exception as e:
        print("can not open Redshift database.", file=sys.stderr)
        print(e, file=sys.stderr)
        error_list.append(e + "\n")

    # Write the list of errors to a file
    if error_list:
        with open(Path(output_root, "view.err"), 'w') as error_file:
            error_file.writelines(error_list)

# Run the main function if this script is executed
if __name__ == "__main__":
    main(sys.argv)

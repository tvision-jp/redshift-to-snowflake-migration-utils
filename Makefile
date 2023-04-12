get_redshift_ddls:
	docker compose run redshift2snowflake python redshift_ddl_getter/redshift_table_ddl_getter.py redshift_ddl_getter/table_list.txt redshift_ddl_getter/table redshift_ddl_getter/exclude_table_list.txt
	docker compose run redshift2snowflake python redshift_ddl_getter/redshift_view_ddl_getter.py redshift_ddl_getter/view_list.txt redshift_ddl_getter/view redshift_ddl_getter/exclude_view_list.txt

convert_sql:
	docker compose run redshift2snowflake python sql_converter/sql_converter.py

compare_tables_and_views:
	docker compose run redshift2snowflake python diff_checker/table_view_diff_checker.py --table_view_list_csv diff_checker/tables_views.csv

compare_sql_results:
	docker compose run redshift2snowflake python diff_checker/sql_diff_checker.py --sql_dir diff_checker/sql
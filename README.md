| English | [日本語](./i18n/README.ja.md) |

# Redshift to Snowflake migration tools

<!-- # Short Description -->
The following steps are required for migration from Redshift to Snowflake.

1. Set up .env file
2. Extract and file Redshift table/view DDL (using this tool)
3. Create table/view DDL for Snowflake (using this tool)
4. Create table and view in Snowflake using DDL
5. UNLOAD table data from Redshift to Amazon S3 (file format = Parquet)
6. Create storage integration in Snowflake
7. Create external stage in Snowflake
8. Create an external table for each table in Snowflake (using this tool)
9. Create INSERT statement for each table in Snowflake (using this tool)
10. Execute the INSERT statement in Snowflake
11. Compare table data between Redshift and Snowflake (using this tool)
12. Convert SQL (SELECT) for Redshift to Snowflake (using this tool) 
13. Comparison of SQL(SELECT) result data between Redshift and Snowflake (using this tool)

Redshift to Snowflake migration tools provides five tools to support the above tasks.
- Extract DDL for Redshift tables/views and output to file (step 2 above)
- Convert SQL for Redshift to Snowflake (step 3 and 12 above)
- Create SQL to INSERT into Snowflake tables from Parquet files in Amazon S3 (step 6 and 9 above)
- Compare Redshift and Snowflake table data and output the results to a file (step 11 above)
- Compare SQL (SELECT) result data between Redshift and Snowflake and output the result to file (step 13 above)


# Configure .env.
Execute the following command and write the connection environment settings for Redshift and Snowflake in `.env`.
```bash
cp .env.example .env
```

# Extract DDL for Redshift tables/views and output to file
Edit the following file under the `redshift_ddl_getter` directory.

| file name | description |
|-----------|------------|
|table_list.txt|List the table `<SCHEMA NAME>.<TALBLE NAME>` for which you want to acquire DDL for each line|
|exclude_table_list.txt|List the table `<SCHEMA NAME>.<TALBLE NAME>` that you want to exclude from the tables listed in `table_list.txt` for each line|
|view_list.txt|List the view `<SCHEMA NAME>.<VIEW NAME>` for which you want to acquire DDL for each line|
|exclude_view_list.txt|List the view `<SCHEMA NAME>.<VIEW NAME>` that you want to exclude from the views listed in `view_list.txt` for each line|


The following command will create a sql file for each DDL.
```bash
make get_redshift_ddls
```

The resulting sql files will be placed in the `table` and `view` directories under the `redshift_ddl_getter` directory.


# Convert SQL for Redshift to Snowflake
First, convert SQL files for Redshift (*.sql) to `./sql-conversion/redshift-sql` directory.

Next, run the following command.
```bash
docker compose build
make convert_sql
```
The SQL file converted for Snowflake will be located in `./sql-conversion/snowflake-sql`.


## Create SQL to INSERT from a Parquet file in Amazon S3 into a Snowflake table 
Before executing, you need to create a [storage-integration](https://docs.snowflake.com/ja/sql-reference/sql/create-storage-integration) and [external-stage](https://docs.snowflake.com/ja/ sql-reference/sql/create-external-table) must be created in advance.

Example of storage integration and external stage creation
```sql
-- create storage integration
CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/myrole'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket1/path1/', 's3://mybucket2/path2/');

-- create external stage
CREATE STAGE my_ext_stage URL='s3://mybucket1/path1/' STORAGE_INTEGRATION = s3_int;
```

Run the contents of the following file in Snowflake to create a procedure (public.MAKE_PARQUET_INSERT) to create INSERT statements for data migration.
[./snowflake-make-parquet-insert/procedure.public.MAKE_PARQUET_INSERT.sql](../snowflake_make_parquet_insert/procedure.public.MAKE_PARQUET_INSERT.sql)
The following SQL will create an external table creation DDL and an INSERT statement to the table.


```sql
call make_parquet_insert('<external stage path>', '<database name>', '<schema name>', '<table name>');
```

Example SQL to be created
```sql
-- creating an external table
CREATE EXTERNAL TABLE public.ext_sample_table 
  WITH LOCATION = @public.my_ext_stage/20230220/public/sample_table/
  FILE_FORMAT = (TYPE=PARQUET);

-- INSERT via foreign table
INSERT INTO public.sample_table 
SELECT
  $1:id::NUMBER(18,0),
  $1:first_name::TEXT(256),
  $1:family_name::TEXT(256),
  $1:address::TEXT(1024),
  $1:created_at::TIMESTAMP_NTZ,
  $1:updated_at::TIMESTAMP_NTZ 
FROM public.ext_sample_table;
```

# Compare Redshift and Snowflake table data and file out the results
## Create tables_views.csv
Edit after the second line of `diff_checker/tables_views.csv`.
- table_or_view: Enter `<schema name>. <table name | view name> `to be compared. 
- where(optional): Enter where statement for each table or view.

## Run comparison
```bash
make compare_tables_and_views
```

The output csv file will be placed under the `diff_checker/table_view_diff_result` directory.


# Compare data from Redshift and Snowflake SQL (SELECT) results and output the results to a file.
Compare the output results of the `redshift` directory under the `diff_checker/sql` directory and the sql file with the same name under the `snowflake` directory. For example, place `a.sql` for redshift in the `redshift` directory and `a.sql` for snowflake under the `snowflake` directory.

```bash
make compare_sql_results
```

The resulting csv files will be placed under the `diff_checker/sql_diff_result` directory.


# Contributors

- [yukidome25](https://github.com/yukidome25) 
- [ImYuya](https://github.com/ImYuya) 
- [naoki-matsumura-tvision](https://github.com/naoki-matsumura-tvision)
- [motoy3d](https://github.com/motoy3d)

# License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)




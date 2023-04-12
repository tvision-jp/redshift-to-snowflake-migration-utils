| [English](../README.md) | 日本語 |

# Redshift to Snowflake migration tools
<!-- # Short Description -->
RedshiftからSnowflakeへの移行において、下記の手順が必要です。

1. .envファイルの設定
2. Redshiftのテーブル・ビューDDLを抽出してファイル化（当ツール利用）
3. Snowflake用のテーブル・ビューDDLを作成（当ツール利用）
4. DDLを使ってSnowflakeにテーブル・ビューを作成
5. RedshiftからAmazon S3にテーブルデータをUNLOAD（ファイル形式=Parquet）
6. Snowflakeでストレージ統合を作成
7. Snowflakeで外部ステージを作成
8. Snowflakeでテーブルごとに外部テーブルを作成（当ツール利用）
9. SnowflakeでテーブルごとにINSERT文を作成（当ツール利用）
10. SnowflakeでINSERT文を実行
11. RedshiftとSnowflakeのテーブルデータ比較（当ツール利用） 
12. Redshift用SQL(SELECT)をSnowflake用に変換（当ツール利用） 
13. RedshiftとSnowflakeのSQL(SELECT)結果データ比較（当ツール利用）


Redshift to Snowflake migration toolsは、上記の作業をサポートする5つのツールを提供します。
- Redshiftのテーブル／ビューのDDLを抽出してファイル出力（上記手順2）
- Redshift用SQLをSnowflake用に変換（上記手順3,12）
- Amazon S3にあるParquetファイルからSnowflakeのテーブルにINSERTするSQLを作成（上記手順8,9）
- RedshiftとSnowflakeのテーブルデータを比較して結果をファイル出力（上記手順11）
- RedshiftとSnowflakeのSQL(SELECT)結果のデータを比較して結果をファイル出力（上記手順13）


# .envの設定
次のコマンドを実行して、`.env`にRedshiftとSnowflakeへの接続環境設定などを記載してください。
```bash
cp .env.example .env
```

# Redshiftのテーブル／ビューのDDLを抽出してファイル出力
`redshift_ddl_getter`ディレクトリ配下の以下のファイルを編集してください。

| file name | description |
|-----------|------------|
|table_list.txt|DDLを取得したいテーブル`<SCHEMA　NAME>.<TALBLE NAME>` を1行ごとに記載|
|exclude_table_list.txt|`table_list.txt`に記載したテーブルの内、除外したいテーブル `<SCHEMA　NAME>.<TALBLE NAME>` を1行ごとに記載|
|view_list.txt|DDLを取得したいビュー`<SCHEMA　NAME>.<VIEW NAME>` を1行ごとに記載|
|exclude_view_list.txt|`view_list.txt`に記載したビューの内、除外したいビュー`<SCHEMA　NAME>.<VIEW NAME>` を1行ごとに記載|


次のコマンドを実行するとDDLごとにsqlファイルが作成されます。
```bash
make get_redshift_ddls
```

出力結果のsqlファイルは`redshift_ddl_getter`ディレクトリ配下の`table`及び`view`ディレクトリ内に配置されます。


# Redshift用SQLをSnowflake用に変換
まず、Redshift用のSQLファイル(*.sql)を `./sql-conversion/redshift-sql` ディレクトリにコピーしてください。

次に以下のコマンドを実行してください。
```bash
docker compose build
make convert_sql
```
Snowflake用に変換されたSQLファイルが `./sql-conversion/snowflake-sql` に保存されます。


# Amazon S3にあるParquetファイルからSnowflakeのテーブルにINSERTするSQLを作成 
実行前に、[ストレージ統合](https://docs.snowflake.com/ja/sql-reference/sql/create-storage-integration)と[外部ステージ](https://docs.snowflake.com/ja/sql-reference/sql/create-external-table)を作成しておく必要があります。

ストレージ統合と外部ステージ作成例
```sql
-- ストレージ統合作成
CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/myrole'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket1/path1/', 's3://mybucket2/path2/');

-- 外部ステージ作成
CREATE STAGE my_ext_stage URL='s3://mybucket1/path1/' STORAGE_INTEGRATION = s3_int;
```

下記ファイルの中身をSnowflakeで実行して、データ移行用のINSERT文作成プロシージャ（public.MAKE_PARQUET_INSERT）を作成してください。


[./snowflake-make-parquet-insert/procedure.public.MAKE_PARQUET_INSERT.sql](../snowflake_make_parquet_insert/procedure.public.MAKE_PARQUET_INSERT.sql)


以下のSQLを実行すると、外部テーブル作成DDLとテーブルへのINSERT文が作成されます。


```sql
call make_parquet_insert('<外部ステージパス>', '<データベース名>', '<スキーマ名>', '<テーブル名>');
```

作成されるSQL例
```sql
-- 外部テーブル作成
CREATE EXTERNAL TABLE public.ext_sample_table 
  WITH LOCATION = @public.my_ext_stage/20230220/public/sample_table/
  FILE_FORMAT = (TYPE=PARQUET);

-- 外部テーブル経由でINSERT
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

# RedshiftとSnowflakeのテーブルとビューデータを比較して結果をファイル出力
## tables_views.csvの作成
`diff_checker/tables_views.csv`の2行目以降を編集してください.
- table_or_view: 比較したい`<schema name>.<table name | view name>`を記載ください. 
- where(optional): 各テーブル or ビューにおけるwhere文を記載ください.

## 比較実行

```bash
make compare_tables_and_views
```

出力結果のcsvファイルは`diff_checker/table_view_diff_result`ディレクトリ配下に配置されます。


# RedshiftとSnowflakeのSQL(SELECT)結果のデータを比較して結果をファイル出力

`diff_checker/sql`ディレクトリ配下の`redshift` ディレクトリと `snowflake`ディレクトリ配下にある同一名のsqlファイルの出力結果同士を比較します。例えば、redshift用の`a.sql`を`redshift` ディレクトリに、snowflake用の`a.sql`を`snowflake`ディレクトリ配下に設置してください。

```bash
make compare_sql_results
```

出力結果のcsvファイルは`diff_checker/sql_diff_result`ディレクトリ配下に配置されます。


# Contributors

- [yukidome25](https://github.com/yukidome25) 
- [ImYuya](https://github.com/ImYuya) 
- [naoki-matsumura-tvision](https://github.com/naoki-matsumura-tvision)
- [motoy3d](https://github.com/motoy3d)

# License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)




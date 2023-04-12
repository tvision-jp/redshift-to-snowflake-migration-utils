/*
外部テーブル作成DDLと、外部テーブルを使用したINSERT文を作成する（前提：ストレージ統合と外部ステージは別途作成）
ex) call make_parquet_insert('@public.migration_stage/parquet', 'dbname', 'schemaname', 'tablename');
*/

CREATE OR REPLACE PROCEDURE PUBLIC.MAKE_PARQUET_INSERT(
  EXTERNAL_STAGE_PATH VARCHAR, MYDB VARCHAR, MYSCHEMA VARCHAR, MYTABLE VARCHAR
  )
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
AS
$$
  var sql = "SELECT '$1:' || LOWER(COLUMN_NAME) || '::' || DATA_TYPE ||"
          +       " CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '('||CHARACTER_MAXIMUM_LENGTH||')'"
          +            " WHEN NUMERIC_PRECISION IS NOT NULL THEN '('||NUMERIC_PRECISION||','||NUMERIC_SCALE||')'"
          +            " ELSE '' END AS COL"
          +       " FROM INFORMATION_SCHEMA.COLUMNS"
          +      " WHERE TABLE_CATALOG = UPPER(:1)" //database name
          +        " AND TABLE_SCHEMA = UPPER(:2)"
          +        " AND TABLE_NAME = UPPER(:3)"
          +      " ORDER BY ORDINAL_POSITION";
  var stmt = snowflake.createStatement(
    {
      sqlText: sql,
      binds: [MYDB, MYSCHEMA, MYTABLE]
    }
  );
  var result = stmt.execute();
  var column_lines = "";
  var delim = " ";
  while (result.next()) {
    column_lines += delim + result.getColumnValue(1);
    delim = ",";
  }
  return "CREATE EXTERNAL TABLE " + MYDB + "." + MYSCHEMA + ".EXT_" + MYTABLE
       + " WITH LOCATION = " + EXTERNAL_STAGE_PATH + (EXTERNAL_STAGE_PATH.endsWith("/")? "" : "/") + MYSCHEMA + "/" + MYTABLE + "/"
       + " FILE_FORMAT = (TYPE=PARQUET);"
       + "INSERT INTO " + MYDB + "." + MYSCHEMA + "." + MYTABLE + " SELECT"
       + column_lines
       + " FROM " + MYDB + "." + MYSCHEMA + ".EXT_" + MYTABLE + ";"
$$;

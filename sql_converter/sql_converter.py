def usage():
    print("""\
# Usage: sql_converter.py [--inputdir {dir}] [--outputdir {dir}] [--no_comments]
    """)


import argparse
import os
import re
import sys
from pathlib import Path

from tqdm import tqdm

### General RegExes
comment_line_re = re.compile('^\s*--.*$', re.IGNORECASE)
whitespace_line_re = re.compile('^\s*$', re.IGNORECASE)
comma_line_re = re.compile('^\s*,\s*$', re.IGNORECASE)

# CHAR(n BYTE) => CHAR(n)
char_re = re.compile('(.*)(CHAR\((\d+)(\s+.+)\))(.*)', re.IGNORECASE)

# DEFAULT SYSDATE => deleted (OK only because data loaded from table should already have date)
# Snowflake DEFAULT must be literal
default_sysdate_re = re.compile('(.*)\ (DEFAULT SYSDATE)\ (.*)', re.IGNORECASE)

# SYSDATE => SYSDATE()
sysdate_ignore_re = re.compile('(.*)(SYSDATE)(.*)', re.IGNORECASE)

# SYSDATE => CURRENT_TIMESTAMP()
# sysdate_re = re.compile('(.*)\ (SYSDATE)\ (.*)', re.IGNORECASE)
sysdate_re = re.compile('(.*[,\(\s])(SYSDATE)([,\)\s].*)', re.IGNORECASE)

# find prior period, e.g. trunc(col,'MM')-1 => dateadd('MM', -1, trunc(col, 'MM'))
prior_period_re = re.compile('(.*)(TRUNC\(\s*(.+?),\s*(\'.+?\')\s*\)\s*(-?\s*\d+))(.*)', re.IGNORECASE)

# Empty Comma => ignore (dropping out clauses can leave an empty comma)
empty_comma_re = re.compile('(\s*)(,)\s+(--.*)', re.IGNORECASE)

# NVARCHAR => VARCHAR
nvarchar_re = re.compile('(.*)\ (NVARCHAR)(.*)', re.IGNORECASE)

# NVARCHAR => VARCHAR
nchar_re = re.compile('(.*)\ (NCHAR)(.*)', re.IGNORECASE)

# CREATE TABLE => CREATE OR REPLACE TABLE
createtable_re = re.compile('(.*)(CREATE\sTABLE\s)(.*)', re.IGNORECASE)

# DATE_ADD => DATEADD
date_add_re = re.compile('(.*)\ (DATE_ADD)(.*)', re.IGNORECASE)

# DATE_DIFF => DATEDIFF
date_diff_re = re.compile('(.*)\ (DATE_DIFF)(.*)', re.IGNORECASE)

# PGDATE_PART => DATE_PART
pgdate_part_re = re.compile('(.*)\ (PGDATE_PART)(.*)', re.IGNORECASE)

# PG_CATALOG => ''
pg_catalog_re = re.compile('(.*)\ (PG_CATALOG)(.*)', re.IGNORECASE)

# CEILING => CEIL
ceiling_re = re.compile('(.*)\ (CEILING)(.*)', re.IGNORECASE)

# NOW => CURRENT_TIMESTAMP()
now_re = re.compile('(.*)^(?!.*unknown.*$)^(?=.*now)(.*)', re.IGNORECASE)

# DELETE ...; => DELETE FROM ...;
delete_re = re.compile('(.*)(DELETE\s+)(.*)', re.IGNORECASE)

# min(col_name1) OVER(PARTITION BY col_name2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) => min(col_name1) OVER(PARTITION BY col_name2)
rows_unbounded_re = re.compile('(.*)(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\s*)(.*)', re.IGNORECASE)
previous_order_by_re = re.compile('(.*)(ORDER BY\s*)(.*)', re.IGNORECASE)

# alter => ignore
alter_re = re.compile('(.*)(ALTER\s+TABLE\s+.*)(.*)', re.IGNORECASE)

# owner => ignore
owner_re = re.compile('(.*)(OWNER\s+.*)(.*)', re.IGNORECASE)

# GRANT => ignore
grant_re = re.compile('(.*)(GRANT\s+.*)(.*)', re.IGNORECASE)

# DISTKEY(col) => ignore
# DISTKEY => ignore
distkey_re = re.compile('(.*\s+)(DISTKEY\s*(?:\(.*?\))?)(.*)', re.IGNORECASE)

# SORTKEY(col) => ignore
# SORTKEY => ignore
sortkey_re = re.compile('(.*\s+)(SORTKEY\s*(?:\(.*?\))?)(,?.*)', re.IGNORECASE)

# SORTKEY => ignore through end of statement
sortkey_multiline_re = re.compile('(^\s*)(SORTKEY\s*\(?\s*$)(.*)', re.IGNORECASE)

# ENCODE type => ignore
encode_re = re.compile('(.*)(\sENCODE\s+.+?)((?:,|\s+|$).*)', re.IGNORECASE)

# DISTSTYLE type => ignore
diststyle_re = re.compile('(.*)(\s*DISTSTYLE\s+.+?)((?:,|\s+|$).*)', re.IGNORECASE)

# SORTKEY type => ignore
sortkeystile_re = re.compile('(.*)(\s*SORTKEY\s+.+?)((?:,|\s+|$).*)', re.IGNORECASE)

# 'now'::character varying => current_timestamp
now_character_varying_re = re.compile('(.*)(\'now\'::(?:character varying|text))(.*)', re.IGNORECASE)

# bpchar => char
bpchar_re = re.compile('(.*)(bpchar)(.*)', re.IGNORECASE)

# character varying => varchar
character_varying_re = re.compile('(.*)(character varying)(.*)')

# interleaved => ignore
interleaved_re = re.compile('(.*)(interleaved)(.*)', re.IGNORECASE)

# identity(start, 0, ([0-9],[0-9])::text) => identity(start, 1)
identity_re = re.compile('(.*)\s*DEFAULT\s*"?identity"?\(([0-9]*),.*?(?:.*?::text)\)(.*)', re.IGNORECASE)

# trunc((CURRENT_TIMESTAMP)::timestamp) => date_trunc('DAY', CURRENT_TIMESTAMP)     # Redshift 'now' will have been resolved by now_character_varying_re to CURRENT_TIMESTAMP
trunc_re = re.compile('(.*)((?:trunc\(\()(.*)(?:\)::timestamp.*\)))(.*)', re.IGNORECASE)

### RegExes for common/standard types that Snowflake doesn't support
floatN_re = re.compile('(.*)\ (FLOAT\d+)(.*)', re.IGNORECASE)

# CREATE [type] INDEX => ignore through end of statement
index_re = re.compile('(.*)(CREATE(?:\s+(?:UNIQUE|BITMAP))?\ INDEX)(.*)', re.IGNORECASE)

# ALTER TABLE ... ADD PRIMARY KEY => ignore
pk_re = re.compile('(.*)(ALTER\s+TABLE\s+.*ADD\s+PRIMARY\s+KEY)(.*)', re.IGNORECASE)

# SET ... TO => ignore
set_re = re.compile('(.*)(SET\s+.*TO)(.*)', re.IGNORECASE)

statement_term_re = re.compile('(.*);(.*)', re.IGNORECASE)
clause_term_re = re.compile('(.*)\)(.*)', re.IGNORECASE)

### Regexes for Aurora dialect that Snowflake doesn't support
int_re = re.compile('(.*)\ (INT\s*\((?:.*?)\))(.*)', re.IGNORECASE)

# integer => numeric(18,0)
integer_re = re.compile('(.*)\ (INTEGER\s*)(.*)', re.IGNORECASE)

# bigint => numeric(18,0)
bigint_re = re.compile('(.*)\ (BIGINT\s*)(.*)', re.IGNORECASE)

# numeric(x,y>0) => double
numeric_re = re.compile('(NUMERIC\s*)(.*)', re.IGNORECASE)

# ARRARY[] => []
array_re = re.compile('(.*)\ (ARRAY\s*)(.*)', re.IGNORECASE)
array_parse_re = re.compile('(^.*)(\(.*[\[])(.*)', re.IGNORECASE)

# + => ||
string_plus_re = re.compile('(.*)(\+\s*)(.*)', re.IGNORECASE)
comment_plus_re = re.compile('(.*)( -- .*$)(.*)', re.IGNORECASE)
string_plus_quote_re = re.compile('(.*)((\'|\").*\+.*(\'|\"))(.*)', re.IGNORECASE)
string_plus_nvl_re = re.compile('(.*)(NVL\(.*\+.*\))(.*)', re.IGNORECASE)

# schema
schema_re = re.compile('(.*)(create\s+.*view\s+.(?!.*\.).*$)(.*)', re.IGNORECASE)
schema_temp_re = re.compile('(.*)(CREATE TEMP\s+.*$)(.*)', re.IGNORECASE)
# lowercase => uppercase
lowercase_shema_re = re.compile('(.*)(.*\s[a-zA-Z0-9_]*\s*\.\s*[a-zA-Z0-9_]*\s*)(.*)', re.IGNORECASE)
lowercase_sub_shema_re = re.compile('(.*)(.*\s[^0-9]*\s*\.\s*[a-zA-Z0-9_]*\s*)(.*)', re.IGNORECASE)
lowercase_single_schema_re = re.compile('(.*\{\{)(\s*(.+?))(\}\}.*)', re.IGNORECASE)

# JST => Japan
jst_re = re.compile('(.*)(\'JST\')(.*)', re.IGNORECASE)

# AVG() => TRUNC(AVG())
# avg_re = re.compile('(.*)\ (.*)(avg\s*)(.*)', re.IGNORECASE)
avg_re = re.compile('(.*)(avg\s*)(.*\))(.*)', re.IGNORECASE)
avg_trunc_re = re.compile('(.*\))(.*)(.*AS\s*)(.*)', re.IGNORECASE)
avg_exclusion_re = re.compile('(.*)\ (.*)(avg\(\s*)(.*)', re.IGNORECASE)
denial_ai_vi_re = re.compile('(.*)\ (.*)(f_(ai|vi)\s*)(.*)', re.IGNORECASE)

# f_list_distinct => f_distinct => f_list_distinct
f_list_distinct_re = re.compile('(.*)\ (.*)(f_list_distinct\s*)(.*)', re.IGNORECASE)

# LISTAGG(DISTINCT name, '/') WITHIN GROUP (ORDER BY id DESC) => f_list_distinct(LISTAGG(name, '/') WITHIN GROUP (ORDER BY id DESC),'/')
list_distinct_re = re.compile('(.*)\ (.*)(LISTAGG\(DISTINCT\s*)(.*)(\) WITHIN GROUP \(\s*)(.*)(\)\s*)(AS\s*)(.*)',
                              re.IGNORECASE)
listagg_re = re.compile('(.*)\ (.*)(listagg\(DISTINCT\s*)(.*)', re.IGNORECASE)
listagg_within_re = re.compile('(.*)\ (.*)(WITHIN\s*)(.*)', re.IGNORECASE)
listagg_group_re = re.compile('(.*)\ (.*)(GROUP\s*)(.*)', re.IGNORECASE)
listagg_as_re = re.compile('(.*)\ (.*)(AS\s*)(.*)', re.IGNORECASE)
listagg_name_re = re.compile('(.*)((?=AS)(.*))(.*)', re.IGNORECASE)
listagg_comma_re = re.compile('(.*)((\,)(.*))(.*)', re.IGNORECASE)

# new LISTAGG
new_listagg_distinct_re = re.compile('(.*)(LISTAGG)(\()(DISTINCT.)(.*)', re.IGNORECASE)
new_listagg_column_re = re.compile('(.*)(\,.*\))(.+WITHIN.*)', re.IGNORECASE)
new_listagg_as_re = re.compile('(.*\))( AS .*$)', re.IGNORECASE)

# redash column cast check
redash_column_cast_re = re.compile('(.*)(\::.*$)', re.IGNORECASE)
redash_not_column_cast_re = re.compile('(.*)( AS .*)', re.IGNORECASE)
# redash bit
redash_bit_re = re.compile('(.*)( << )(\()(.*)( -- .*)', re.IGNORECASE)
# redash calc check
redash_calc_re = re.compile('(.*)(ROW_NUMBER\(\))(.OVER.\()(\))(.*)', re.IGNORECASE)

# SIMILAR TO => REGEXP_LIKE
similar_to_re = re.compile('(.*)(\(.*)(SIMILAR TO)(.*)', re.IGNORECASE)

charset_re = re.compile('(.*)((?:DEFAULT)?(?:CHARACTER SET|CHARSET)\s*=?\s*utf8)(.*)', re.IGNORECASE)

auto_increment_re = re.compile('(.*)(auto_increment)(.*)', re.IGNORECASE)

decimal_re = re.compile('(.*)(decimal\(([0-9]*),([0-9]*)\))(.*)', re.IGNORECASE)

float_double_re = re.compile('(.*)((float|double)\([0-9]*,[0-9]*\))(.*)', re.IGNORECASE)

text_types_re = re.compile('(.*)((?:LONG|MEDIUM)TEXT)(.*)', re.IGNORECASE)

uncommented_set_re = re.compile('(.*)(^SET)(.*)', re.IGNORECASE)

unsigned_re = re.compile('(.*)(unsigned)(.*)', re.IGNORECASE)

default_zero_re = re.compile('(.*)(default\s*\'0\')(.*)', re.IGNORECASE)

default_zero_date_re = re.compile('(.*)(default\s*\'0000-00-00\')(?:\s+|$)(.*)', re.IGNORECASE)

default_zero_ts_re = re.compile('(.*)(default\s*\'0000-00-00 00:00:00(?:\.0*)?\')(?:\s+|$)(.*)', re.IGNORECASE)

binary_default_re = re.compile('(.*)(BINARY.*?)(DEFAULT.*)', re.IGNORECASE)


# COALESCE(col1) => COALESCE(col1,null)
coalesce_re = re.compile('(.*)(coalesce)(.*)', re.IGNORECASE)

# NVL(col1) => NVL(col1,null)
nvl_re = re.compile('(.*)(nvl)(.*)', re.IGNORECASE)
unknown_re = re.compile('(.*)(UNKNOWN)(.*)', re.IGNORECASE)

# JSON_EXTRACT_ARRAY_ELEMENT_TEXT => PARSE_JSON
json_extract_array_element_text_re = re.compile('(.*)(json_extract_array_element_text)(.*)', re.IGNORECASE)
parse_json_re = re.compile('(.*)(parse_json)(.*)', re.IGNORECASE)

json_re = re.compile('(.*)(JSON)(.*)', re.IGNORECASE)

# Convert source SQL to Snowflake SQL
def make_snow(sqlin, sqlout, no_comments):
    ### processing mode
    comment_lines = None
    term_re = None

    # filename
    filename = sqlin.name

    previous_line = None

    redash_overview = None
    redash_select = None
    redash_from = None
    underscore_as = None

    multi_vi_ai_list = []

    for line in sqlin:
        ### state variables
        pre = None
        clause = None
        post = None
        comment = None

        sql = line.rstrip()
        sql = sql.replace('[', '').replace(']', '')
        sql = sql.replace('`', '')

        # print >> sys.stdout, 'input: ' + sql

        # if current line is already fully commented, don't bother with any matching
        result = comment_line_re.match(sql)
        if result:
            write_line(sqlout, sql, comment)
            continue

        # if current line is already all whitespace, don't bother with any matching
        result = whitespace_line_re.match(sql)
        if result:
            write_line(sqlout, sql, comment)
            continue

        # if we're commenting out multiple lines, check if this is the last
        if comment_lines:
            result = term_re.match(sql)
            if result:
                comment_lines = None
                term_re = None
            comment = append_comment(comment, sql, no_comments)
            sql = None
            write_line(sqlout, sql, comment)
            continue

        # CHAR(n BYTE) => CHAR(n)
        result = char_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)  # char clause
            cnt = result.group(3)
            discard = result.group(4)
            post = result.group(5)
            sql = '{0}{1}({2}){3}'.format(pre, clause[0:4], cnt, post)
            comment = append_comment(comment, clause, no_comments)

        # DEFAULT SYSDATE => deleted (OK only because data loaded from table should already have date)
        result = default_sysdate_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # NVARCHAR => VARCHAR
        result = nvarchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} VARCHAR {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # NCHAR => CHAR
        result = nchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} CHAR {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DATE_ADD => DATEADD
        result = date_add_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)

            # added processing when there is more than one.
            multiple_date_add_line = date_add_re.match(pre)
            if multiple_date_add_line:
                multiple_pre = multiple_date_add_line.group(1)
                multiple_clause = multiple_date_add_line.group(2)
                multiple_post = multiple_date_add_line.group(3)

                _multiple_clause = multiple_clause.replace('_', '')
                if not multiple_clause.isspace():
                    _multiple_clause = f" {_multiple_clause}"
                pre = f"{multiple_pre}{_multiple_clause}{multiple_post}"

            _clause = clause.replace('_', '')
            if not clause.isspace():
                _clause = f" {_clause}"

            sql = f"{pre}{_clause}{post}"
            comment = append_comment(comment, clause, no_comments)

        # DATE_DIFF => DATEDIFF
        result = date_diff_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = f"datediff{clause}"
            comment = append_comment(comment, pre + clause, no_comments)

        # PGDATE_PART => DATE_PART
        result = pgdate_part_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = f"date_part{clause}"
            comment = append_comment(comment, pre + clause, no_comments)

        # PG_CATALOG => ''
        result = pg_catalog_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = clause.replace('.', '')
            comment = append_comment(comment, pre + clause, no_comments)

        # ceiling => ceil
        result = ceiling_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)

            sql = f"{pre}CEIL{post}"
            comment = append_comment(comment, clause, no_comments)

        # now => CURRENT_TIMESTAMP()
        result = now_re.match(sql)
        if result:
            clause = result.group(2)

            sql = re.sub(re.compile('now', re.IGNORECASE), 'CURRENT_TIMESTAMP()', clause)
            comment = append_comment(comment, clause, no_comments)

        # delete ...; => delete from ...;
        result = delete_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = f"{clause}from {post}"
            comment = append_comment(comment, pre + clause, no_comments)

        # min(survey_id) OVER(PARTITION BY panel_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        # => min(survey_id) OVER(PARTITION BY panel_id)
        result = rows_unbounded_re.match(sql)
        order_by_re = previous_order_by_re.match(sql)
        # not commented out if order by.
        previous_order = None
        if previous_line:
            previous_order = previous_order_by_re.match(previous_line)
        if result and previous_order is None and order_by_re is None:
            pre = result.group(1)
            clause = result.group(2)
            sql = f"{pre.strip()})"
            comment = append_comment(comment, pre + clause, no_comments)

        # alter => ignore
        result = alter_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = pre
            comment = append_comment(comment, clause, no_comments)

        # owner => ignore
        result = owner_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = pre
            comment = append_comment(comment, clause, no_comments)

        # grant => ignore
        result = grant_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            sql = pre
            comment = append_comment(comment, clause, no_comments)

        # FLOAT8 => FLOAT
        result = floatN_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} FLOAT {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # CREATE TABLE => CREATE OR REPLACE TABLE
        result = createtable_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}CREATE OR REPLACE TABLE {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DISTKEY(col) => ignore
        result = distkey_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SORTKEY => ignore through end of statement
        result = sortkey_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            str = '{1} {0}'.format(post, clause)
            comment = append_comment(comment, str, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # SORTKEY(col) => ignore
        result = sortkey_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # character set utf8 => ignore
        result = charset_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # auto_increment => autoincrement
        result = auto_increment_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}AUTOINCREMENT{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # unsigned => ignore
        result = unsigned_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # default '0' => default 0
        result = default_zero_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}DEFAULT 0{1}'.format(pre, post, clause)
            comment = append_comment(comment, clause, no_comments)

        # default '0000-00-00' => default '0000-00-00'::date
        result = default_zero_date_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}::DATE{2}'.format(pre, clause, post)
            comment = append_comment(comment, clause, no_comments)

        # default '0000-00-00 00:00:00' => default '0000-00-00 00:00:00'::timestamp
        result = default_zero_ts_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}::TIMESTAMP{2}'.format(pre, clause, post)
            comment = append_comment(comment, clause, no_comments)

        # binary default => binary ignore default
        result = binary_default_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # decimal(n>38,m) => decimal(38,m)
        result = decimal_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            precision = result.group(3)
            scale = result.group(4)
            post = result.group(5)
            if int(precision) > 38:
                precision = 38
                sql = '{0}DECIMAL({2},{3}){1}'.format(pre, post, precision, scale)
                comment = append_comment(comment, clause, no_comments)

        # float|double(n,m) => float|double
        result = float_double_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            coltype = result.group(3)
            post = result.group(4)
            sql = '{0}{1}{2}'.format(pre, coltype, post)
            comment = append_comment(comment, clause, no_comments)

        # longtext => string
        result = text_types_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} STRING {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SET ... = ; => ignore
        result = uncommented_set_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            str = '{1}{0}'.format(post, clause)
            comment = append_comment(comment, str, no_comments)

        # ENCODE type => ignore
        result = encode_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DISTSTYLE type => ignore
        result = diststyle_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SORTKEY type => ignore
        result = sortkeystile_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            comment = append_comment(comment, clause + post, no_comments)

        # 'now'::(character varying|text) => current_timestamp
        result = now_character_varying_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}CURRENT_TIMESTAMP{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # bpchar => char
        result = bpchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}char{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # character varying => varchar
        result = character_varying_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}varchar{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # interleaved => ignore
        result = interleaved_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # redshift identity syntax => identity
        result = identity_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} IDENTITY({1},1) {2}'.format(pre, clause, post)

        # redshift date trunc syntax => date_trunc
        result = trunc_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            timespec = result.group(3)
            post = result.group(4)
            sql = '{0}DATE_TRUNC(\'DAY\', {1}) {2}'.format(pre, timespec, post)
            comment = append_comment(comment, clause, no_comments)

        result = int_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} INTEGER {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # integer or bigint => numeric(18,0)
        result = integer_re.match(sql) or bigint_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)

            if len(post) > 1:
                post = " " + post

            sql = '{0} numeric(18,0){1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # numeric(x,y>0) => double
        result = numeric_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)

            _sql = clause.lstrip('(').rstrip(')').split(',')
            if int(_sql[1]) > 0:
                sql = 'double'
                comment = append_comment(comment, pre + clause, no_comments)

        # array[] => []
        result = array_re.match(sql)
        if result:
            _result = line.rstrip().replace('`', '')
            __result = array_parse_re.match(_result)

            _pre = __result.group(1)
            _clause = __result.group(2)
            _post = __result.group(3)

            sql = f"{_pre}{_clause.replace('array', '')}{_post}"
            comment = append_comment(comment, f"{_result}", no_comments)

        # + => ||
        result = string_plus_re.match(sql)
        if result:

            _single_double_quote = string_plus_quote_re.match(sql)
            _string_plus_nvl = string_plus_nvl_re.match(sql)

            if (_single_double_quote or _string_plus_nvl) is None:

                __separate_comment = comment_plus_re.match(sql)
                if __separate_comment:
                    __text = __separate_comment.group(1)
                    __comment = __separate_comment.group(2)

                    _search_text = string_plus_re.match(__text)

                    if _search_text:

                        def is_num(string_or_number) -> bool:
                            try:
                                float(string_or_number)
                            except ValueError:
                                return False
                            else:
                                return True

                        if not is_num(result.group(3).split()[0].replace(')', '').replace('(', "")):
                            _comment_plus = comment_plus_re.match(sql)
                            if _comment_plus is not None:
                                _comment = _comment_plus.group(2)

                                _result = string_plus_re.match(_comment_plus.group(1))
                                _pre = _result.group(1)
                                _clause = _result.group(2)
                                _post = _result.group(3)

                                sql = f"{_pre} || {_post}{_comment}"
                                comment = append_comment(comment, f"{_pre}{_clause}{_post}", no_comments)
                            else:
                                pre = result.group(1)
                                clause = result.group(2)
                                post = result.group(3)

                                sql = f"{pre} || {post}"
                                comment = append_comment(comment, f"{pre}{clause}{post}", no_comments)
                else:

                    def is_num(string_or_number) -> bool:
                        try:
                            float(string_or_number)
                        except ValueError:
                            return False
                        else:
                            return True

                    if not is_num(result.group(3).split()[0].replace(')', '').replace('(', "")):
                        _comment_plus = comment_plus_re.match(sql)
                        if _comment_plus is not None:
                            _comment = _comment_plus.group(2)

                            _result = string_plus_re.match(_comment_plus.group(1))
                            _pre = _result.group(1)
                            _clause = _result.group(2)
                            _post = _result.group(3)

                            sql = f"{_pre} || {_post}{_comment}"
                            comment = append_comment(comment, f"{_pre}{_clause}{_post}", no_comments)
                        else:
                            pre = result.group(1)
                            clause = result.group(2)
                            post = result.group(3)

                            sql = f"{pre} || {post}"
                            comment = append_comment(comment, f"{pre}{clause}{post}", no_comments)

        # schema
        result = schema_re.match(sql)
        # create temp was also covered, so it was excluded.
        temp_result = schema_temp_re.match(sql)
        if result and temp_result is None:
            __clause = result.group(2)
            _clause = __clause.split()
            pre = _clause[2]
            __filename = filename.split('.')
            _clause[2] = f"{__filename[0]}.{_clause[2]}"
            clause = " ".join(_clause)

            sql = '{0}'.format(clause)
            comment = append_comment(comment, pre, no_comments)

        # lower => upper
        if redash_overview is None:
            result = lowercase_shema_re.match(sql)
            check_result = lowercase_sub_shema_re.match(sql)
            check_order_by = previous_order_by_re.match(sql)
            if (result or check_result) and check_order_by is None:

                if result is None:
                    result = check_result

                pre = result.group(1)
                clause = result.group(2)
                post = result.group(3)

                __sub_pre_case = lowercase_sub_shema_re.match(pre)
                _pre = None
                if __sub_pre_case:
                    _sub_pre = __sub_pre_case.group(1)
                    _sub_clause = __sub_pre_case.group(2)
                    _sub_post = __sub_pre_case.group(3)
                    _pre = f"{_sub_pre.upper()}{_sub_clause.upper()}{_sub_post}"

                if _pre is not None:
                    sql = f"{_pre}{clause.upper()}{post}"
                else:
                    sql = f"{pre}{clause.upper()}{post}"

                if 1 <= (sql.count('{{') or sql.count('}}')):
                    single_sche = lowercase_single_schema_re.match(sql)

                    _single_sche_pre = single_sche.group(1)
                    if 2 == _single_sche_pre.count('{{') and 1 == _single_sche_pre.count('}}'):
                        double_sche = lowercase_single_schema_re.match(_single_sche_pre)

                        _double_sql_pre = double_sche.group(1)
                        _double_sql_clause = double_sche.group(3)
                        _double_sql_post = double_sche.group(4)

                        _single_sche_pre = f"{_double_sql_pre}{_double_sql_clause.lower()}{_double_sql_post}"

                    _single_sche_clause = single_sche.group(3)
                    _single_sche_post = single_sche.group(4)

                    sql = f"{_single_sche_pre}{_single_sche_clause.lower()}{_single_sche_post}"

                comment = append_comment(comment, pre + clause + post, no_comments)

        # redash cast
        if redash_overview:
            if redash_select:
                __column_cast = redash_column_cast_re.match(sql)
                not_column_cast = redash_not_column_cast_re.match(sql)

                if __column_cast and not_column_cast is None:

                    pre = __column_cast.group()
                    column_cast = __column_cast.group(1)
                    _column_cast = __column_cast.group(2)
                    original_column = column_cast

                    if '.' in column_cast:
                        column_cast = column_cast[column_cast.find('.') + 1:]

                    if ',' in _column_cast:
                        sql = f"{original_column}{_column_cast.replace(',', '')} AS {column_cast.lstrip()},"
                    else:
                        sql = f"{original_column}{_column_cast} AS {column_cast.lstrip()}"

                    comment = append_comment(comment, pre, no_comments)
                # if redash_from:
                #     redash_select = None
                #     redash_from = None

        # redash calc change
        if redash_overview:
            if redash_select:

                result = redash_calc_re.match(sql)
                if result:
                    pre = f"{result.group(3).lstrip()}{result.group(4)}"
                    calc = f"{result.group(3)}order by true{result.group(4)}"

                    sql = f"{result.group(1)}{result.group(2)}{calc}{result.group(5)}"
                    comment = append_comment(comment, pre, no_comments)
                # if redash_from:
                #     redash_select = None
                #     redash_from = None

        # redash bit
        if redash_overview:
            if redash_select:

                result = redash_bit_re.match(sql)
                if result:
                    sql = f"{result.group(1).replace(result.group(1).split()[0], '')}bitshiftleft{result.group(3)}{result.group(1).strip()}, {result.group(4)}"
                    comment = append_comment(comment,
                                             result.group(1) + result.group(2) + result.group(3) + result.group(4),
                                             no_comments)

        # similar to => regexp_like
        result = similar_to_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(4)

            posix_changed = post.replace('%', '.*').replace('(', '\(').replace(')', '\)')[:-2] + ')'

            sql = f"{pre}REGEXP_LIKE{clause.rstrip()},{posix_changed}"
            comment = append_comment(comment, result.group(3) + ' %', no_comments)

        # jst => japan
        result = jst_re.match(sql)
        if result:
            sql = result.group().replace('JST', 'Japan')
            comment = append_comment(comment, result.group(2).replace('JST', 'Japan'), no_comments)

        # AVG() => TRUNC(AVG())
        result = avg_re.match(sql)
        # avg pattern is excluded.
        avg_exclusion = avg_exclusion_re.match(sql)
        ai_vi_denial = denial_ai_vi_re.match(sql)
        if result and avg_exclusion is not None and ai_vi_denial is None:
            sql = f"{result.group(1)}TRUNC({result.group(2)}{result.group(3)}){result.group(4)}"
            comment = append_comment(comment, result.group(2), no_comments)

            # pre = result.group(1)
            # clause = result.group(2)
            # post = result.group(3)
            #
            # articles = result.group(4)
            # article_rt = avg_trunc_re.match(articles)
            #
            # if article_rt:
            #     sql = f"{pre} TRUNC({post}{article_rt.group(1)}) {article_rt.group(3)} {article_rt.group(4)}"
            # else:
            #     if not clause.isspace():
            #         sql = f"{pre} {clause}TRUNC({post}{articles})"
            #     else:
            #         # sql = f"{pre}TRUNC({clause}{post})"
            #         sql = f"{pre} TRUNC({post}{articles})"
            # comment = append_comment(comment, pre + clause + post, no_comments)

        # listagg(DISTINCT name, '/') WITHIN GROUP (ORDER BY id DESC)
        # => list_distinct(listagg(name, '/') WITHIN GROUP (ORDER BY id DESC),'/')

        result = new_listagg_distinct_re.match(sql)
        if result:

            __column = new_listagg_column_re.match(result.group(5))
            __as = new_listagg_as_re.match(result.group(5))

            if __column is not None:

                if __as is None:
                    _column = __column.group(2)
                    letter_body = result.group(5)
                else:
                    __column = new_listagg_column_re.match(__as.group(1))
                    _column = f"{__column.group(2)}{__as.group(2)}"
                    letter_body = __as.group(1)

                sql = f"{result.group(1)}f_list_distinct({result.group(2)}{result.group(3)}{letter_body}{_column}"
                comment = append_comment(comment, result.group(), no_comments)
            else:
                comment = append_comment(comment, "warning: no within group order by, write it down.", no_comments)

        # INDEX CREATION => ignore through end of statement
        result = index_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            str = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, str, no_comments)
            comment_lines = 1
            term_re = statement_term_re
            write_line(sqlout, sql, comment)
            continue

        # ALTER TABLE ... ADD PRIMARY KEY => ignore
        result = pk_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            str = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, str, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # SET ... TO => ignore
        result = set_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            str = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, str, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # coalesce(col1) => coalesce(col1,null)
        result = coalesce_re.match(sql)
        if result:
            pre = result.group(2)
            clause = result.group(3)

            para = clause.lstrip('(').rstrip(')').split(',')
            if len(para) == 1:
                sql = f"{pre}({para[0]},null)"
                comment = append_comment(comment, pre + clause, no_comments)

        # nvl(col1) => nvl(col1,null)
        result = nvl_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)

            unmatched = unknown_re.match(post)
            not_order_by = previous_order_by_re.match(sql)
            if unmatched is None and not_order_by is None:
                if post != '(':
                    para = post.lstrip('(').rstrip(')').split(',')
                    if len(para) == 1:
                        sql = f"{pre}{clause}({para[0]}, null)"
                        comment = append_comment(comment, clause + post, no_comments)

        # json_extract_array_element_text => parse_json
        result = json_extract_array_element_text_re.match(sql)
        if result:
            clause = result.group(2)
            post = result.group(3)

            _original = post
            _post = post.replace('(', '').replace(')', '').split(',')
            _zero_post = _post[0].replace("'", '')
            _two_post = _post[2].replace("'", '')
            sql = f"parse_json('[{_zero_post},{_post[1]},{_two_post}]')[{_post[3].replace(' ', '')}]"
            comment = append_comment(comment, clause + _original, no_comments)

        # json => variant
        check_result = parse_json_re.match(sql)
        result = json_re.match(sql)
        if not check_result and result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}VARIANT{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # Empty Comma => ignore
        result = empty_comma_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # redash query
        if redash_overview:
            if (redash_select and redash_from) is not None:
                redash_select = None
                redash_from = None

        ## DML transformations that might appear multiple times per line
        dml_repeat = True
        while dml_repeat:
            dml_repeat = False

            # determine prior period
            # e.g. trunc(sysdate,'MM')-1
            result = prior_period_re.match(sql)
            if result:
                pre = result.group(1)
                clause = result.group(2)
                col = result.group(3)
                units = result.group(4)
                offset = result.group(5)
                post = result.group(6)
                sql = '{0}dateadd({4}, {5}, trunc({3}, {4}))'.format(pre, post, clause, col, units, offset)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

            # sysdate => sysdate()
            result = sysdate_ignore_re.match(sql)
            if result:
                clause = result.group(2)
                sql = f"{clause}()"
                comment = append_comment(comment, clause, no_comments)

            # SYSDATE => CURRENT_TIMESTAMP()
            result = sysdate_re.match(sql)
            if result:
                pre = result.group(1)
                clause = result.group(2)
                post = result.group(3)
                sql = '{0} CURRENT_TIMESTAMP() {1}'.format(pre, post, clause)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

            previous_line = sql

        # write out possibly modified line
        result = whitespace_line_re.match(sql)
        if result:
            sql = None  # the mods have reduced this line to empty whitespace
        else:
            result = comma_line_re.match(sql)
            if result:
                sql = None  # the mods have reduced this line to a single vestigial comma
        write_line(sqlout, sql, comment)
        continue


def append_comment(old_comment, new_comment, no_comments):
    if no_comments:
        return None
    if old_comment and new_comment:
        return '{0} // {1}'.format(old_comment, new_comment)
    if not old_comment:
        return new_comment
    return old_comment


def write_line(sqlout, sql, comment):
    if sql is not None:
        sqlout.write(sql)
    if comment:
        # owner and grant and create temp => ignore
        if not (owner_re.match(comment) or grant_re.match(comment) or schema_temp_re.match(comment)):
            if comment.rstrip()[-1] == ';':
                sqlout.write(';')
        sqlout.write('\t\t--// {0}'.format(comment))
    if sql is not None or comment:
        sqlout.write('\n')
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert SQL dialects to Snowflake.')
    parser.add_argument('--no_comments', action='store_true',
                        help='suppress comments with changes (default: show changes)')
    parser.add_argument('--inputdir', action='store', default="sql_converter/redshift-sql",
                        help='input SQL file directory in Redshift dialect (default: "redshift-sql")')
    parser.add_argument('--outputdir', action='store', default="sql_converter/snowflake-sql",
                        help='output SQL file directory in Snowflake dialect (default: "snowflake-sql")')
    args = parser.parse_args()
    print(f'=============={args.inputdir=}')
    redshift_sql_dir_path: Path = Path(args.inputdir)
    print(f'=============={args.outputdir=}')
    snowflake_sql_dir_path: Path = Path(args.outputdir)
    src_sql_files = list(redshift_sql_dir_path.glob("**/*.sql"))
    if not src_sql_files:
        print("[WARN] No files found.")
        exit()
    print(f"{len(src_sql_files)} files found.")

    for src_sql_path in tqdm(src_sql_files, desc="Converting sql to snowflake style"):
        snowflake_sql_subdir = snowflake_sql_dir_path / Path(src_sql_path.parts[-2])
        if snowflake_sql_subdir.name != redshift_sql_dir_path.name:
          os.makedirs(snowflake_sql_subdir, exist_ok=True)

        # sjis(cp932) or utf-8
        try:
            with open(src_sql_path) as f:
                ENCODING = 'utf-8'
        except UnicodeDecodeError as e:
            ENCODING = 'sjis'

        no_comments: bool = args.no_comments
        with open(src_sql_path, encoding=ENCODING) as src_sql_file, \
            open(str(snowflake_sql_dir_path / src_sql_path.name), mode='w') as dest_sql_file:
            make_snow(src_sql_file, dest_sql_file, no_comments)
        print(f"done converting {src_sql_path=}", file=sys.stderr)

import argparse
import datetime
import os
import time
import warnings

import numpy as np
import pandas as pd
import pytz
from diff_checker_base import (RedshiftConnector, SnowflakeConnector,
                               exec_query_redshift, exec_query_snowflake,
                               setup_logger)
from dotenv import load_dotenv
from pandas.testing import assert_frame_equal
from tqdm import tqdm

load_dotenv()
warnings.simplefilter('ignore')

parser = argparse.ArgumentParser(description='Compare redshift and snowflake table or view.')
parser.add_argument('--table_view_list_csv', nargs='*',
                    help='table or view list csv file name', type=str, required=True)

args = parser.parse_args()
now = datetime.datetime.now(pytz.timezone(os.getenv('TIME_ZONE')))
logger = setup_logger(__name__, f'{os.path.dirname(__file__)}/logs/table_view_diff_checker_{now.strftime("%Y%m%d_%H%M")}.log')
logger.info('----------------------------------------------------')
logger.info('Start compare redshift and snowflake table or view. ')
logger.info('----------------------------------------------------')

exclude_columns = os.getenv('TABLE_VIEW_DIFF_CHECKER_EXCLUDED_COLUMNS').split(',')
err_rate_threshold = float(os.getenv('DIFF_CHECKER_ERROR_RATE_THRESHOLD', '0.0001'))

snowflake_conn = SnowflakeConnector()
redshift_conn = RedshiftConnector()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def compare_result(table_view: dict, sql_redshift: str, sql_snowflake: str, result: dict):
    logger.debug(f'compare_result() {table_view=}, {sql_redshift=} {sql_snowflake=} {result=}')
    assert_flg = 0
    df_redshift, df_snowflake = None, None
    time_redshift, time_snowflake = None, None
    try:
        logger.debug(f'Execute sql on Snowflake\n{sql_snowflake}')
        df_snowflake, time_snowflake = exec_query_snowflake(snowflake_conn, sql_snowflake)
        logger.debug(f'Execute sql on Redshift\n{sql_redshift}')
        df_redshift, time_redshift = exec_query_redshift(redshift_conn, sql_redshift)

        if len(df_redshift) == 0 or len(df_snowflake) == 0:
            logger.warn(f'{len(df_redshift)=} {len(df_snowflake)=}')
            raise Exception(f'No data. {df_redshift=}, {df_snowflake=}')

        sort_columns: list = df_redshift.columns.tolist()  # use columns of Redshift table
        df_snowflake = df_snowflake.sort_values(sort_columns).reset_index(drop=True)[sort_columns]
        df_redshift = df_redshift.sort_values(sort_columns).reset_index(drop=True)[sort_columns]
        assert_flg = 1
        # Confirm that the number of digits (including integer part and decimal part) is the same within 13 digits
        # (atol is absolute error, rtol is relative error)
        assert_frame_equal(df_redshift, df_snowflake, check_dtype=False, atol=1e-13, rtol=1e-13)

        # For table comparison, select count(*) and select sum(column_name) if numeric type for each column,
        # otherwise select min(column name), max(column name), count(distinct column name) comparison
        redshift_row_count: int = df_redshift.iat[0, 0]
        snowflake_row_count: int = df_snowflake.iat[0, 0]
        if redshift_row_count == 0 or snowflake_row_count == 0:
            logger.warn(f'Redshift row count: {redshift_row_count}')
            logger.warn(f'Snowflake row count: {snowflake_row_count}')
            raise Exception(f'No data. {redshift_row_count=}, {snowflake_row_count=}')
        pd.set_option('display.max_columns', 100)
        pd.set_option('display.max_rows', 1000)
        pd.set_option('display.width', 200)

        result.update(
            {
                'query_redshift': sql_redshift,
                'query_snowflake': sql_redshift,
                'result_redshift': f'{df_redshift.set_index("count_all").T}',
                'result_snowflake': f'{df_snowflake.set_index("count_all").T}',
                'time_redshift': round(time_redshift, 2),
                'time_snowflake': round(time_snowflake, 2),
                'is_data_equal': True,
                'is_error': False,
                'diff_rate': '-',
                f'result(<= {err_rate_threshold}%)': 'OK'
            }
        )
    except Exception as ex:
        result['is_data_equal'] = False
        result['message'] = ex
        if assert_flg:  # Error after assert_frame_equal
            if df_redshift.iat[0, 0] != 0:  # Redshift row count is not 0
                err_rate_max, col_max = 0.0, ''
                for i, col in enumerate(list(df_redshift.columns)):
                    sf_val, rs_val = df_snowflake.iat[0, i], df_redshift.iat[0, i]
                    # Get the error rate for numeric columns
                    if (type(sf_val) and type(rs_val)) in [np.int64, np.float64]:
                        err_rate: float = abs(sf_val - rs_val) / rs_val * 100
                        err_rate_max, col_max = (err_rate, col) if err_rate_max < err_rate else (err_rate_max, col_max)

                err_rate_str: str = f'{"{:.12f}".format(err_rate_max)}%.' if err_rate_max != 0 else '0%.'
                judge, err_rate_print = \
                    ('OK', f'{err_rate_str} {col_max}') if err_rate_max <= err_rate_threshold \
                        else ('NG', f'{err_rate_str} {col_max}')
            else:  # Row count 0 error
                judge, err_rate_print = 'NG', '-'

            result.update(
                {
                    'query_redshift': sql_redshift,
                    'query_snowflake': sql_snowflake,
                    'result_redshift': f'{df_redshift.set_index("count_all").T}',
                    'result_snowflake': f'{df_snowflake.set_index("count_all").T}',
                    'time_redshift': round(time_redshift, 2) if time_redshift is not None else '',
                    'time_snowflake': round(time_snowflake, 2) if time_snowflake is not None else '',
                    'is_data_equal': False,
                    'is_error': False,
                    'diff_rate': err_rate_print,
                    f'result(<= {err_rate_threshold}%)': judge
                }
            )
        else:  # etc error
            result.update(
                {
                    'query_redshift': sql_redshift,
                    'query_snowflake': sql_snowflake,
                    'is_data_equal': False,
                    'is_error': True,
                    'diff_rate': '-',
                    f'result(<= {err_rate_threshold}%)': 'NG'
                }
            )
        logger.exception(ex)

    return result


def get_table_default_result(table_view_name: str) ->  dict:
    default_result = {
        'table/view': table_view_name,
        'query_redshift': None,
        'query_snowflake': None,
        'result_redshift': '-',
        'result_snowflake': '-',
        'time_redshift': '-',
        'time_snowflake': '-',
        'is_data_equal': '-',
        'is_error': True,
        'message': '',
        'diff_rate': '-',
        f'result(<= {err_rate_threshold}%)': 'NG',
    }
    return default_result


def compare_table_results(table_view: dict) -> dict:
    logger.info(f'{table_view=}')
    table_view_name: str = table_view['name']
    where: str = table_view['where']

    result: dict = get_table_default_result(table_view_name)
    numeric_columns = ['SMALLINT', 'INT2', 'INTEGER', 'INT', 'INT4', 'BIGINT', 'INT8',
                       'DECIMAL', 'NUMERIC', 'REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'FLOAT']
    int_columns = ['SMALLINT', 'INT2', 'INTEGER', 'INT', 'INT4', 'BIGINT', 'INT8', 'DECIMAL', 'NUMERIC']

    # get column names and types from redshift.
    sql_redshift = f"SELECT column_name, data_type FROM information_schema.columns " \
                   f"WHERE table_schema || '.' || table_name = '{table_view_name}'"
    df_redshift, time_redshift = exec_query_redshift(redshift_conn, sql_redshift)
    query = 'SELECT COUNT(*) AS count_all,'
    for index, row in df_redshift.iterrows():
        column_type: str = row.iloc[1].upper()
        c = row.iloc[0]
        if c in exclude_columns:
            logger.info(f'Column {c} excluded.')
        elif column_type in numeric_columns:
            if column_type in int_columns:  # int : sum, min, max, avg
                query = f'{query} SUM({c}) AS sum_{c}, MIN({c}) AS min_{c}, MAX({c}) AS max_{c},' \
                        f' TRUNC(AVG({c})) AS avg_{c},'
            else:  # decimal : sum, min, max, avg
                query = f'{query} SUM({c}) AS sum_{c}, MIN({c}) AS min_{c}, MAX({c}) AS max_{c}, AVG({c}) AS avg_{c},'
        elif column_type == 'BOOLEAN':  # boolean : count, count_distinct
            query = f'{query} SUM(case when {c} then 1 else 0 end) AS count_{c}, COUNT(DISTINCT {c}) AS count_distinct_{c},'
        elif 'CHAR' in column_type or 'STRING' in column_type or 'TEXT' in column_type:
            # character : min, max, count_distinct
            query = f'{query} MIN({c}) AS min_{c}, MAX({c}) AS max_{c}, COUNT(DISTINCT TRIM({c})) AS count_distinct_{c},'
        else:  # etc(ex: date) : min, max,count_distinct
            query = f'{query} MIN({c}) AS min_{c}, MAX({c}) AS max_{c}, COUNT(DISTINCT {c}) AS count_distinct_{c},'
    query = f'{query[:-1]} FROM {table_view_name}'
    if where:
        query = f'{query} WHERE {where}'

    return compare_result(table_view, query, query, result)

def format_excel(diff_results_df: pd.DataFrame, excel_writer: pd.ExcelWriter):
    sheet = excel_writer.sheets['Diff results']
    columns = diff_results_df.columns
    # column width
    col_idx = columns.get_loc('table/view')
    sheet.set_column(col_idx, col_idx, 30)
    col_idx = columns.get_loc('query_redshift')
    sheet.set_column(col_idx, col_idx, 15)
    col_idx = columns.get_loc('query_snowflake')
    sheet.set_column(col_idx, col_idx, 15)
    col_idx = columns.get_loc('result_redshift')
    sheet.set_column(col_idx, col_idx, 35)
    col_idx = columns.get_loc('result_snowflake')
    sheet.set_column(col_idx, col_idx, 35)
    col_idx = columns.get_loc('time_redshift')
    sheet.set_column(col_idx, col_idx, 12)
    col_idx = columns.get_loc('time_snowflake')
    sheet.set_column(col_idx, col_idx, 12)
    col_idx = columns.get_loc('is_data_equal')
    sheet.set_column(col_idx, col_idx, 11)
    col_idx = columns.get_loc('message')
    sheet.set_column(col_idx, col_idx, 16)
    col_idx = columns.get_loc('diff_rate')
    sheet.set_column(col_idx, col_idx, 22)
    col_idx = columns.get_loc(f'result(<= {err_rate_threshold}%)')
    sheet.set_column(col_idx, col_idx, 17)


if __name__ == '__main__':
    start = time.time()
    diff_results: list[dict] = []
    table_views: list[dict] = []
    logger.info(f'Compare tables data. {args.table_view_list_csv=}')
    for csv in args.table_view_list_csv:
        with open(csv) as f:
            lines = f.read()
            table_view_list = [{'name': line.split(',')[0], 'where': line.split(',')[1]}
                               for line in lines.split('\n')[1:] if line and not line.startswith('#')]
        table_views.extend(table_view_list)

    logger.info(f'{table_views=}')
    # compare data each tables or views
    for table_view in tqdm(table_views):
        try:
            res = compare_table_results(table_view)
            diff_results.append(res)
        except Exception as e:
            diff_results.append({
                'table/view': table_view['name'],
                'query_redshift': '',
                'query_snowflake': '',
                'result_redshift': '-',
                'result_snowflake': '-',
                'time_redshift': '-',
                'time_snowflake': '-',
                'is_data_equal': False,
                'is_error': str(e),
                'diff_rate': '-',
                f'result(<= {err_rate_threshold}%)': 'NG'
            })
            logger.error(f'Error. {table_view=}', e)

    diff_results_df = pd.DataFrame(diff_results)
    now = datetime.datetime.now(pytz.timezone(os.getenv('TIME_ZONE')))

    if not os.path.exists('table_view_diff_results'):
        os.mkdir('table_view_diff_results')
    file_name: str = f'{os.path.dirname(__file__)}/table_view_diff_results/diff_results_{now.strftime("%Y%m%d_%H%M")}.xlsx'

    with pd.ExcelWriter(file_name) as writer:
        diff_results_df.style.set_properties(**{'vertical-align': 'top'}).\
            to_excel(writer, sheet_name='Diff results', index=False, na_rep='NaN')
        format_excel(diff_results_df, writer)
        writer.save()
    end = time.time()
    logger.info(f'output results to {file_name}. {round(end - start, 1)} sec')

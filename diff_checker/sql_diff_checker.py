import argparse
import datetime
import json
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

load_dotenv()
warnings.simplefilter('ignore')

parser = argparse.ArgumentParser(description='Compare sql result on redshift to snowflake.')
parser.add_argument('--sql_dirs', nargs='*',
                    help='sql dir to compare result', type=str, default=['./diff_checker/sql'])
parser.add_argument('--sql_param_file', nargs='*',
                    help='file to set sql parameters', type=str, default='./diff_checker/sql/sql_param.json')

args = parser.parse_args()
print(f'>>>>>>>>>>>>>>>>>>>>> {os.path.dirname(__file__)=}')
now = datetime.datetime.now(pytz.timezone(os.getenv('TIME_ZONE')))
logger = setup_logger(__name__, f'{os.path.dirname(__file__)}/logs/sql_diff_checker_{now.strftime("%Y%m%d_%H%M")}.log')
logger.info('----------------------------------------------------')
logger.info('Start compare sql result on redshift to snowflake.')
logger.info('----------------------------------------------------')

err_rate_threshold = float(os.getenv('DIFF_CHECKER_ERROR_RATE_THRESHOLD', '0.0001'))

snowflake_conn = SnowflakeConnector()
redshift_conn = RedshiftConnector()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def compare_result(sql_redshift: str, sql_snowflake: str, result: dict):
    logger.debug(f'compare_result()')
    assert_flg = 0
    df_redshift, df_snowflake = None, None
    time_redshift, time_snowflake = None, None
    try:
        logger.debug(f'Execute sql on Snowflake')
        df_snowflake, time_snowflake = exec_query_snowflake(snowflake_conn, sql_snowflake)
        logger.debug(f'Execute sql on Redshift')
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
                'sql_redshift': sql_redshift,
                'sql_snowflake': sql_snowflake,
                'result_redshift': df_redshift,
                'result_snowflake': df_snowflake,
                'time_redshift': round(time_redshift, 2) if time_redshift is not None else '',
                'time_snowflake': round(time_snowflake, 2) if time_snowflake is not None else '',
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
                    'sql_redshift': sql_redshift,
                    'sql_snowflake': sql_snowflake,
                    'result_redshift': df_redshift,
                    'result_snowflake': df_snowflake,
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
                    'sql_redshift': sql_redshift,
                    'sql_snowflake': sql_snowflake,
                    'is_data_equal': False,
                    'is_error': True,
                    'diff_rate': '-',
                    f'result(<= {err_rate_threshold}%)': 'NG'
                }
            )
        logger.exception(ex)

    return result


def get_sql_default_result(dir: str, file_name: str) -> dict:
    default_result = {
        'file_name': f'{file_name}',
        'sql_redshift': None,
        'sql_snowflake': None,
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


def format_excel(diff_results_df: pd.DataFrame, excel_writer: pd.ExcelWriter):
    sheet = excel_writer.sheets['Diff results']
    columns = diff_results_df.columns
    # column width
    col_idx = columns.get_loc('file_name')
    sheet.set_column(col_idx, col_idx, 30)
    col_idx = columns.get_loc('sql_redshift')
    sheet.set_column(col_idx, col_idx, 15)
    col_idx = columns.get_loc('sql_snowflake')
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


def set_params(sql: str, params: dict) -> str:
    for k, v in params.items():
        param = '{{' + k + '}}'
        sql = sql.replace(param, v)
    return sql


if __name__ == '__main__':
    start = time.time()
    diff_results: list[dict] = []
    sqls: list[str] = []
    logger.info(f'Compare sql result data. {args.sql_dirs=}')
    for sql_dir in args.sql_dirs:
        redshift_dir = os.path.join(sql_dir, 'redshift')
        snowflake_dir = os.path.join(sql_dir, 'snowflake')
        files: list[str] = os.listdir(redshift_dir)
        sql_files: list[str] = [f for f in files
                             if os.path.isfile(os.path.join(redshift_dir, f)) and f.endswith('.sql')]

        logger.info(f'Compare sql result data. {sql_files=}')

        # compare sql results
        for sql_file in sql_files:
            logger.info(f'{sql_file=}')
            with open(f'{redshift_dir}/{sql_file}') as file:
                sql_redshift = file.read()
            with open(f'{snowflake_dir}/{sql_file}') as file:
                sql_snowflake = file.read()

            # set params
            with open(args.sql_param_file) as json_file:
                sql_params = json.load(json_file)
                sql_redshift = set_params(sql_redshift, sql_params)
                sql_snowflake = set_params(sql_snowflake, sql_params)

            try:
                default_result = get_sql_default_result(sql_dir, file.name)
                result = compare_result(sql_redshift, sql_snowflake, default_result)
                diff_results.append(result)
            except Exception as e:
                diff_results.append({
                    'file_name': file.name,
                    'sql_redshift': '',
                    'sql_snowflake': '',
                    'result_redshift': '-',
                    'result_snowflake': '-',
                    'time_redshift': '-',
                    'time_snowflake': '-',
                    'is_data_equal': False,
                    'is_error': str(e),
                    'diff_rate': '-',
                    f'result(<= {err_rate_threshold}%)': 'NG'
                })
                logger.error(f'Error. {file.name=}', e)

    diff_results_df = pd.DataFrame(diff_results)
    now = datetime.datetime.now(pytz.timezone(os.getenv('TIME_ZONE')))

    if not os.path.exists('sql_diff_results'):
        os.mkdir('sql_diff_results')
    file_name: str = f'{os.path.dirname(__file__)}/sql_diff_results/diff_results_{now.strftime("%Y%m%d_%H%M")}.xlsx'

    with pd.ExcelWriter(file_name) as writer:
        diff_results_df.style.set_properties(**{'vertical-align': 'top'}).\
            to_excel(writer, sheet_name='Diff results', index=False, na_rep='NaN')
        format_excel(diff_results_df, writer)
        writer.save()
    end = time.time()
    logger.info(f'output results to {file_name}. {round(end - start, 1)} sec')

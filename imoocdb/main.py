"""
该文件，是IMoocDB的核心功能总入口
"""
import logging
import os

from imoocdb.catalog import init_catalog
from imoocdb.constant import DEFAULT_WORKING_DIRECTORY
from imoocdb.sql.parser.parser import query_parse
from imoocdb.sql.optimizier.planner import query_plan
from imoocdb.executor import exec_plan, Result
from imoocdb.errors import RollbackError, NoticeError

empty_result = Result()


def notice_client(level, message):
    # print(f'{level}: {message}')
    logging.error(message)


def init_database_working_directory(path=DEFAULT_WORKING_DIRECTORY):
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)


def init_database(path=DEFAULT_WORKING_DIRECTORY):
    init_database_working_directory(path)
    init_catalog()


def exec_imoocdb_query(query_string) -> Result:
    try:
        ast = query_parse(query_string)
        plan = query_plan(ast)
        result = exec_plan(plan)
        return result
    except RollbackError as e:
        notice_client('ERROR', f'Cannot execute this query because {e}, aborting.')
        # todo: rollback operation
    except NoticeError as e:
        notice_client('ERROR', f'Cannot execute this query because {e}.')
    except Exception as e:
        logging.exception(e)
    return empty_result


def start_simple_terminal_client():
    init_database()
    while True:
        full_query_string = ''
        partial_string = input('> ')
        full_query_string += partial_string
        while ';' not in partial_string:
            partial_string = input('> ')
            full_query_string += partial_string
        print("starting query: ", full_query_string)
        result = exec_imoocdb_query(full_query_string.strip('; '))
        print(result)


start_simple_terminal_client()

"""
该文件，是IMoocDB的核心功能总入口
"""
import logging

from imoocdb.sql.parser.parser import query_parse
from imoocdb.sql.optimizier.planner import query_plan
from imoocdb.executor import exec_plan, Result
from imoocdb.errors import RollbackError, NoticeError

empty_result = Result()


def notice_client(level, message):
    # print(f'{level}: {message}')
    logging.error(message)


def exec_imoocdb_query(query_string) -> Result:
    try:
        ast = query_parse(query_string)
        plan = query_plan(ast)
        result = exec_plan(plan)
        return result
    except RollbackError as e:
        notice_client('ERROR', f'Cannot execute this query due to {e}, aborting.')
        # todo: rollback operation
    except NoticeError as e:
        notice_client('ERROR', f'Cannot execute this query due to {e}.')
    return empty_result


def start_simple_terminal_client():
    while True:
        full_query_string = ''
        partial_string = input('$ ')
        full_query_string += partial_string
        while ';' not in partial_string:
            partial_string = input('> ')
            full_query_string += partial_string
        print("starting query: ", full_query_string)
        result = exec_imoocdb_query(full_query_string.strip('; '))
        print(result)


start_simple_terminal_client()

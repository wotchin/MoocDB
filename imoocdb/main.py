"""
该文件，是IMoocDB的核心功能总入口
"""
import logging
import os
import re
import threading

from imoocdb.catalog import init_catalog
from imoocdb.constant import DEFAULT_WORKING_DIRECTORY
from imoocdb.sql.parser.parser import query_parse
from imoocdb.sql.optimizier.planner import query_plan
from imoocdb.executor import exec_plan, Result
from imoocdb.errors import RollbackError, NoticeError
from imoocdb.storage.transaction.entry import transaction_mgr
from network.pg_protocol import PGHandler, Int8Field, TextField, start_server
from session_manager import set_session_parameter, get_session_parameter
import instr

empty_result = Result()


def notice_client_terminal(level, message):
    # print(f'{level}: {message}')
    logging.error(message)


def init_database_working_directory(path=DEFAULT_WORKING_DIRECTORY):
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)


def init_database(path=DEFAULT_WORKING_DIRECTORY):
    init_database_working_directory(path)
    init_catalog()
    transaction_mgr.recovery()


def exec_imoocdb_query(query_string, notice_client=notice_client_terminal) -> Result:
    xid = -1
    try:
        ast = query_parse(query_string)
        plan = query_plan(ast)
        if plan.name == 'Command':
            result = exec_plan(plan)
        else:
            xid = transaction_mgr.start_transaction()
            # 传递引用，不要传递具体值
            instr.transaction_count += 1
            result = exec_plan(plan)
            transaction_mgr.commit_transaction(xid)
        return result
    except RollbackError as e:
        if xid > 0:
            transaction_mgr.abort_transaction(xid)
        notice_client('ERROR', f'Cannot execute this query because {e}, aborting.')
        # todo: rollback operation
    except NoticeError as e:
        notice_client('ERROR', f'Cannot execute this query because {e}.')
    except Exception as e:
        if xid > 0:
            transaction_mgr.abort_transaction(xid)
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


def bytes_to_str(b):
    return b.strip(b'\x00').decode()


class IMoocDBHandler(PGHandler):
    def set_session_info(self, parameters):
        pair = []
        for i, p in enumerate(parameters):
            pair.append(bytes_to_str(p))
            if i % 2 != 0:
                k, v = pair
                set_session_parameter(k, v)
                pair.clear()
        set_session_parameter('id', threading.get_native_id())
        set_session_parameter('client', '%s:%d' % self.client_address)

    def check_password(self, password: bytes):
        # ACL的鉴权过程
        client = get_session_parameter('client')
        user = get_session_parameter('user')
        # if not client.startswith('127.0.0.1') or user != 'postgres':
        #     return False
        return bytes_to_str(password) == 'abcd'

    def query(self, sql):
        # # mock测试数据
        # fields = [Int8Field('a'), TextField('b')]
        # rows = [[1, 'a'], [3, None], [5, 'c']]
        # 用l来表示如果报错的话，这个错误级别
        # m表示具体的报错的错误信息
        l = None
        m = None

        # 这里是个callback函数，当发生问题的时候，会调用该函数，把
        # 报错信息返回出去
        def helper(level, message):
            nonlocal l, m
            l = level
            m = message

        result = exec_imoocdb_query(bytes_to_str(sql).strip(';'),
                                    notice_client=helper)
        if m is not None and l is not None:
            if l == 'NOTICE':
                raise NoticeError(m)
            elif l == 'ERROR':
                raise RollbackError(m)
            else:
                raise

        if result.target_columns is None:
            # insert协议实际上不是走下面的返回结果，但是不影响真实的
            # 执行效果，也不影响用户的理解
            fields = [Int8Field('effect rows')]
            rows = [[len(result.rows)]]
        else:
            fields = [TextField(str(c)) for c in result.target_columns]
            rows = result.rows
        return fields, rows


def start_simple_imoocdb_process():
    start_server('localhost', 54321, IMoocDBHandler)


# start_simple_terminal_client()
start_simple_imoocdb_process()

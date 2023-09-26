import os
import shutil

import pytest

from imoocdb.catalog import CatalogTableForm, CatalogIndexForm
from imoocdb.catalog.entry import catalog_index, catalog_table
from imoocdb.main import init_database
from imoocdb.storage.entry import table_tuple_insert_one, index_tuple_create
from imoocdb.storage.transaction.entry import transaction_mgr

TEST_DATA_DIRECTORY = 'test_database'


#
# mock_table_data = {
#     't1': [
#         (1, 'xiaoming'),
#         (2, 'xiaohong'),
#         (3, 'xiaoli'),
#         (4, 'xiaoguo'),
#     ],
#     't2': [
#         (1, 'ming', 'BJ'),
#         (5, 'hong', 'SH'),
#         (3, 'li', 'SZ'),
#     ]
# }
#
# mock_dead_tuple = {
#     't1': {},
#     't2': {}
# }

def setup():
    if os.path.exists(TEST_DATA_DIRECTORY):
        shutil.rmtree(TEST_DATA_DIRECTORY)

    init_database(TEST_DATA_DIRECTORY)

    catalog_table.insert(CatalogTableForm('t1', ['id', 'name'], [int, str]))
    catalog_table.insert(CatalogTableForm('t2', ['id', 'name', 'address'], [int, str, str]))

    # 下面这个造的索引等价于执行了:
    # create index idx on t1(id);
    catalog_index.insert(CatalogIndexForm('idx', ['id'], 't1'))

    xid = transaction_mgr.start_transaction()

    table_tuple_insert_one('t1', (1, 'xiaoming'))
    table_tuple_insert_one('t1', (2, 'xiaohong'))
    table_tuple_insert_one('t1', (3, 'xiaoli'))
    table_tuple_insert_one('t1', (4, 'xiaoguo'))

    table_tuple_insert_one('t2', (1, 'ming', 'BJ'))
    table_tuple_insert_one('t2', (5, 'hong', 'SH'))
    table_tuple_insert_one('t2', (3, 'li', 'SZ'))

    transaction_mgr.commit_transaction(xid)

    index_tuple_create('idx', 't1', ['id'])


def teardown():
    shutil.rmtree(TEST_DATA_DIRECTORY, ignore_errors=True)


@pytest.fixture(scope='session', autouse=True)
def run_before_and_after_test_case():
    setup()

    yield

    teardown()

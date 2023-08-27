from imoocdb.executor.operator.physical_operator import (
    TableScan, IndexScan, CoveredIndexScan, Sort, HashAgg
)
from imoocdb.common.fabric import TableColumn

from imoocdb.sql.logical_operator import Condition
from imoocdb.sql.parser.ast import BinaryOperation, Identifier, Constant


def construct_condition(sign, column, value):
    b = BinaryOperation(op=sign, args=(
        Identifier(column), Constant(value)))
    return Condition(b)


def test_table_scan():
    opt = TableScan('t1')
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(1, 'xiaoming'), (2, 'xiaohong'), (3, 'xiaoli'), (4, 'xiaoguo')]

    opt = TableScan('t1', construct_condition('>', 't1.id', 3))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(4, 'xiaoguo')]

    opt = TableScan('t1', construct_condition('<', 't1.id', 0))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == []

    opt = TableScan('t1', construct_condition('=', 't1.id', 1))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(1, 'xiaoming'), ]


def test_index_scan():
    opt = IndexScan('idx', construct_condition('>', 't1.id', 3))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(4, 'xiaoguo')]

    opt = IndexScan('idx', construct_condition('<', 't1.id', 0))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == []

    opt = IndexScan('idx', construct_condition('=', 't1.id', 1))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(1, 'xiaoming'), ]


def test_covered_index_scan():
    opt = CoveredIndexScan('idx', construct_condition('>', 't1.id', 3))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(4,)]

    opt = CoveredIndexScan('idx', construct_condition('<', 't1.id', 0))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == []

    opt = CoveredIndexScan('idx', construct_condition('=', 't1.id', 1))
    opt.open()
    results = list(opt.next())
    opt.close()
    assert results == [(1,), ]


def test_sort():
    opt = Sort(TableColumn('t1', 'name'))
    opt.method = Sort.EXTERNAL_SORT
    opt.add_child(TableScan('t1'))
    # 到此位置，这个计划“树”就是：
    # Sort (sort_column: name)
    #   -> TableScan (table_name: t1)
    opt.open()
    results = list(opt.next())
    assert results == [(4, 'xiaoguo'), (2, 'xiaohong'), (3, 'xiaoli'), (1, 'xiaoming')]
    opt.close()


def test_hash_agg():
    opt = HashAgg(group_by_column=TableColumn('t1', 'id'),
                  aggregate_function_name='count',
                  aggregate_column=TableColumn('t1', 'name'))
    opt.add_child(TableScan('t1'))
    # 到此位置，这个计划“树”就是：
    # HashAgg (group_column: id, agg_column: name, function: count)
    #   -> TableScan (table_name: t1)
    # 相当于执行了:
    # select count(t1.name) from t1 group by t1.id;
    opt.open()
    results = list(opt.next())
    print(results)
    # assert results == [(4, 'xiaoguo'), (2, 'xiaohong'), (3, 'xiaoli'), (1, 'xiaoming')]
    opt.close()


test_hash_agg()

from imoocdb.executor.operator.physical_operator import (
    TableScan, IndexScan, CoveredIndexScan
)

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


test_table_scan()
test_index_scan()
test_covered_index_scan()


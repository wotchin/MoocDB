from imoocdb.executor.operator.physical_operator import (
    TableScan, IndexScan, CoveredIndexScan, Sort, HashAgg,
    cast_tuple_pair_to_values, NestedLoopJoin,
    PhysicalInsert, PhysicalDelete, PhysicalUpdate,
    LocationScan)
from imoocdb.common.fabric import TableColumn

from imoocdb.sql.logical_operator import (
    Condition, InsertOperator, UpdateOperator, DeleteOperator)
from imoocdb.sql.parser.ast import BinaryOperation, Identifier, Constant, JoinType
from imoocdb.storage.entry import table_tuple_get_all, index_tuple_get_equal_value
from imoocdb.storage.transaction.entry import transaction_mgr


def construct_condition(sign, column, value):
    b = BinaryOperation(op=sign, args=(
        Identifier(column), Constant(value)))
    return Condition(b)


def construct_join_condition(sign, column1, column2):
    b = BinaryOperation(op=sign, args=(
        Identifier(column1), Identifier(column2)))
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


def test_cast_tuple_pair_to_values():
    columns = (TableColumn('t1', 'id'), TableColumn('t1', 'name'),
               TableColumn('t2', 'id'), TableColumn('t2', 'name'))
    tup = (1, 'aaa', 1, 'bbbb')
    rv = cast_tuple_pair_to_values(columns, tup)
    assert str(rv) == "{t1.id: 1, t1.name: 'aaa', t2.id: 1, t2.name: 'bbbb'}"


def test_nested_loop_join():
    opt = NestedLoopJoin(
        JoinType.FULL_JOIN, 't1', 't2', construct_join_condition('=', 't1.id', 't2.id')
    )
    opt.add_child(TableScan('t1'))
    opt.add_child(TableScan('t2'))
    opt.open()
    print((list(opt.next())))
    opt.close()


def test_physical_dml():
    xid = transaction_mgr.start_transaction()

    logical_insert = InsertOperator(
        't1',
        [TableColumn('t1', 'id'), TableColumn('t1', 'name')],
        [(1, 'foo'), (2, 'bar')]
    )
    physical_insert = PhysicalInsert(logical_insert)
    physical_insert.open()
    list(physical_insert.next())
    physical_insert.close()
    assert list(table_tuple_get_all('t1')) == [(1, 'xiaoming'),
                                               (2, 'xiaohong'),
                                               (3, 'xiaoli'),
                                               (4, 'xiaoguo'),
                                               (1, 'foo'),
                                               (2, 'bar')]
    logical_delete = DeleteOperator(
        't1',
        condition=construct_condition('=', 't1.name', 'foo')
    )
    physical_delete = PhysicalDelete(
        logical_delete
    )
    location_scan = LocationScan(TableScan('t1',
                                           condition=logical_delete.condition))
    physical_delete.add_child(location_scan)
    physical_delete.open()
    list(physical_delete.next())
    physical_delete.close()
    assert (list(table_tuple_get_all('t1'))) == \
           [(1, 'xiaoming'), (2, 'xiaohong'), (3, 'xiaoli'), (4, 'xiaoguo'), (2, 'bar')]

    logical_update = UpdateOperator('t1',
                                    [TableColumn('t1', 'name')],
                                    ['foo'],
                                    condition=construct_condition('=', 't1.name', 'bar'))
    physical_update = PhysicalUpdate(logical_update)
    physical_update.add_child(LocationScan(TableScan('t1', condition=logical_update.condition)))
    physical_update.open()
    list(physical_update.next())
    physical_update.close()
    assert (list(table_tuple_get_all('t1'))) == \
           [(1, 'xiaoming'), (2, 'xiaohong'), (3, 'xiaoli'), (4, 'xiaoguo'), (2, 'foo')]

    logical_delete = DeleteOperator(
        't1',
        condition=construct_condition('=', 't1.name', 'foo')
    )
    physical_delete = PhysicalDelete(
        logical_delete
    )
    location_scan = LocationScan(TableScan('t1',
                                           condition=logical_delete.condition))
    physical_delete.add_child(location_scan)
    physical_delete.open()
    list(physical_delete.next())
    physical_delete.close()

    transaction_mgr.commit_transaction(xid)


def test_abort_physical_dml():
    old_rows = list(table_tuple_get_all('t1'))
    xid = transaction_mgr.start_transaction()
    logical_insert = InsertOperator(
        't1',
        [TableColumn('t1', 'id'), TableColumn('t1', 'name')],
        [(1, 'hello'), (2, 'world')]
    )
    physical_insert = PhysicalInsert(logical_insert)
    physical_insert.open()
    list(physical_insert.next())
    physical_insert.close()

    inserted_rows = list(table_tuple_get_all('t1'))
    assert inserted_rows == old_rows + [(1, 'hello'), (2, 'world')]
    transaction_mgr.abort_transaction(xid)
    rows = list(table_tuple_get_all('t1'))
    assert rows == old_rows

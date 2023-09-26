import os

from imoocdb.catalog.entry import catalog_table, catalog_index
from imoocdb.errors import PageError
from imoocdb.storage.bplus_tree import BPlusTree, BPlusTreeTuple, load_root_node
from imoocdb.storage.common import get_table_filename, table_tuple_get_pages, table_tuple_get_page, tuple_to_bytes, \
    bytes_to_tuple, get_index_filename
from imoocdb.storage.lru import buffer_pool
from imoocdb.storage.slotted_page import Page
from imoocdb.storage.transaction.entry import transaction_mgr
from imoocdb.storage.transaction.redo import RedoRecord, RedoAction
from imoocdb.storage.transaction.undo import UndoRecord, UndoOperation


def table_tuple_get_all(table_name):
    # todo: 如果该函数的所有调用接口，都能确保传递进来的参数 table_name
    # 是有意义的，那么，此函数内部，就不需要再进行重复的判断了！！！
    assert catalog_table.select(lambda r: r.table_name == table_name)
    # 迪米特法则：（最小知道/知识原则）：上层的函数/class对底层的实现知道越少越好
    for location in table_tuple_get_all_locations(table_name):
        yield table_tuple_get_one(table_name, location)


def table_tuple_get_page_tuples(table_name, pageno):
    page = table_tuple_get_page(table_name, pageno)
    return len(page.slot_directory)


def table_tuple_get_all_locations(table_name):
    # 我们手动跳过被标记为空的元组
    for pageno in range(0, table_tuple_get_pages(table_name)):
        for sid in range(0, table_tuple_get_page_tuples(table_name, pageno)):
            # 返回的 location 是一个二元组
            # todo: 判断当前返回给用户的tuple是否为死元组，
            # 无效元组应该过滤
            location = (pageno, sid)
            if not table_tuple_is_dead(table_name, location):
                yield location


def table_tuple_get_one(table_name, location):
    pageno, sid = location
    page = table_tuple_get_page(table_name, pageno=pageno)
    return bytes_to_tuple(page.select(sid))


def table_tuple_is_dead(table_name, location):
    return len(table_tuple_get_one(table_name, location)) == 0


def table_tuple_update_one(table_name, location, tup):
    pageno, sid = location
    page = table_tuple_get_page(table_name, pageno=pageno)
    xid = transaction_mgr.session_xid()
    old_tuple_bytes = tuple_to_bytes(page.select(sid))

    try:
        new_sid = page.update(sid, tuple_to_bytes(tup))
        buffer_pool.mark_dirty((table_name, pageno))
        # 写日志
        undo_record = UndoRecord(
            xid, UndoOperation.TABLE_UPDATE, table_name, (pageno, sid),
            old_tuple_bytes
        )
        redo_record = RedoRecord(
            xid, RedoAction.TABLE_UPDATE, table_name, (pageno, new_sid),
            tuple_to_bytes(tup)
        )
        transaction_mgr.undo_mgr.write(undo_record)
        lsn = transaction_mgr.redo_mgr.write(redo_record)
        page.set_header(lsn)
    except PageError:
        # 只存在 insert 无法插入数据，是因为没有空间了，才会导致
        # 因此我们只需要处理该种异常即可
        table_tuple_delete_one(table_name, location)
        new_sid = table_tuple_insert_one(table_name, tup)
    return new_sid


def table_tuple_get_last_pageno(table_name):
    return max(table_tuple_get_pages(table_name) - 1, 0)


def table_tuple_allocate_page(table_name):
    pageno = table_tuple_get_last_pageno(table_name)
    table_tuple_get_page(table_name, pageno)
    new_pageno = table_tuple_get_last_pageno(table_name)
    assert new_pageno == pageno + 1
    return new_pageno


def table_tuple_insert_one(table_name, tup):
    pageno = table_tuple_get_last_pageno(table_name)
    if pageno < 0:
        pageno = table_tuple_allocate_page(table_name)

    # 产生了非常多的 overhead, 这也进一步证明了 buffer 的重要性
    page = table_tuple_get_page(table_name, pageno=pageno)
    xid = transaction_mgr.session_xid()
    tuple_bytes = tuple_to_bytes(tup)

    try:
        sid = page.insert(tuple_to_bytes(tup))
        buffer_pool.mark_dirty((table_name, pageno))
        # write logs
        undo_record = UndoRecord(xid,
                                 UndoOperation.TABLE_DELETE,
                                 table_name, (pageno, sid),
                                 b'')
        redo_record = RedoRecord(
            xid, RedoAction.TABLE_INSERT, table_name, (pageno, sid), tuple_bytes
        )
        transaction_mgr.undo_mgr.write(undo_record)
        lsn = transaction_mgr.redo_mgr.write(redo_record)
        page.set_header(lsn=lsn)
    except PageError:
        new_pageno = table_tuple_allocate_page(table_name)
        new_page = table_tuple_get_page(table_name, new_pageno)
        sid = new_page.insert(tuple_bytes)
        buffer_pool.mark_dirty((table_name, new_pageno))

        # write logs
        undo_record = UndoRecord(xid,
                                 UndoOperation.TABLE_DELETE,
                                 table_name, (new_pageno, sid),
                                 b'')
        redo_record = RedoRecord(
            xid, RedoAction.TABLE_INSERT, table_name, (new_pageno, sid), tuple_bytes
        )
        transaction_mgr.undo_mgr.write(undo_record)
        lsn = transaction_mgr.redo_mgr.write(redo_record)
        page.set_header(lsn=lsn)

    return pageno, sid


def table_tuple_delete_one(table_name, location):
    pageno, sid = location
    page = table_tuple_get_page(table_name, pageno=pageno)
    xid = transaction_mgr.session_xid()
    old_tuple_bytes = tuple_to_bytes(table_tuple_get_one(table_name, location))
    page.delete(sid)
    buffer_pool.mark_dirty((table_name, pageno))

    # write logs
    undo_record = UndoRecord(xid,
                             UndoOperation.TABLE_INSERT,
                             table_name, (pageno, sid),
                             old_tuple_bytes)
    redo_record = RedoRecord(
        xid, RedoAction.TABLE_DELETE, table_name, (pageno, sid), b''
    )
    transaction_mgr.undo_mgr.write(undo_record)
    lsn = transaction_mgr.redo_mgr.write(redo_record)
    page.set_header(lsn=lsn)


def table_tuple_reorganize(table_name):
    # 把所有的tuple的状态为 dead 的元组，统一进行整理
    pass


def table_tuple_delete_multiple(table_name, locations):
    for location in locations:
        table_tuple_delete_one(table_name, location)
    table_tuple_reorganize(table_name)


def index_tuple_create(index_name, table_name, columns):
    table_columns = catalog_table.select(
        lambda r: r.table_name == table_name
    )[0].columns
    # 获取索引列的下标
    columns_indexes = [table_columns.index(c) for c in columns]

    filename = get_index_filename(index_name)
    tree = BPlusTree(filename)

    for location in table_tuple_get_all_locations(table_name):
        tup = table_tuple_get_one(table_name, location)
        key = BPlusTreeTuple(tuple(tup[i] for i in columns_indexes))
        tree.insert(key, location)

    tree.serialize()


def range_compare(value, start, end):
    if start is None and end is None:
        return False
    elif start is None:
        return value < end
    elif end is None:
        return start < value
    else:
        return start < value < end


def index_tuple_get_range_locations(index_name, start=float('-inf'), end=float('inf')):
    """start, end 两个参数，是用来指定扫描索引中部分数据的，如果不给这两个参数赋值，
    那么，就默认拿这个索引中的全部数据.
    """
    filename = get_index_filename(index_name)
    tree = BPlusTree(filename, load_root_node(filename))
    for location in tree.find_range(start, end):
        yield location


def index_tuple_get_range(index_name, start=None, end=None):
    """start, end 两个参数，是用来指定扫描索引中部分数据的，如果不给这两个参数赋值，
    那么，就默认拿这个索引中的全部数据.
    """
    results = catalog_index.select(lambda r: r.index_name == index_name)
    table_name = results[0].table_name
    for location in index_tuple_get_range_locations(index_name, start, end):
        # 该过程就是**回表**过程，即从全量表数据中获取location的部分
        yield table_tuple_get_one(table_name, location)


def index_tuple_get_equal_value_locations(index_name, equal_value):
    filename = get_index_filename(index_name)
    tree = BPlusTree(filename, load_root_node(filename))
    for location in tree.find(equal_value):
        yield location


def index_tuple_get_equal_value(index_name, equal_value):
    results = catalog_index.select(lambda r: r.index_name == index_name)
    table_name = results[0].table_name
    for location in index_tuple_get_equal_value_locations(index_name, equal_value):
        yield table_tuple_get_one(table_name, location)


def covered_index_tuple_get_range(index_name, start=float('-inf'), end=float('inf')):
    filename = get_index_filename(index_name)
    tree = BPlusTree(filename, load_root_node(filename))
    for key in tree.find_range(start, end, return_keys=True):
        # key 的数据类型是 BPlusTreeTuple
        yield key.tup


def covered_index_tuple_get_equal_value(index_name, equal_value):
    for location in index_tuple_get_equal_value_locations(index_name, equal_value):
        yield equal_value


def index_tuple_insert_one(index_name, key, value):
    filename = get_index_filename(index_name)
    xid = transaction_mgr.session_xid()
    tree = BPlusTree(filename, load_root_node(filename))
    tree.insert(BPlusTreeTuple(key), value)

    # 这个key的数据类型并不是字节集合，但是不影响其的序列化过程，
    # 这个Python的弱类型允许这么写
    transaction_mgr.undo_mgr.write(UndoRecord(
        xid, UndoOperation.INDEX_DELETE,
        index_name, value,
        key
    ))

    # todo: LSN
    tree.serialize()


def index_tuple_delete_one(index_name, key, location=None):
    filename = get_index_filename(index_name)
    xid = transaction_mgr.session_xid()

    tree = BPlusTree(filename, load_root_node(filename))
    old_locations = tree.find(key)
    tree.delete(key, location)

    # only undo log
    for old_location in old_locations:
        transaction_mgr.undo_mgr.write(UndoRecord(
            xid, UndoOperation.INDEX_INSERT,
            index_name, old_location,
            key
        ))

    # todo: LSN
    tree.serialize()


def index_tuple_update_one(index_name, key, old_value, value):
    index_tuple_delete_one(index_name, key, old_value)
    index_tuple_insert_one(index_name, key, value)

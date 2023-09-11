import os
import pickle

from imoocdb.catalog.entry import catalog_table, catalog_index
from imoocdb.constant import DATA_DIRECTORY
from imoocdb.errors import PageError
from imoocdb.storage.slotted_page import Page, PAGE_SIZE
from imoocdb.storage.transaction.entry import get_current_lsn


def table_tuple_get_all(table_name):
    # todo: 如果该函数的所有调用接口，都能确保传递进来的参数 table_name
    # 是有意义的，那么，此函数内部，就不需要再进行重复的判断了！！！
    assert catalog_table.select(lambda r: r.table_name == table_name)
    # 迪米特法则：（最小知道/知识原则）：上层的函数/class对底层的实现知道越少越好
    for location in table_tuple_get_all_locations(table_name):
        yield table_tuple_get_one(table_name, location)


def get_table_filename(table_name):
    if not os.path.exists(DATA_DIRECTORY):
        os.mkdir(DATA_DIRECTORY)
    return os.path.join(DATA_DIRECTORY, table_name + '.tbl')


def table_tuple_get_pages(table_name):
    filename = get_table_filename(table_name)
    if not os.path.exists(filename):
        return 0

    if os.stat(filename).st_size > 0:
        assert os.stat(filename).st_size % PAGE_SIZE == 0
    return os.stat(filename).st_size // PAGE_SIZE


# 后面我们用LRU去替换该cache
cache = {}


def table_tuple_get_page(table_name, pageno):
    filename = get_table_filename(table_name)
    if not os.path.exists(filename):
        return None

    key = (table_name, pageno)
    if key not in cache:
        cache[key] = Page()
    page = cache[key]
    return page

    # # todo: 当前page 没有首先从 buffer 里读取，而是直接读的磁盘，后面
    # # 需要先过buffer
    # with open(filename, 'rb') as f:
    #     f.seek(pageno * PAGE_SIZE)
    #     buff = f.read(PAGE_SIZE)
    #     page = Page.deserialize(buff)
    # return page


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


def tuple_to_bytes(tup):
    return pickle.dumps(tup)


def bytes_to_tuple(bytes_):
    if len(bytes_) == 0:
        return ()
    return pickle.loads(bytes_)


def table_tuple_update_one(table_name, location, tup):
    # table_tuple_delete_one(table_name, location)
    # return table_tuple_insert_one(table_name, tup)
    pageno, sid = location
    page = table_tuple_get_page(table_name, pageno=pageno)
    try:
        new_sid = page.update(sid, tuple_to_bytes(tup))
    except PageError:
        # 只存在 insert 无法插入数据，是因为没有空间了，才会导致
        # 因此我们只需要处理该种异常即可
        page.delete(sid)
        new_pageno = table_tuple_allocate_page(table_name)
        new_page = table_tuple_get_page(table_name, new_pageno)
        new_sid = new_page.insert(tuple_to_bytes(tup))
    # todo: WAL
    page.set_header(lsn=get_current_lsn())
    return new_sid


def table_tuple_get_last_pageno(table_name):
    return table_tuple_get_pages(table_name) - 1


def table_tuple_allocate_page(table_name):
    filename = get_table_filename(table_name)
    with open(filename, 'ab') as f:
        page = Page()
        page.set_header(get_current_lsn())
        f.write(page.serialize())
        os.fsync(f.fileno())
    return table_tuple_get_last_pageno(table_name)


def table_tuple_insert_one(table_name, tup):
    # mock_table_data[table_name].append(tup)
    # return len(mock_table_data[table_name]) - 1
    pageno = table_tuple_get_last_pageno(table_name)
    if pageno < 0:
        pageno = table_tuple_allocate_page(table_name)

    # 产生了非常多的 overhead, 这也进一步证明了 buffer 的重要性
    page = table_tuple_get_page(table_name, pageno=pageno)
    try:
        sid = page.insert(tuple_to_bytes(tup))
    except PageError:
        new_pageno = table_tuple_allocate_page(table_name)
        new_page = table_tuple_get_page(table_name, new_pageno)
        sid = new_page.insert(tuple_to_bytes(tup))
    # todo: WAL
    page.set_header(lsn=get_current_lsn())
    return sid


def table_tuple_delete_one(table_name, location):
    # table_tuple_mark_dead(table_name, location)
    pageno, sid = location
    page = table_tuple_get_page(table_name, pageno=pageno)
    page.delete(sid)
    page.set_header(lsn=get_current_lsn())


def table_tuple_reorganize(table_name):
    # 把所有的tuple的状态为 dead 的元组，统一进行整理
    pass


def table_tuple_delete_multiple(table_name, locations):
    for location in locations:
        table_tuple_delete_one(table_name, location)
    table_tuple_reorganize(table_name)


mock_idx = {
    # 该索引的 key 是 t1 的 id 列的具体值，
    # 该索引的 value 是该行元组在原始表数据中的逻辑位置(location)
    'idx': {
        (1,): [(0, 0)],
        (2,): [(0, 1)],
        (3,): [(0, 2)],
        (4,): [(0, 3)]
    }
}


def index_tuple_create(index_name, table_name, columns):
    pass


def range_compare(value, start, end):
    if start is None and end is None:
        return False
    elif start is None:
        return value < end
    elif end is None:
        return start < value
    else:
        return start < value < end


def index_tuple_get_range_locations(index_name, start=None, end=None):
    """start, end 两个参数，是用来指定扫描索引中部分数据的，如果不给这两个参数赋值，
    那么，就默认拿这个索引中的全部数据.
    """
    for key in mock_idx[index_name]:
        # python语言天然支持元组(tuple)的比较，其他语言可能不支持，需要自己写
        if range_compare(key, start, end):
            # yield mock_idx[index_name][key]
            locations = mock_idx[index_name][key]
            for location in locations:
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
    if equal_value not in mock_idx[index_name]:
        return ()
    for location in mock_idx[index_name][equal_value]:
        yield location


def index_tuple_get_equal_value(index_name, equal_value):
    results = catalog_index.select(lambda r: r.index_name == index_name)
    table_name = results[0].table_name
    for location in index_tuple_get_equal_value_locations(index_name, equal_value):
        yield table_tuple_get_one(table_name, location)


def covered_index_tuple_get_range(index_name, start=None, end=None):
    for key in mock_idx[index_name]:
        if range_compare(key, start, end):
            # yield mock_idx[index_name][key]
            locations = mock_idx[index_name][key]
            # 该过程就是**回表**过程，即从全量表数据中获取location的部分
            # 我们注意了！covered index 没有回表过程，这就是 covered 的含义，
            # 也是 IndexOnlyScan 中 Only 的意思
            for location in locations:
                yield key


def covered_index_tuple_get_equal_value(index_name, equal_value):
    for location in mock_idx[index_name][equal_value]:
        yield equal_value


def index_tuple_insert_one(index_name, key, value):
    if key not in mock_idx[index_name]:
        mock_idx[index_name][key] = []
    mock_idx[index_name][key].append(value)


def index_tuple_delete_one(index_name, key, location=None):
    if location is None:
        # 移除所有该Key的索引
        del mock_idx[index_name][key]
    else:
        i = 0
        while i < len(mock_idx[index_name][key]):
            if location == mock_idx[index_name][key][i]:
                mock_idx[index_name][key].pop(i)
                i -= 1
            i += 1


def index_tuple_update_one(index_name, key, old_value, value):
    index_tuple_delete_one(index_name, key, old_value)
    index_tuple_insert_one(index_name, key, value)

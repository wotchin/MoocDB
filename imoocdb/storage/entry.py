from imoocdb.catalog.entry import catalog_table, catalog_index


def table_tuple_get_all(table_name):
    # todo: 如果该函数的所有调用接口，都能确保传递进来的参数 table_name
    # 是有意义的，那么，此函数内部，就不需要再进行重复的判断了！！！
    assert catalog_table.select(lambda r: r.table_name == table_name)
    # 迪米特法则：（最小知道/知识原则）：上层的函数/class对底层的实现知道越少越好
    for location in table_tuple_get_locations(table_name):
        yield table_tuple_get_one(table_name, location)


mock_table_data = {
    't1': [
        (1, 'xiaoming'),
        (2, 'xiaohong'),
        (3, 'xiaoli'),
        (4, 'xiaoguo'),
    ],
    't2': [
        (1, 'ming', 'BJ'),
        (2, 'hong', 'SH'),
        (3, 'li', 'SZ'),
    ]
}

def table_tuple_get_locations(table_name):
    if table_name == 't1':
        return [0, 1, 2, 3]
    elif table_name == 't2':
        return [0, 1, 2]


def table_tuple_get_one(table_name, location):
    return mock_table_data[table_name][location]


mock_idx = {
    # 该索引的 key 是 t1 的 id 列的具体值，
    # 该索引的 value 是该行元组在原始表数据中的逻辑位置(location)
    'idx': {
        (1,): [0],
        (2,): [1],
        (3,): [2],
        (4,): [3]
    }
}


def range_compare(value, start, end):
    if start is None and end is None:
        return False
    elif start is None:
        return value < end
    elif end is None:
        return start < value
    else:
        return start < value < end


def index_tuple_get_range(index_name, start=None, end=None):
    """start, end 两个参数，是用来指定扫描索引中部分数据的，如果不给这两个参数赋值，
    那么，就默认拿这个索引中的全部数据.
    """
    results = catalog_index.select(lambda r: r.index_name == index_name)
    table_name = results[0].table_name
    for key in mock_idx[index_name]:
        # python语言天然支持元组(tuple)的比较，其他语言可能不支持，需要自己写
        if range_compare(key, start, end):
            # yield mock_idx[index_name][key]
            locations = mock_idx[index_name][key]
            # 该过程就是**回表**过程，即从全量表数据中获取location的部分
            for location in locations:
                yield table_tuple_get_one(table_name, location)


def index_tuple_get_equal_value(index_name, equal_value):
    results = catalog_index.select(lambda r: r.index_name == index_name)
    table_name = results[0].table_name
    for location in mock_idx[index_name][equal_value]:
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

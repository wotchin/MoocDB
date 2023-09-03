from imoocdb.catalog.entry import catalog_table, catalog_index


def table_tuple_get_all(table_name):
    # todo: 如果该函数的所有调用接口，都能确保传递进来的参数 table_name
    # 是有意义的，那么，此函数内部，就不需要再进行重复的判断了！！！
    assert catalog_table.select(lambda r: r.table_name == table_name)
    # 迪米特法则：（最小知道/知识原则）：上层的函数/class对底层的实现知道越少越好
    for location in table_tuple_get_all_locations(table_name):
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
        (5, 'hong', 'SH'),
        (3, 'li', 'SZ'),
    ]
}

mock_dead_tuple = {
    't1': {},
    't2': {}
}


def table_tuple_get_all_locations(table_name):
    # 我们手动跳过被标记为空的元组
    for location in range(0, len(mock_table_data[table_name])):
        if not table_tuple_is_dead(table_name, location):
            yield location


def table_tuple_is_dead(table_name, location):
    return location in mock_dead_tuple[table_name]


def table_tuple_mark_dead(table_name, location):
    # todo: 此处可以用来标注一些数据，例如空闲区域大小，以便后面
    # 可以进行回收利用
    mock_dead_tuple[table_name][location] = None


def table_tuple_reuse(table_name, location):
    return mock_dead_tuple[table_name].pop(location)


def table_tuple_get_one(table_name, location):
    return mock_table_data[table_name][location]


def table_tuple_update_one(table_name, location, tup):
    table_tuple_delete_one(table_name, location)
    return table_tuple_insert_one(table_name, tup)


def table_tuple_insert_one(table_name, tup):
    mock_table_data[table_name].append(tup)
    return len(mock_table_data[table_name]) - 1


def table_tuple_delete_one(table_name, location):
    table_tuple_mark_dead(table_name, location)


def table_tuple_reorganize(table_name):
    pass


def table_tuple_delete_multiple(table_name, locations):
    for location in locations:
        table_tuple_delete_one(table_name, location)
    table_tuple_reorganize(table_name)


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

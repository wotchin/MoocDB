from imoocdb.storage.entry import (table_tuple_get_all,
                                   index_tuple_get_range,
                                   index_tuple_get_equal_value,
                                   covered_index_tuple_get_range,
                                   covered_index_tuple_get_equal_value,
                                   )


def test_table_tuple():
    expected_results = [
        (1, 'xiaoming'),
        (2, 'xiaohong'),
        (3, 'xiaoli'),
        (4, 'xiaoguo'),
    ]
    real_results = []
    for t in table_tuple_get_all('t1'):
        real_results.append(t)

    assert expected_results == real_results

#
# def test_index_tuple():
#     # Python 的生成器 generator
#     results = index_tuple_get_range('idx', (2,), (4,))
#     assert (list(results)) == [(3, 'xiaoli')]
#     results = index_tuple_get_range('idx', (2,))
#     assert (list(results)) == [(3, 'xiaoli'), (4, 'xiaoguo')]
#
#     results = index_tuple_get_equal_value('idx', (1,))
#     assert (list(results)) == [(1, 'xiaoming')]


def test_covered_index_tuple():
    results = covered_index_tuple_get_range('idx', (2,), (4,))
    assert (list(results)) == [(3,)]
    results = covered_index_tuple_get_equal_value('idx', (2,))
    assert (list(results)) == [(2,), (2,)]
    results = covered_index_tuple_get_range('idx', (2,))
    assert (list(results)) == [(3,), (4,)]



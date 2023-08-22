from imoocdb.common.fabric import TableColumn


def test_fabric():
    c1 = TableColumn('t1', 'a')
    assert str(c1) == 't1.a'
    c2 = TableColumn('t1', 'b')
    assert (c1 != c2)
    c3 = TableColumn('t1', 'a')
    assert (c1 == c3)


def test_pop_elements():
    rows = [1, 2, 3, 4, 5, 5, 5, 6]
    # 删除元素值是 4 和 5 的值
    i = 0
    while i < len(rows):
        if rows[i] == 4 or rows[i] == 5:
            rows.pop(i)
            i -= 1  # 是一些初级开发者，比较遗忘的地方
        i += 1
    assert rows == [1, 2, 3, 6]


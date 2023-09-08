from imoocdb.storage.bplus_tree import BPlusTree


def test_bplus_tree():
    tree = BPlusTree()
    for i in range(0, 100):
        tree.insert(i, i)

    for i in range(0, 100):
        assert tree.find(i) == [i]

    tree.insert(1, 100)
    tree.insert(2, 200)
    assert (tree.find(1)) == [1, 100]
    assert (tree.find(2)) == [2, 200]

    assert (tree.find_range(0, 3)) == [1, 100, 2, 200]
    expected = list(range(1, 100))
    expected.insert(1, 100)
    expected.insert(3, 200)
    assert (tree.find_range(start=0)) == expected
    assert len(tree.find_range()) == 100 + 2

    tree.delete(1)
    assert (tree.find(1)) == []
    tree.delete(3)
    assert (tree.find(3)) == []

    for i in range(0, 100):
        tree.insert(3, i)
    assert (len((tree.find(3)))) == 100
    result = tree.find(3)  # [0, ..., 99]
    result.sort()
    # print(result)
    assert result == list(range(0, 100))


test_bplus_tree()

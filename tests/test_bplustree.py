import os
from imoocdb.storage.bplus_tree import BPlusTree, BPlusTreeTuple


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


def test_bplus_tree_tuple():
    t1 = BPlusTreeTuple((None, 1, 2))
    t2 = BPlusTreeTuple((1, 1, 2))
    t3 = BPlusTreeTuple((2, 1, 2))
    t4 = BPlusTreeTuple((2, 0, 2))

    assert t1 == t1
    assert not t1 == t2
    assert t1 < t2
    assert t2 < t3
    assert t3 > t4
    assert not t3 <= t4
    assert not t3 < t4

    tree = BPlusTree()
    tree.insert(t1, (0, 1))
    tree.insert(t2, (0, 2))
    tree.insert(t3, (0, 3))
    tree.insert(t4, (0, 4))

    assert (tree.find_range()) == [(0, 1), (0, 2), (0, 4), (0, 3)]


def test_bplus_tree_serialize():
    filename = 'test.idx'
    if os.path.exists(filename):
        os.unlink(filename)

    tree = BPlusTree('test.idx')
    tree.insert(BPlusTreeTuple((None, 1)), (0, 1))
    tree.insert(BPlusTreeTuple((2, 1)), (2, 1))
    tree.insert(BPlusTreeTuple((None, 1)), (0, 2))

    assert (tree.find_range()) == [(0, 1), (0, 2), (2, 1)]

    tree.serialize()

    tree2 = BPlusTree.deserialize('test.idx')
    assert (tree2.find_range()) == [(0, 1), (0, 2), (2, 1)]

    # for i in range(100):
    #     tree2.insert(BPlusTreeTuple((None, 1)), (100, i))
    # print(tree2.find_range())

from imoocdb.storage.lru import LRUCache


def test_lru():
    lru = LRUCache(3)
    lru.put(1, 1)
    assert lru.get(1) == 1
    assert lru.get(2) is None
    lru.put(2, 2)
    lru.put(3, 3)
    lru.put(4, 4)
    assert lru.get(1) is None

    assert lru.get(2) == 2
    lru.put(5, 5)
    assert lru.get(3) is None
    assert lru.get(2) == 2
    assert lru.get(4) == 4
    assert lru.get(5) == 5

    assert str(lru.evicted) == '{1: 1, 3: 3}'
    # [2,4,5] <-
    lru.pin(2)
    lru.put(6, 6)
    assert lru.get(2) == 2
    assert lru.get(4) is None
    lru.unpin(2)
    lru.get(5)
    lru.get(6)
    lru.put(7, 7)
    assert lru.get(2) is None


test_lru()

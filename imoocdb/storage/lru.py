from imoocdb.errors import LRUError

# 理论上，当前数据库的内存使用，大约就应该是：
# PAGE_SIZE * LRU_CAPACITY + 小部分其他开销 = 数据库进程总使用内存
# innodb_buffer_pool_size, shared_buffers
LRU_CAPACITY = 100


class LRUNode:
    def __init__(self, key, value):
        self.key = key
        self.value = value  # 在LRU中就是PAGE
        self.prev = None
        self.next = None

        # 额外的字段
        # 用于判断当前 node 是否被上层业务代码使用
        # 如果被显性pinned, 那么就意味着，该节点暂时还不能剔除（淘汰）
        self.pinned = False

    def __repr__(self):
        return f'{self.key}:{self.value}'

    def __eq__(self, other):
        if not isinstance(other, LRUNode):
            return False
        return self.key == other.key

    def __hash__(self):
        return hash(self.key)


# todo: 当前LRU不是线程安全的，这在面对多个线程的时候，要加锁
class LRUCache:
    def __init__(self, capacity=LRU_CAPACITY):
        self.capacity = capacity
        self.cache = {}
        # 我们做一个小优化：
        # 先把被淘汰的数据暂存在这里 evicted
        self.evicted = {}

        # LRU中的链表, 这里面我们实现的是双向链表
        # dummy node 技巧!! 可以帮我们少写很多边界条件的判断逻辑
        self.head = LRUNode(None, None)
        self.tail = LRUNode(None, None)

        self.head.next = self.tail
        self.tail.prev = self.head

    def put(self, key, value):
        if key in self.cache:
            # 此时，相当于访问LRU中已经存在的一个节点
            # 需要把这个节点提取到最前面的位置
            self._remove(self.cache[key])

        node = LRUNode(key, value)
        self.cache[key] = node
        self._add(node)

        # 开始进行淘汰判断，即超过capacity上限，需要从头部head进行剔除
        if len(self.cache) > self.capacity:
            # 大于就要进行淘汰
            evicted_node = self.head.next
            while evicted_node and evicted_node.pinned:
                evicted_node = evicted_node.next

            if evicted_node is self.tail:
                # 删除刚刚插入的新node, 同时报错
                self._remove(node)
                del self.cache[key]
                raise LRUError('no available space for current node.')
            self._remove(evicted_node)
            del self.cache[evicted_node.key]
            self.evicted[evicted_node.key] = evicted_node.value

    def get(self, key):
        if key in self.cache:
            node = self.cache[key]
            # 下面一个删除 + 一个添加节点，就可以实现
            # 该节点的位置调整
            self._remove(node)
            self._add(node)
            return node.value
        return None

    def _add(self, node):
        # 新节点从尾部进行插入，旧节点从头部剔除
        prev_node = self.tail.prev
        prev_node.next = node
        node.prev = prev_node
        node.next = self.tail

        self.tail.prev = node

    @staticmethod
    def _remove(node: LRUNode):
        # 这就是为什么我们在LRU中必须实现双向链表的原因
        prev_node = node.prev
        next_node = node.next
        prev_node.next = next_node
        next_node.prev = prev_node

    def pin(self, key):
        if key not in self.cache:
            raise LRUError(f'not found key {key}')
        self.cache[key].pinned = True

    def unpin(self, key):
        if key not in self.cache:
            raise LRUError(f'not found key {key}')
        self.cache[key].pinned = False

    def items(self):
        # 思考：如果想遍历当前LRU中的所有元素，应该
        # 怎么遍历呢？
        pass


class BufferPool:
    def __init__(self, buffer_size=LRU_CAPACITY):
        self.lru_cache = LRUCache(buffer_size)
        self.dirty_pages = set()

    def mark_dirty(self, key):
        assert key in self.lru_cache.cache
        self.dirty_pages.add(key)

    def unmark_dirty(self, key):
        self.dirty_pages.remove(key)

    def get_all_dirty_pages(self):
        for key in sorted(self.dirty_pages):
            if key in self.lru_cache.cache:
                yield key, self.lru_cache.get(key)
            elif key in self.lru_cache.evicted:
                yield key, self.lru_cache.evicted[key]
            else:
                assert False

    def find_max_pageno(self, relation_name):
        result = list(sorted(filter(lambda t: t[0] == relation_name, self.dirty_pages)))
        if len(result) == 0:
            return -1
        return result[-1][1]

    def __getitem__(self, item):
        return self.lru_cache.get(item)

    def __setitem__(self, key, value):
        return self.lru_cache.put(key, value)

    def __contains__(self, item):
        return item in self.lru_cache.cache


buffer_pool = BufferPool()

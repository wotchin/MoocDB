import pickle
import os
import math

from imoocdb.errors import BPlusTreeError
from imoocdb.storage.slotted_page import Page, LITTLE_ORDER, PAGE_SIZE


class BPlusTreeTuple:
    def __init__(self, tup):
        assert isinstance(tup, tuple)
        self.tup = tup

    @staticmethod
    def _cmp(a, b):
        # a - b
        if a is None and b is None:
            return 0
        if a is None:
            return -1
        if b is None:
            return 1

        if a == b:
            return 0
        elif a < b:
            return -1
        else:
            return 1

    def __repr__(self):
        return str(self.tup)

    def __eq__(self, other):
        if isinstance(other, BPlusTreeTuple):
            tup = other.tup
        elif isinstance(other, tuple):
            tup = other
        else:
            return False
        return self.tup == tup

    def __lt__(self, other):
        # self < other, less than
        if isinstance(other, BPlusTreeTuple):
            tup = other.tup
        elif isinstance(other, tuple):
            tup = other
        elif isinstance(other, float):
            # 不能直接 float('inf') == float('inf')
            if math.inf == other:
                return True
            elif - math.inf == other:
                return False
            else:
                raise
        else:
            raise

        assert len(self.tup) == len(tup)

        for i in range(len(self.tup)):
            r = self._cmp(self.tup[i], tup[i])
            if r == 0:
                continue
            elif r < 0:
                return True
            elif r > 0:
                return False

        # 兜底的，但是不太可能会跑到着
        return False

    def __le__(self, other):
        if self == other:
            return True
        return self < other

    def __ge__(self, other):
        # >=
        # not <
        if self == other:
            return True
        return not self < other

    def __gt__(self, other):
        # self > other
        # >
        # not <=
        if self == other:
            return False
        return not self < other


# todo: 把这个node可以序列化为slotted page的字节集 bytes
# todo: LSN的实现 -> 被修改的节点，需要显性标记一下LSN号
class BPlusTreeNode:
    def __init__(self, is_leaf=True):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        # 只有叶子节点才有value
        self.values = []
        # 如果我们通过指针进行回溯，那么我们应该有下述字段：
        # self.parent
        # 用于从磁盘中定位数据
        self.loaded = False
        self.pageno = 0xffffffff  # 给一个初始值，不合法的值

        self.next_leaf = None
        self.lsn = 0

    def get_child(self, i):
        # keys ->          [1, 3, 5]
        # children -> [>1, 1-3, 3-5, >=5]
        if i >= len(self.children):
            return self.next_leaf
        return self.children[i]

    def count_children(self):
        if self.next_leaf:
            return len(self.children) + 1
        return len(self.children)

    def to_page(self):
        # 也就是序列化过程的一部分，因为Page本身自带序列化的方法
        page = Page()
        page.page_header.flags = 1 if self.is_leaf else 0
        # page.page_header.lsn = self.lsn

        if self.is_leaf:
            page.page_header.reserved = self.next_leaf.pageno if self.next_leaf else 0xffffffff
            for k, v in zip(self.keys, self.values):
                page.insert(pickle.dumps((k, v)))
        else:
            for i in range(len(self.keys)):
                k = self.keys[i]
                v = self.children[i].pageno
                page.insert(pickle.dumps((k, v)))
            # 打补丁，因为 children 是空隙，应该是 keys + 1
            if len(self.children) > len(self.keys):
                v = self.children[-1].pageno
                page.insert(pickle.dumps((None, v)))

        page.set_header(self.lsn)
        return page

    def from_page(self, page):
        # 用来把 page 中的数据，反解析一下（反序列化），用于赋值到
        # 当前的 node 上
        self.loaded = True
        self.is_leaf = page.page_header.flags == 1
        self.lsn = page.page_header.lsn

        if self.is_leaf:
            if page.page_header.reserved < 0xffffffff:
                self.next_leaf = BPlusTreeNode()
                self.next_leaf.pageno = page.page_header.reserved

            for sid in range(len(page.slot_directory)):
                k, v = pickle.loads(page.select(sid))
                self.keys.append(k)
                self.values.append(v)
        else:
            for sid in range(len(page.slot_directory)):
                k, v = pickle.loads(page.select(sid))
                if k is None:
                    node = BPlusTreeNode()
                    node.pageno = v
                    self.children.append(node)
                else:
                    self.keys.append(k)
                    node = BPlusTreeNode()
                    node.pageno = v
                    self.children.append(node)

    def __eq__(self, other):
        if not isinstance(other, BPlusTreeNode):
            return False
        return self.pageno == other.pageno

    def __hash__(self):
        return self.pageno


HEADER_SIZE = 8  # 64bit int


def load_page_from_disk(filename, pageno):
    with open(filename, 'rb') as f:
        f.seek(HEADER_SIZE + pageno * PAGE_SIZE)
        # 标志着读的数据是 [pageno, pageno + 1)
        buff = f.read(PAGE_SIZE)
        page = Page.deserialize(buff)
    return page


def load_root_node(filename):
    if not os.path.exists(filename):
        raise BPlusTreeError(f'not found the file {filename}.')
    with open(filename, 'rb') as f:
        buff = f.read(HEADER_SIZE)
        root_node_pageno = int.from_bytes(buff, LITTLE_ORDER)
        assert root_node_pageno >= 0
    page = load_page_from_disk(filename, root_node_pageno)
    node = BPlusTreeNode()
    node.from_page(page)
    # 下面这个字段，很容易遗忘！
    node.pageno = root_node_pageno
    return node


def count_pages(filename):
    with open(filename, 'rb') as f:
        file_size = os.fstat(f.fileno()).st_size

    assert (file_size - HEADER_SIZE) % PAGE_SIZE == 0
    return (file_size - HEADER_SIZE) // PAGE_SIZE


class BPlusTree:
    def __init__(self, filename=None, root_node=None):
        if root_node is None:
            # 是一个新的b+树，也就是create index 过程
            self.node_count = 0
            self.root = self.allocate_node(is_leaf=True)
        else:
            # 由于走到这个分支的b+树，不是新的b+树，因此，我们
            # 需要从磁盘里的文件大小进行计算
            self.root = root_node
            self.node_count = count_pages(filename)

        self.filename = filename

    def allocate_node(self, is_leaf):
        node = BPlusTreeNode(is_leaf)
        node.pageno = self.node_count
        node.loaded = True
        self.node_count += 1
        return node

    def insert(self, key, value):
        if key is None:
            raise BPlusTreeError('invalid key')

        # 直接插入叶子节点中
        node = self.find_leaf_node(key)
        # 正因为，调用了下面的函数，我们可以保证，插入过程是
        # 有序的，因为该函数，寻找的是最右边的相同的key的下标，
        # 如果没有找到 0
        index = self._find_rightmost_key_index(node, key)
        node.keys.insert(index, key)
        node.values.insert(index, value)

        # 分裂，也就是不断递归，向父节点插入元素的过程
        if self._need_split(node):
            self._split(node)

    def _split(self, node):
        """用于调整B+树的结构，用于做节点的分裂"""
        middle_index = len(node.keys) // 2
        # 把当前的 node 节点，拆分成相等元素的两个节点
        # 这块在工程上可能有不同的发挥和改良
        right_node = self.allocate_node(is_leaf=node.is_leaf)
        left_node = node

        # 拆分完之后，就要导数据
        # 新节点就是右节点，原来的旧节点就是左节点
        # 我们这里面之所以复用原来的节点，是因为传入的参数是一个引用（指针）
        # 如果直接用新的节点进行替换，出现找不到节点的问题
        right_node.keys.extend(node.keys[middle_index:])
        right_node.children.extend(node.children[middle_index:])
        right_node.values.extend(node.values[middle_index:])
        right_node.next_leaf = node.next_leaf

        left_node.keys = node.keys[:middle_index]
        left_node.children = node.children[:middle_index]
        left_node.values = node.values[:middle_index]
        left_node.next_leaf = right_node

        assert len(left_node.keys) > 0 and len(right_node.keys) > 0
        assert tuple(left_node.keys) < tuple(right_node.keys)

        if node is self.root:
            new_root = self.allocate_node(is_leaf=False)
            new_root.keys.append(right_node.keys[0])
            new_root.children.extend([left_node, right_node])
            self.root = new_root
        else:
            parent = self._find_parent(self.root, node)
            index = parent.children.index(node)
            parent.keys.insert(index, right_node.keys[0])
            # parent.children[index] = left_node
            parent.children.insert(index + 1, right_node)

            if self._need_split(parent):
                self._split(parent)

    @staticmethod
    def _need_split(node):
        # 这里是随便写的，意思是，B+树节点中的元素数量，如果大于这个值
        # 那么就分裂
        return len(node.keys) > 10

    @staticmethod
    def _find_rightmost_key_index(node, key):
        # 假如：
        # node.keys: [ 1,   3,  10,      100]
        #            /    |   |      \         \
        #  [ -1, 0 ]     [2] [7]   [11, 15, 99] [101]   -> children

        # 寻找小于等于key的最大下标 i
        i = 0
        while i < len(node.keys):
            if node.keys[i] <= key:
                i += 1
            else:
                return i
        return i

    @staticmethod
    def _find_leftmost_key_index(node, key):
        # 寻找 key <= node.keys 中最小值时的下标 i
        # 他有更高效实现方法：
        # 二分查找，但是二分查找，不能解决相同元素的情况，例如：
        # 元素值   [1,2,2,2,3,3,4,5,6]
        # 元素下标 [0,1,2,3,4,5,6,7,8]
        # leetcode 的算法题：寻找二分搜索的最大/小值
        i = 0
        while i < len(node.keys):
            if key <= node.keys[i]:
                return i
            i += 1
        return len(node.keys)

    def _find_parent(self, current, target):
        if current.is_leaf or target in current.children:
            return current

        for child in current.children:
            if target in child.children:
                return self._find_parent(child, target)
        return None

    def delete(self, key, value=None):
        node = self.find_leaf_node(key)
        while node:
            indexes = list(self._find_indexes(node.keys, key))
            if len(indexes) == 0:
                break
            # 如何去解决删除一个元素后，对应的下标移位的问题
            i = 0
            deleted_count = 0
            while i < len(indexes):
                index = indexes[i]
                actual_index = index - deleted_count
                # 跳过 value 不等于参数的 key
                if value is not None:
                    if node.values[actual_index] != value:
                        i += 1
                        continue
                node.keys.pop(actual_index)
                node.values.pop(actual_index)
                i += 1
                deleted_count += 1

            node = node.next_leaf
            node = self.load_node(node)
        # 准确来说，此时还应该补充一个合并机制(coalesce)，即把小于 n/2 的
        # node进行合并

    def find(self, key):
        values = []
        node = self.find_leaf_node(key)
        while node:
            indexes = list(self._find_indexes(node.keys, key))
            if len(indexes) == 0:
                break
            for index in indexes:
                values.append(node.values[index])
            node = node.next_leaf
            node = self.load_node(node)
        return values

    def find_range(self, start=float('-inf'), end=float('inf'), return_keys=False):
        # select * from t1 where a > 100;
        # 不包含等值
        # 因为，对于等值，可以直接补充等值查询即可
        # 算法复杂度没有新增多少
        values = []
        node = self.find_leaf_node(start)
        while node:
            for index, key in enumerate(node.keys):
                # 提前退出
                if key >= end:
                    break
                # 如果我们不在上面指定trick start=-inf, ...
                # 那么我们就要判断 start/end 是否为 None
                if start < key < end:
                    if return_keys:
                        values.append(key)
                    else:
                        values.append(node.values[index])
            node = node.next_leaf
            node = self.load_node(node)
        return values

    def find_leaf_node(self, key):
        """寻找最左边的叶子节点（我们B+树是按照从小到大组织数据的）"""
        node = self.root
        while node and not node.is_leaf:
            node = self.load_node(node)
            index = self._find_leftmost_key_index(node, key)
            # 如果没有这样的 index, 则 node 为最后一个子节点
            if index >= len(node.keys):
                node = node.children[-1]
            # 对于唯一key，应该使用下述方法，但是不唯一的key
            # 会很麻烦，不能直接使用下述代码
            # elif key == node.keys[index]:
            #     node = node.get_child(index + 1)
            else:
                node = node.get_child(index)

        while node.keys and node.keys[-1] < key and node.next_leaf:
            node = node.next_leaf
            node = self.load_node(node)
        return node

    @staticmethod
    def _find_indexes(keys, key):
        for i, k in enumerate(keys):
            if key == k:
                yield i
            # 这样可以做个剪枝
            elif k > key:
                break

    def load_node(self, node: BPlusTreeNode):
        if node is None:
            return None
        if node.loaded:
            return node

        # 开始真正加载数据
        page = load_page_from_disk(self.filename, node.pageno)
        node.from_page(page)
        return node

    def serialize(self):
        assert self.filename

        # 遍历树的节点
        nodes = []
        queue = [self.root]
        while len(queue) > 0:
            node = queue.pop(0)  # 拿第一个节点
            # 等价于for 遍历children
            if not node.is_leaf:
                queue.extend(node.children)

            nodes.append(node)

        nodes.sort(key=lambda n: n.pageno)

        # 校验所有node都有pageno，且赋值正确
        for i in range(len(nodes)):
            assert i == nodes[i].pageno

        with open(self.filename, 'w+b') as f:
            root_node_pageno = self.root.pageno
            f.write(
                int.to_bytes(root_node_pageno,
                             HEADER_SIZE,
                             LITTLE_ORDER,
                             signed=False)
            )
            for node in nodes:
                f.write(node.to_page().serialize())
            # 是一个系统调用，确保文件能够刷到磁盘里
            # 如果没有刷进去，就一直等着 blocking
            os.fsync(f.fileno())

    @staticmethod
    def deserialize(filename):
        # 做一个判断
        with open(filename, 'rb') as f:
            file_size = os.fstat(f.fileno()).st_size
            assert (file_size - HEADER_SIZE) % PAGE_SIZE == 0
        return BPlusTree(filename, load_root_node(filename))

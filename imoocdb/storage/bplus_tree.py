from imoocdb.errors import BPlusTreeError


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


class BPlusTree:
    def __init__(self):
        self.root = BPlusTreeNode(is_leaf=True)
        # 暂时用 disk 变量来表示磁盘，后面我们会具体来实现这部分
        self.disk = {}

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
        right_node = BPlusTreeNode(is_leaf=node.is_leaf)
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
            new_root = BPlusTreeNode(is_leaf=False)
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

    def delete(self, key):
        node = self.find_leaf_node(key)
        while node:
            indexes = list(self._find_indexes(node.keys, key))
            if len(indexes) == 0:
                break
            # 如何去解决删除一个元素后，对应的下标移位的问题
            i = 0
            while i < len(indexes):
                index = indexes[i]
                node.keys.pop(index - i)
                node.values.pop(index - i)
                i += 1

            node = node.next_leaf
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
        return values

    def find_range(self, start=float('-inf'), end=float('inf')):
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
                    values.append(node.values[index])
            node = node.next_leaf
        return values

    def find_leaf_node(self, key):
        """寻找最左边的叶子节点（我们B+树是按照从小到大组织数据的）"""
        node = self.root
        while node and not node.is_leaf:
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
        return node

    @staticmethod
    def _find_indexes(keys, key):
        for i, k in enumerate(keys):
            if key == k:
                yield i
            # 这样可以做个剪枝
            elif k > key:
                break

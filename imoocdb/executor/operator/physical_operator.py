import os
import pickle

from imoocdb.storage.entry import (table_tuple_get_all,
                                   index_tuple_get_range,
                                   index_tuple_get_equal_value,
                                   covered_index_tuple_get_range,
                                   covered_index_tuple_get_equal_value,
                                   )
from imoocdb.sql.logical_operator import Condition
from imoocdb.common.fabric import TableColumn
from imoocdb.catalog.entry import catalog_table, catalog_index, catalog_function
from imoocdb.errors import ExecutorCheckError
from imoocdb.constant import TEMP_DIRECTORY
from imoocdb.session_manager import get_current_session_id


def is_condition_true(values: dict, condition):
    left = condition.left if not isinstance(condition.left, TableColumn) else \
        values[condition.left]
    right = condition.right if not isinstance(condition.right, TableColumn) else \
        values[condition.right]
    if condition.sign == '>':
        return left > right
    elif condition.sign == '<':
        return left < right
    elif condition.sign == '=':
        return left == right
    elif condition.sign == '!=':
        return left != right
    else:
        raise NotImplementedError()


class PhysicalOperator:
    def __init__(self, name):
        self.name = name  # 算子名

        # 如果我们采用CBO的内置公式进行优化，
        # 那么，我们需要记录该算子的 cost 值
        # notice:
        # 由于我们课程中涉及到的SQL语句都很简单，RBO已经非常足够使用了，
        # 就足以产生比较优的执行计划，CBO在这里基本属于多余，因此，
        # 这个公式值可能不一定能用到
        self.cost = 0

        # 该算子的子节点，即该算子的数据来源
        # Java里面来写的话，这个 field/attr 应该被声明称 private 的
        self.children = []

        # 输出的结构: 用于给上层的算子提供输入
        # todo: 该 columns 是 list 还是 dict 我们后面要思考，要权衡
        self.columns = None

    def open(self):
        """用途：初始化该执行算子，例如提前缓存一些数据，提前获取数据库内部
        的某些状态信息，提前申请一些变量等"""
        raise NotImplementedError()

    def close(self):
        """用途：用于清理 open 方法创建出来的临时数据，关闭一些资源等"""
        raise NotImplementedError()

    def next(self):
        """用途：用于获取一行数据（tuple, record, row, et al）
        数据库的执行过程，是一个迭代器的遍历过程，所以，要用 next() 方法进行抽象，
        用于表示这个迭代过程。

        例如，下面的例子，展示了一个迭代器的迭代过程：
        ```
        children = [Iterator1, Iterator2]
        results = []
        for child in children:
            if not child.has_next():
                continue
            tuple = child.next()
            returns.append(process(tuple))

        return results
        ```
        """
        raise NotImplementedError()

    def add_child(self, operator):
        # 相比于直接在外面操作 children 列表的好处是：
        # 1. 可以面向抽象的接口进行编程，避免面向具体的实现进行编程
        # 2. 可以避免霰弹式修改
        # 3. ...
        assert isinstance(operator, PhysicalOperator)
        self.children.append(operator)


class TableScan(PhysicalOperator):
    def __init__(self, table_name, condition: Condition = None):
        super().__init__('TableScan')
        self.table_name = table_name
        self.condition = condition
        self.columns = None

    def open(self):
        # 表采集到的 columns 要跟这个采集到的 tuple 元素下标一一对应上
        self.columns = []
        for column in catalog_table.select(
                lambda r: r.table_name == self.table_name)[0].columns:
            self.columns.append(TableColumn(self.table_name, column))

    def close(self):
        pass

    def next(self):
        for tup in table_tuple_get_all(self.table_name):
            if not self.condition:
                yield tup
            else:
                # 案例：
                # 表结构 t1 (id, name)
                # 获取到的元组是 (1, 'xiaoming')
                # 则，我们可以构造出 values 为 {TableColumn(t1, id): 1,
                #                             TableColumn(t1, name): 'xiaoming'}
                values = {k: tup[i] for i, k in enumerate(self.columns)}
                if is_condition_true(values, self.condition):
                    yield tup


class IndexScan(PhysicalOperator):
    def __init__(self, index_name, condition=None):
        super().__init__('IndexScan')
        self.index_name = index_name
        self.condition = condition
        self.condition_column = None
        self.constant = None
        self.tuple_get_equal_value = index_tuple_get_equal_value
        self.tuple_get_range = index_tuple_get_range

    def open(self):
        constants = []
        columns = []
        for node in (self.condition.left, self.condition.right):
            if isinstance(node, TableColumn):
                columns.append(node)
            else:
                constants.append(node)
        if not (constants and columns):
            raise ExecutorCheckError('bad scan condition')

        if len(columns) != 1 or len(constants) != 1:
            raise ExecutorCheckError('only supported one condition/value column.')
        self.condition_column = columns[0]
        self.constant = constants[0]
        self.fill_in_columns()

    def fill_in_columns(self):
        # 采集上来的元组tuple结构
        self.columns = []
        table_name = catalog_index.select(
            lambda r: r.index_name == self.index_name
        )[0].table_name
        for column in catalog_table.select(
                lambda r: r.table_name == table_name)[0].columns:
            self.columns.append(TableColumn(table_name, column))

    def close(self):
        pass

    def next(self):
        if not self.condition:
            raise NotImplementedError()
        elif self.condition.sign == '=':
            for tup in self.tuple_get_equal_value(
                    self.index_name, equal_value=(self.constant,)
            ):
                yield tup
        elif self.condition.sign == '>':
            # eg, ... where t1.a > 100
            # 等价于 ... where 100 < t1.a
            start = end = None
            if isinstance(self.condition.left, TableColumn):
                start = (self.constant,)
            else:
                end = (self.constant,)

            for tup in self.tuple_get_range(
                    self.index_name, start=start, end=end
            ):
                yield tup
        elif self.condition.sign == '<':
            # eg, ... t1.a < 100
            start = end = None
            if isinstance(self.condition.left, TableColumn):
                end = (self.constant,)
            else:
                start = (self.constant,)
            for tup in self.tuple_get_range(
                    self.index_name, start=start, end=end
            ):
                yield tup
        else:
            raise NotImplementedError(
                f'not supported operation {self.condition.sign} for {self.name}'
            )


class CoveredIndexScan(IndexScan):
    def __init__(self, index_name, condition=None):
        super().__init__(index_name, condition)
        self.name = 'CoveredIndexScan'
        self.tuple_get_equal_value = covered_index_tuple_get_equal_value
        self.tuple_get_range = covered_index_tuple_get_range

    def fill_in_columns(self):
        # 采集上来的元组tuple结构，就是index的column结构
        self.columns = []
        table_name = catalog_index.select(
            lambda r: r.index_name == self.index_name
        )[0].table_name

        for column in catalog_index.select(
                lambda r: r.index_name == self.index_name
        )[0].columns:
            self.columns.append(TableColumn(table_name, column))


class Materialize(PhysicalOperator):
    def __init__(self, name):
        super().__init__(name)
        # 物化的二维数组（list of tuple）:
        self.tuples = []

    def open(self):
        if len(self.children) != 1:
            raise ExecutorCheckError(
                f'{self.name} operator only supports one child operator.'
            )

        child = self.children[0]
        child.open()
        self.columns = child.columns  # 这里是一个引用，不是一个copy

    def close(self):
        child = self.children[0]
        child.close()

    def materialize(self):
        child = self.children[0]
        # 或者
        # for child self.children:
        # 这种for 循环的表现力可能会好点

        # 下面做一下元组的物化过程
        for tup in child.next():
            self.tuples.append(tup)

    def next(self):
        pass


class Sort(Materialize):
    INTERNAL_SORT = 'internal sort'
    EXTERNAL_SORT = 'external sort'
    HEAP_SORT = 'heap sort'  # 暂时先不实现，只是一个替换排序算法的过程

    def __init__(self, sort_column, asc=True):
        super().__init__('Sort')
        self.sort_column = sort_column  # TableColumn 对象
        self.sort_column_index = None  # sort column 在tuple这种的下标位置

        assert isinstance(self.sort_column, TableColumn)
        self.asc = asc
        self.method = self.INTERNAL_SORT  # 默认内排序

    def open(self):
        super().open()
        self.sort_column_index = self.columns.index(self.sort_column)

    def internal_sort(self):
        # Python 自带了 sort 排序算法，用到的就是快速排序
        # 如果想手写，则直接从PPT材料中获取就行，相关材料太多，就不在出浪费大家的时间，
        # 此处，简单调用即可
        self.tuples.sort(key=lambda t: t[self.sort_column_index],
                         reverse=(not self.asc))
        for tup in self.tuples:
            yield tup

    def external_sort(self):
        max_part_size = 2
        chunks = [
            self.tuples[i: i + max_part_size] for i in range(
                0, len(self.tuples), max_part_size)
        ]

        # 下面，我们要进行外排序，即把每个chunk分别排序，部分结果要放到磁盘中进行缓存
        if not os.path.exists(TEMP_DIRECTORY):
            os.mkdir(TEMP_DIRECTORY)

        temp_files = []
        for i, chunk in enumerate(chunks):
            chunk.sort(
                key=lambda t: t[self.sort_column_index],
                reverse=(not self.asc))
            temp_file = os.path.join(TEMP_DIRECTORY,
                                     f'temp_sort_{get_current_session_id()}_{i}')
            with open(temp_file, 'wb') as f:
                for item in chunk:
                    f.write(pickle.dumps(item) + b'\n')
            temp_files.append(temp_file)

        # 接下来，我们处理合并过程 merge
        file_fds = [open(temp_file, 'rb') for temp_file in temp_files]

        # 此时，我们来获取来自每个chunk的第一个元素，然后把他们进行排序
        first_items = []
        # 补偿机制
        file_fd_index = {}
        for i, file_fd in enumerate(file_fds):
            # 序列化，python自带的
            item = pickle.loads(file_fd.readline())
            first_items.append(item)

            # 作用：保证，当前 first_items 中每个元素都尽可能从所有chunk中获取
            # 有了该 file_fd_index 之后，我们就知道当前已经排好序的item是从哪个 fd 中获取的了
            # 相当于一种反查机制
            if item not in file_fd_index:
                file_fd_index[item] = []
            file_fd_index[item].append(i)

        # merge them
        first_items.sort(key=lambda t: t[self.sort_column_index],
                         reverse=(not self.asc))
        while len(first_items) > 0:
            item = first_items.pop(0)
            i = file_fd_index[item].pop(0)
            if len(file_fd_index[item]) == 0:
                del file_fd_index[item]  # 对GC友好一点，也不是必须的
            yield item
            next_item = file_fds[i].readline()
            if not next_item:
                continue
            # 反序列化
            next_item = pickle.loads(next_item)
            first_items.append(next_item)
            if next_item not in file_fd_index:
                file_fd_index[next_item] = []
            file_fd_index[next_item].append(i)

        for file_fd in file_fds:
            file_fd.close()
            os.unlink(file_fd.name)

    def next(self):
        self.materialize()

        if self.method == self.INTERNAL_SORT:
            for tup in self.internal_sort():
                yield tup
        elif self.method == self.EXTERNAL_SORT:
            for tup in self.external_sort():
                yield tup
        else:
            raise NotImplementedError(f'not supported {self.method} yet.')


class HashAgg(Materialize):
    def __init__(self, group_by_column, aggregate_function_name, aggregate_column):
        super().__init__('HashAgg')
        self.group_by_column = group_by_column
        self.aggregate_function_name = aggregate_function_name
        self.aggregate_column = aggregate_column

        assert isinstance(self.group_by_column, TableColumn)
        assert isinstance(self.aggregate_column, TableColumn)

        self.group_by_column_idx = None
        self.aggregate_column_idx = None

        # having 子句，由于我们没有实现这个语法，所以此时不用过滤
        # 如果实现了having，那么就是 self.having_condition = Condition(...)

        # 额外的信息：

    # override
    def open(self):
        super().open()

        self.group_by_column_idx = self.columns.index(self.group_by_column)
        self.aggregate_column_idx = self.columns.index(self.aggregate_column)

        self.columns = (self.group_by_column, self.aggregate_column)

    @staticmethod
    def _aggregate_function(name):
        results = catalog_function.select(
            lambda r: r.function_name == name and r.agg_function)
        if len(results) != 1:
            raise ExecutorCheckError(f'not found the aggregation function {name}.')
        return results[0].callback

    def next(self):
        self.materialize()

        # 第一步，先做hash过程
        # 该哈希表的 key 和 value 形式，大家要记住
        hash_table = {}
        for tup in self.tuples:
            # Python 有更高级的数据类型，如NamedTuple, 此时是最简单粗暴的写法
            # key 和 value 分别哪里来的，大家要记住，分别是来自哪个列的！
            key = tup[self.group_by_column_idx]
            value = tup[self.aggregate_column_idx]

            # 往哈希表里放数据
            if key not in hash_table:
                hash_table[key] = [value]
            else:
                hash_table[key].append(value)

        # 哈希表构造完毕，接下来对哈希表内部的value进行聚合
        for key, values in hash_table.items():
            aggregated_value = self._aggregate_function(
                self.aggregate_function_name)(values)
            yield (key, aggregated_value)

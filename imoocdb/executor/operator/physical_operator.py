import os
import pickle
import time

import instr
from imoocdb.catalog import CatalogTableForm, CatalogIndexForm
from imoocdb.catalog.entry import catalog_table, catalog_index, catalog_function
from imoocdb.common.fabric import TableColumn
from imoocdb.constant import TEMP_DIRECTORY
from imoocdb.errors import ExecutorCheckError, RollbackError
from imoocdb.session_manager import get_current_session_id
from imoocdb.sql.logical_operator import *
from imoocdb.sql.utils import table_exists, column_exists
from imoocdb.sql.parser.ast import JoinType, CreateTable, CreateIndex
from imoocdb.storage.entry import (table_tuple_get_all,
                                   table_tuple_insert_one,
                                   covered_index_tuple_get_range,
                                   covered_index_tuple_get_equal_value,
                                   index_tuple_insert_one,
                                   index_tuple_delete_one,
                                   index_tuple_update_one,
                                   index_tuple_get_equal_value_locations, index_tuple_get_range_locations,
                                   table_tuple_get_one, table_tuple_get_all_locations, table_tuple_update_one,
                                   table_tuple_delete_multiple, index_tuple_create)
from imoocdb.storage.lock.lock import lock_manager
from imoocdb.storage.transaction.entry import checkpoint, transaction_mgr


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


def cast_tuple_pair_to_values(columns, tup):
    rv = {}
    for k, v in zip(columns, tup):
        rv[k] = v
    return rv


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

        xid = transaction_mgr.session_xid()
        lock_manager.acquire_lock(('table', self.table_name), xid, 's')

    def close(self):
        xid = transaction_mgr.session_xid()
        lock_manager.release_lock(('table', self.table_name), xid)

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
                # values = {k: tup[i] for i, k in enumerate(self.columns)}, 等价于
                values = cast_tuple_pair_to_values(self.columns, tup)
                if is_condition_true(values, self.condition):
                    yield tup

    def next_location(self):
        for location in table_tuple_get_all_locations(self.table_name):
            tup = table_tuple_get_one(self.table_name, location)
            if not self.condition:
                yield location
            else:
                values = cast_tuple_pair_to_values(self.columns, tup)
                if is_condition_true(values, self.condition):
                    yield location


class IndexScan(PhysicalOperator):
    def __init__(self, index_name, condition=None):
        super().__init__('IndexScan')
        self.index_name = index_name
        self.table_name = None  # 该索引涉及到的表名
        self.condition = condition
        self.condition_column = None
        self.constant = None
        self.tuple_get_equal_value = index_tuple_get_equal_value_locations
        self.tuple_get_range = index_tuple_get_range_locations

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

        xid = transaction_mgr.session_xid()
        lock_manager.acquire_lock(('index', self.index_name), xid, 's')

    def fill_in_columns(self):
        # 采集上来的元组tuple结构
        self.columns = []
        self.table_name = catalog_index.select(
            lambda r: r.index_name == self.index_name
        )[0].table_name
        for column in catalog_table.select(
                lambda r: r.table_name == self.table_name)[0].columns:
            self.columns.append(TableColumn(self.table_name, column))

    def close(self):
        xid = transaction_mgr.session_xid()
        lock_manager.release_lock(('index', self.index_name), xid)

    def next_location(self):
        if not self.condition:
            raise NotImplementedError()
        elif self.condition.sign == '=':
            for location in self.tuple_get_equal_value(
                    self.index_name, equal_value=(self.constant,)
            ):
                yield location
        elif self.condition.sign == '>':
            # eg, ... where t1.a > 100
            # 等价于 ... where 100 < t1.a
            start = float('-inf')
            end = float('inf')
            if isinstance(self.condition.left, TableColumn):
                start = (self.constant,)
            else:
                end = (self.constant,)

            for location in self.tuple_get_range(
                    self.index_name, start=start, end=end
            ):
                yield location
        elif self.condition.sign == '<':
            # eg, ... t1.a < 100
            start = float('-inf')
            end = float('inf')
            if isinstance(self.condition.left, TableColumn):
                end = (self.constant,)
            else:
                start = (self.constant,)
            for location in self.tuple_get_range(
                    self.index_name, start=start, end=end
            ):
                yield location
        else:
            raise NotImplementedError(
                f'not supported operation {self.condition.sign} for {self.name}'
            )

    def next(self):
        # next_location() 类似于取指针/引用
        # next() 相当于解引用 *p , 或者直接返回具体的值
        for location in self.next_location():
            yield table_tuple_get_one(self.table_name, location)


class CoveredIndexScan(IndexScan):
    def __init__(self, index_name, condition=None):
        super().__init__(index_name, condition)
        self.name = 'CoveredIndexScan'
        self.tuple_get_equal_value = covered_index_tuple_get_equal_value
        self.tuple_get_range = covered_index_tuple_get_range

    def next(self):
        # 覆盖索引扫描的返回的就是key, 可以直接返回给上层算子了
        for key in self.next_location():
            yield key


class LocationScan(PhysicalOperator):
    def __init__(self, scan):
        super().__init__('LocationScan')
        # 该类是个代理类，真正执行扫描动作的是下面的 real_scan 算子
        # real_scan 算子可以是 IndexScan/TableScan, 但是调用的不是它们的
        # next() 方法，而是 next_location() 方法。
        assert isinstance(scan, IndexScan) or isinstance(scan, TableScan)
        self.real_scan = scan

    def open(self):
        self.columns = []  # nothing to return
        self.real_scan.open()

    def close(self):
        self.real_scan.close()

    def next(self):
        locations = []
        for location in self.real_scan.next_location():
            locations.append(location)
        # 这里缓存一下，是为了避免被删除元素之后，由于 next_location() 是动态
        # 获取下标的，tuple 下标会发生位置变换（例如 -1），
        # 导致后续扫描失败
        for location in locations:
            yield location


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


class NestedLoopJoin(PhysicalOperator):
    def __init__(self, join_type: str, left_table_name, right_table_name, join_condition):
        super().__init__('NestedLoopJoin')
        self.join_type = join_type
        self.left_table_name = left_table_name
        self.right_table_name = right_table_name
        self.join_condition = join_condition

    def open(self):
        assert len(self.children) == 2
        for child in self.children:
            child.open()

        self.columns = self.left_table.columns + self.right_table.columns

    def close(self):
        for child in self.children:
            child.close()

    @property
    def left_table(self):
        assert self.children[0].columns[0].table_name == self.left_table_name
        # 在我们的代码里，只支持两个表join，我们的Join算子的子节点，只能是 scan
        # 等价于：
        # assert self.children[0].table_name == self.left_table_name
        return self.children[0]

    @property
    def right_table(self):
        assert self.children[1].columns[1].table_name == self.right_table_name
        return self.children[1]

    def cross_join(self):
        # 我们前面说过，笛卡尔积就是cross join, 就是两个简单的，没有判断条件的
        # for 循环，不考虑其他I/O代价，索引代价等，其算法复杂度是 O(M * N)
        for left_tuple in self.left_table.next():  # 在循环外面的是外表
            for right_tuple in self.right_table.next():  # 里面的是内表
                yield left_tuple + right_tuple

    def inner_join(self):
        for tup in self.cross_join():
            values = cast_tuple_pair_to_values(self.columns, tup)
            if is_condition_true(values, self.join_condition):
                yield tup

    def outer_join(self, outer_table, inner_table, exchange=False):
        # None 用来表示 null
        # 没有 exchange 之前，outer_table 就是 left_table
        if not exchange:
            padding_nulls = tuple(None for _ in range(len(inner_table.columns)))
        else:
            padding_nulls = tuple(None for _ in range(len(outer_table.columns)))

        for outer_tuple in outer_table.next():
            matching_tuples = []
            for inner_tuple in inner_table.next():
                if not exchange:
                    joined_tuple = outer_tuple + inner_tuple
                else:
                    joined_tuple = inner_tuple + outer_tuple
                values = cast_tuple_pair_to_values(self.columns, joined_tuple)
                if is_condition_true(values, self.join_condition):
                    matching_tuples.append(joined_tuple)

            if not matching_tuples:
                if not exchange:
                    matching_tuples.append(outer_tuple + padding_nulls)
                else:
                    matching_tuples.append(padding_nulls + outer_tuple)

            for tup in matching_tuples:
                yield tup

    def left_join(self):
        # 左连接和右连接是等价的，例如
        # t1 left join t2 等价于 t2 right join t1
        # 再强调一下：正常模式下 （没有exchange）outer table 就是 left table
        for tup in self.outer_join(outer_table=self.left_table,
                                   inner_table=self.right_table,
                                   exchange=False):
            yield tup

    def right_join(self):
        # 左连接和右连接是等价的，例如
        # t1 left join t2 等价于 t2 right join t1
        for tup in self.outer_join(outer_table=self.right_table,
                                   inner_table=self.left_table,
                                   exchange=True):
            yield tup

    def full_join(self):
        padding_nulls_left = tuple([None] * len(self.left_table.columns))
        padding_nulls_right = tuple([None] * len(self.right_table.columns))

        # 注意：此处是一个物化过程，因为这些元组不止一次被使用，不像上面哪些join类型，
        # 那些元组只使用一次，因此不需要物化。
        left_tuples = []
        right_tuples = []
        for tup in self.left_table.next():
            left_tuples.append(tup)
        for tup in self.right_table.next():
            right_tuples.append(tup)

        # 下面开始进行Full Join过程
        for left_tuple in left_tuples:
            matching_tuples = []
            for right_tuple in right_tuples:
                joined_tuple = left_tuple + right_tuple
                values = cast_tuple_pair_to_values(self.columns, joined_tuple)
                if is_condition_true(values, self.join_condition):
                    matching_tuples.append(joined_tuple)
            # 到此时，相当于做完了内连接
            # 下面的部分，是左连接部分
            if not matching_tuples:
                matching_tuples.append(left_tuple + padding_nulls_right)
            # 到此时，已经完成了左连接的实现
            for tup in matching_tuples:
                yield tup

        # 下面开始做右连接的处理了
        for right_tuple in right_tuples:
            not_matched = True
            for left_tuple in left_tuples:
                joined_tuple = left_tuple + right_tuple
                values = cast_tuple_pair_to_values(self.columns, joined_tuple)
                if is_condition_true(values, self.join_condition):
                    # 此时，相当于去重，避免两次返回 Inner join 结果
                    not_matched = False
                    break
            if not_matched:
                yield padding_nulls_left + right_tuple

    def next(self):
        join_type = self.join_type.upper()
        if join_type == JoinType.CROSS_JOIN:
            generator = self.cross_join()
        elif join_type == JoinType.INNER_JOIN:
            generator = self.inner_join()
        elif join_type == JoinType.LEFT_JOIN:
            generator = self.left_join()
        elif join_type == JoinType.RIGHT_JOIN:
            generator = self.right_join()
        elif join_type == JoinType.FULL_JOIN:
            generator = self.full_join()
        else:
            raise NotImplementedError(f'not supported {self.join_type}.')

        for tup in generator:
            yield tup


class PhysicalQuery(PhysicalOperator):
    def __init__(self):
        """
        该物理算子相当于一个dummy, 相当于一种编程技巧，
        但是我们给他找点活干，让他帮忙记录一下执行阶段的信息，
        如执行耗时等等
        """
        super().__init__('Result')
        self.open_time = 0
        self.close_time = 0
        self.actual_rows = 0
        self.projection_column_ids = []

    def open(self):
        # 这个获取时间戳的好处是，获取单调时间戳，可以忽略操作系统上
        # 用户手动修改时间的影响
        self.open_time = time.monotonic()

        for child in self.children:
            child.open()

        # 遍历要输出的列，寻找子节点中对应的下标位置
        child_columns = self.children[0].columns  # 子节点返回的所有列信息
        for target_column in self.columns:
            for j, child_column in enumerate(child_columns):
                if target_column == child_column:
                    self.projection_column_ids.append(j)

    def close(self):
        for child in self.children:
            child.close()
        self.close_time = time.monotonic()

    def next(self):
        for child in self.children:
            self.actual_rows += 1
            for tup in child.next():
                # 要做没有用的列的删除，即投影操作
                yield tuple(tup[i] for i in self.projection_column_ids)

    @property
    def elapsed_time(self):
        # 执行阶段的总耗时
        return self.close_time - self.open_time


class PhysicalInsert(PhysicalOperator):
    def __init__(self, logical_operator: InsertOperator):
        super().__init__('Insert')
        self.logical_operator = logical_operator
        self.column_ids = []
        self.table_column_num = 0
        self.indexes = None

    def open(self):
        all_columns = catalog_table.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )[0].columns
        for i, column in enumerate(all_columns):
            for table_column in self.logical_operator.columns:
                if column == table_column.column_name:
                    self.column_ids.append(i)
        self.table_column_num = len(all_columns)
        if len(self.column_ids) != len(self.logical_operator.columns) or (
                len(self.column_ids) > self.table_column_num
        ):
            raise ExecutorCheckError(f'error caused by columns.')

        # 遍历所有涉及到的索引,后面也要同步更新他
        indexes = catalog_index.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )
        self.indexes = []
        for index_form in indexes:
            index_name = index_form.index_name
            column_ids = []
            for column_name in index_form.columns:
                column_ids.append(all_columns.index(column_name))
            self.indexes.append(
                dict(index_name=index_name, column_ids=column_ids)
            )

        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.acquire_lock(('table', table_name), xid, 'x')
        for index_item in self.indexes:
            lock_manager.acquire_lock(('index', index_item['index_name']), xid, 'x')

    def close(self):
        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.release_lock(('table', table_name), xid)
        for index_item in self.indexes:
            lock_manager.release_lock(('index', index_item['index_name']), xid)

    @staticmethod
    def _pad_null(tup, set_ids, total_length):
        full_tuple = [None] * total_length
        for i, value in zip(set_ids, tup):
            full_tuple[i] = value
        return tuple(full_tuple)

    def next(self):
        for tup in self.logical_operator.values:
            # 更新基础数据表
            try:
                location = table_tuple_insert_one(
                    self.logical_operator.table_name, self._pad_null(
                        tup, self.column_ids, self.table_column_num
                    )
                )
            except Exception as e:
                # 出现问题了，要回滚！
                raise RollbackError(
                    f'cannot insert data into the table {self.logical_operator.table_name}.'
                )
            # 同步更新所有涉及到的索引
            # insert into t1 (id) values ...; 补充null的场景
            # insert into t1 values ...;
            for index_info in self.indexes:
                index_tuple_insert_one(
                    index_info['index_name'],
                    key=self._pad_null(
                        tup, index_info['column_ids'], len(index_info['column_ids'])),
                    value=location)

            yield


class PhysicalUpdate(PhysicalOperator):
    def __init__(self, logical_operator: UpdateOperator):
        super().__init__('Update')
        self.logical_operator = logical_operator
        self.column_ids = []
        self.table_column_num = 0
        self.indexes = None

    def open(self):
        all_columns = catalog_table.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )[0].columns
        for i, column in enumerate(all_columns):
            for table_column in self.logical_operator.columns:
                if column == table_column.column_name:
                    self.column_ids.append(i)
        self.table_column_num = len(all_columns)
        if len(self.column_ids) != len(self.logical_operator.columns) or (
                len(self.column_ids) > self.table_column_num
        ):
            raise ExecutorCheckError(f'error caused by columns.')

        # 遍历所有涉及到的索引,后面也要同步更新他
        indexes = catalog_index.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )
        self.indexes = []
        for index_form in indexes:
            index_name = index_form.index_name
            column_ids = []
            for column_name in index_form.columns:
                column_ids.append(all_columns.index(column_name))
            self.indexes.append(
                dict(index_name=index_name, column_ids=column_ids)
            )

        # Update 是有子节点的，他的子节点就是Scan算子，不同于select
        # 语句，这个Scan算子返回的是位置 location, 可以用于更新
        assert len(self.children) == 1
        assert isinstance(self.children[0], LocationScan)
        for child in self.children:
            child.open()

        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.acquire_lock(('table', table_name), xid, 'x')
        for index_item in self.indexes:
            lock_manager.acquire_lock(('index', index_item['index_name']), xid, 'x')

    def close(self):
        for child in self.children:
            child.close()

        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.release_lock(('table', table_name), xid,)
        for index_item in self.indexes:
            lock_manager.release_lock(('index', index_item['index_name']), xid,)

    def _update_from_old_tuple(self, old_tuple):
        new_tuple = list(old_tuple)
        for i, value in zip(self.column_ids, self.logical_operator.values):
            new_tuple[i] = value
        assert len(new_tuple) == self.table_column_num
        return tuple(new_tuple)

    def next(self):
        # 该节点只有一个子节点，该写法等效于
        # `for location in self.children[0].next_location()`
        for child in self.children:
            for location in child.next():
                if location is None:
                    # 出现问题了，要回滚！
                    raise RollbackError(
                        f'cannot update data for the table {self.logical_operator.table_name}.'
                    )

                old_tuple = table_tuple_get_one(self.logical_operator.table_name,
                                                location)
                new_tuple = self._update_from_old_tuple(old_tuple)
                new_location = table_tuple_update_one(
                    self.logical_operator.table_name,
                    location,
                    new_tuple
                )
                # 同步更新所有涉及到的索引
                for index_info in self.indexes:
                    key = tuple(new_tuple[i] for i in index_info['column_ids'])
                    index_tuple_update_one(
                        index_info['index_name'],
                        key=key,
                        old_value=location,
                        value=new_location
                    )

                yield


class PhysicalDelete(PhysicalOperator):
    def __init__(self, logical_operator: DeleteOperator):
        super().__init__('Insert')
        self.logical_operator = logical_operator
        self.indexes = None

    def open(self):
        all_columns = catalog_table.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )[0].columns

        # 遍历所有涉及到的索引,后面也要同步更新他
        indexes = catalog_index.select(
            lambda r: r.table_name == self.logical_operator.table_name
        )
        self.indexes = []
        for index_form in indexes:
            index_name = index_form.index_name
            column_ids = []
            for column_name in index_form.columns:
                column_ids.append(all_columns.index(column_name))
            self.indexes.append(
                dict(index_name=index_name, column_ids=column_ids)
            )

        # Delete 是有子节点的，他的子节点就是Scan算子，不同于select
        # 语句，这个Scan算子返回的是位置 location, 可以用于更新
        assert len(self.children) == 1
        assert isinstance(self.children[0], LocationScan)
        for child in self.children:
            child.open()

        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.acquire_lock(('table', table_name), xid, 'x')
        for index_item in self.indexes:
            lock_manager.acquire_lock(('index', index_item['index_name']), xid, 'x')

    def close(self):
        for child in self.children:
            child.close()

        xid = transaction_mgr.session_xid()
        table_name = self.logical_operator.table_name
        lock_manager.release_lock(('table', table_name), xid)
        for index_item in self.indexes:
            lock_manager.release_lock(('index', index_item['index_name']), xid)

    def next(self):
        locations = []
        for child in self.children:
            for location in child.next():
                old_tuple = table_tuple_get_one(self.logical_operator.table_name,
                                                location)
                yield
                locations.append(location)
                for index_info in self.indexes:
                    index_name = index_info['index_name']
                    key = tuple(old_tuple[i] for i in index_info['column_ids'])
                    index_tuple_delete_one(index_name, key=key, location=location)

        table_tuple_delete_multiple(self.logical_operator.table_name,
                                    locations)


class PhysicalDDL(PhysicalOperator):
    def __init__(self, logical_operator):
        super().__init__('DDL')
        self.ast = logical_operator.ast

    @staticmethod
    def cast_to_type(type_name):
        if type_name == 'int' or type_name == 'integer':
            return int
        elif type_name == 'text':
            return str
        else:
            raise NotImplementedError(f'not supported this type {type_name}.')

    def open(self):
        if isinstance(self.ast, CreateTable):
            columns = []
            types = []
            for column, type_ in self.ast.columns:
                columns.append(column)
                types.append(self.cast_to_type(type_))
            catalog_table.insert(CatalogTableForm(
                self.ast.table.parts, columns, types))
        elif isinstance(self.ast, CreateIndex):
            index_name = self.ast.index.parts
            table_name = self.ast.table.parts
            if not table_exists(table_name):
                raise ExecutorCheckError(f'not found the table {table_name}.')

            columns = []
            for column in self.ast.columns:
                column = column.parts
                columns.append(column)
                if not column_exists(table_name, column):
                    raise ExecutorCheckError(
                        f'not found the column {column} in table {table_name}.')

            catalog_index.insert(CatalogIndexForm(index_name,
                                                  columns,
                                                  table_name))
            try:
                index_tuple_create(index_name, table_name, columns)
            except Exception as e:
                # 相当于一个小回滚
                catalog_index.delete(lambda r: r.index_name == index_name)
                raise RollbackError(e)
        else:
            raise NotImplementedError(f'not supported this type {type(self.ast)}.')

    def close(self):
        pass

    def next(self):
        yield


class CommandOperator(PhysicalOperator):
    def __init__(self, command, args=None):
        assert isinstance(command, str)
        super().__init__('Command')
        self.command = command
        if args is None:
            self.args = None
        else:
            # args 是 expr 列表，而expr是identifier，需要通过
            # parts 来获取具体的字符串内容
            self.args = [identifier.parts for identifier in args]

    def open(self):
        if self.command == 'SHOW':
            if self.args[0] == 'variables':
                self.columns = ['name', 'value']

    def close(self):
        pass

    def next(self):
        if self.command == 'CHECKPOINT':
            checkpoint()
            yield
        elif self.command == 'SHOW':
            if self.args[0] == 'variables':
                rows = [
                    ('transaction_count', instr.transaction_count),
                    ('current_xid', transaction_mgr.current_xid),
                    ('activity_count', len(transaction_mgr.undo_mgr.active_transactions))
                ]
                for r in rows:
                    yield r
        else:
            raise NotImplementedError(f'not supported this command {self.command}.')

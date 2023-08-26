from imoocdb.storage.entry import (table_tuple_get_all,
                                   index_tuple_get_range,
                                   index_tuple_get_equal_value,
                                   covered_index_tuple_get_range,
                                   covered_index_tuple_get_equal_value,
                                   )
from imoocdb.sql.logical_operator import Condition
from imoocdb.common.fabric import TableColumn
from imoocdb.catalog.entry import catalog_table


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

    def open(self):
        pass

    def close(self):
        pass

    def next(self):
        left = ...
        right = ...

        if not self.condition:
            raise NotImplementedError()
        elif self.condition.sign == '=':
            yield index_tuple_get_equal_value(self.index_name, ...)


class CoveredIndexScan(PhysicalOperator):
    def __init__(self, index_name, condition=None):
        super().__init__('CoveredIndexScan')
        self.index_name = index_name
        self.condition = condition

    def open(self):
        pass

    def close(self):
        pass

    def next(self):
        pass
        # todo: 暂时先不考虑条件检查
        # yield index_tuple_get_range(self.index_name)

import logging

from imoocdb.common.fabric import TableColumn
from imoocdb.catalog.entry import catalog_table

from .parser.ast import BinaryOperation, FunctionOperation, Identifier, Constant
from ..errors import NoticeError


class LogicalOperator:
    def __init__(self, name):
        self.name = name
        self.children = []  # 用于放置该逻辑算子的子节点
        self.parent = None  # 用于快速回溯到父节点

    def add_child(self, operator):
        assert operator is not None  # null, NULL
        # 对于已经有父节点的算子，不能添加为子节点
        # 提示：3.11 课后修复该bug, 对于自连接(self-join)，子节点可以是重复的
        if self.name != 'Join':
            assert operator.parent is None
        self.children.append(operator)
        operator.parent = self

        # 返回刚刚添加的子节点
        return operator


class ScanOperator(LogicalOperator):
    """Scan operator comes from 'FROM' clause."""

    def __init__(self, table_name):
        super().__init__('Scan')
        self.table_name = table_name
        # todo:
        # 如果这个columns是可以裁剪的，此处返回被裁减后的列数组
        self.columns = catalog_table.select(
            lambda r: r.table_name == table_name)[0].columns
        # 注意：关于扫描表，应该扫描哪些字段，是否需要提前进行
        # 列裁剪，应该有对应的字段进行承载
        self.condition = None


class Condition:
    def __init__(self, operation: BinaryOperation):
        self.sign = operation.op

        # 把 condition 中的 Identifier 转换为TableColumn
        self.left = self._to_table_column(operation.args[0])
        self.right = self._to_table_column(operation.args[1])

        # ... where a > 100;
        #    left : a TableColumn封装之后的字段
        #    right : 100 constant: int, string
        #    sign : >

    @staticmethod
    def _to_table_column(node):
        if not isinstance(node, Identifier):
            assert isinstance(node, Constant)
            # 例如，Constant(1)
            return node.value

        full_name = node.parts
        if '.' not in full_name:
            raise NoticeError('not set a table name in the condition.')
        table_name, column = full_name.split('.')
        return TableColumn(table_name, column)


class FilterOperator(LogicalOperator):
    """Filter operator comes from 'WHERE' clause."""

    def __init__(self, condition: Condition):
        super().__init__('Filter')
        # todo: 暂时先用AST结构，后面再考虑是否要进一步封装
        self.condition = condition


class SortOperator(LogicalOperator):
    """Sort operator comes from 'SORT BY' clause."""

    def __init__(self, sort_column, asc=True):
        super().__init__('Sort')
        self.sort_column = sort_column
        self.asc = asc


class GroupOperator(LogicalOperator):
    """Group operator comes from 'GROUP BY' clause."""

    def __init__(self, group_by_column, aggregate_function_name, aggregate_column):
        # 例如，
        # select a, count(a) from t1 group by a;
        # group_by_column 是 group by 子句后面的那个（些）列
        # aggregate_column 是聚合函数里面的那个（些）列
        super().__init__('Group')
        self.group_by_column = group_by_column
        self.aggregate_function_name = aggregate_function_name
        self.aggregate_column = aggregate_column
        # 发散：如果是group by 有having 子句的话，这里面就可以加一个带having子句的条件
        # 例如，self.having_condition = ConditionX
        # 或者，在这个Group 算子上面，再加一个Filter算子，也行，等价的


class JoinOperator(LogicalOperator):
    """Join operator comes from 'JOIN' clause."""

    def __init__(self, join_type: str, left_table_name, right_table_name, join_condition):
        super().__init__('Join')
        self.join_type = join_type
        self.left_table_name = left_table_name
        self.right_table_name = right_table_name
        # 注意：我们此时先思考一下，Join的条件应该抽象成什么类型呢？
        self.join_condition = join_condition


class Query(LogicalOperator):
    """包含projection投影信息，兼职封装了逻辑执行计划，是逻辑执行计划的
    树的根节点"""
    SELECT = 'select'
    DELETE = 'delete'
    INSERT = 'insert'
    UPDATE = 'update'

    def __init__(self, query_type: str):
        super().__init__('Query')  # Projection
        self.query_type = query_type
        assert query_type in (self.SELECT, self.DELETE, self.INSERT, self.UPDATE)

        self.project_columns = []
        # 有些“缓存”信息，可以放到这里面，便于加快后面的执行效果
        self.scan_operators = []
        self.where_condition = None
        self.join_operator = None
        self.sort_operator = None
        self.group_by_column = None
        self.aggregate_columns = []


class InsertOperator(LogicalOperator):
    """Insert语句产生的逻辑算子"""

    def __init__(self, table_name, columns: list, values: list):
        super().__init__('Insert')
        # insert into t1 (a, b) values (1, 2), (3, 4);
        #            表名 列名          values嵌套列表
        self.table_name = table_name
        self.columns = columns
        # 这个values字段，应该是一个list套list, 即嵌套列表（nested list）
        # 一般应该是二维数组
        self.values = values


class DeleteOperator(LogicalOperator):
    """Delete语句产生的逻辑算子"""

    def __init__(self, table_name, condition=None):
        super().__init__('Delete')
        self.table_name = table_name
        self.condition = condition

        # 转变为query 的逻辑：
        # condition 可以转为 select 逻辑位置 where ...;
        # 只是这些逻辑位置不能通过select得到
        # 例如，PostgreSQL有 ctid 可以表示行的逻辑地址


class UpdateOperator(LogicalOperator):
    """Update语句产生的逻辑算子"""

    def __init__(self, table_name, columns, values, condition=None):
        super().__init__('Update')
        self.table_name = table_name
        # 有个小技巧，后面我们可以用 zip() 函数
        self.columns = columns
        self.values = values
        self.condition = condition


class DDLOperator(LogicalOperator):
    def __init__(self, ast):
        super().__init__('DDL')
        self.ast = ast


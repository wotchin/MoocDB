from typing import Union

from imoocdb.common.fabric import FunctionColumn
from imoocdb.errors import SQLLogicalPlanError
from imoocdb.sql.parser.ast import *
from imoocdb.sql.logical_operator import *
from imoocdb.catalog import catalog_table, catalog_function, catalog_index
from imoocdb.executor.operator.physical_operator import (
    TableScan, IndexScan, CoveredIndexScan, Sort, HashAgg,
    NestedLoopJoin, PhysicalQuery,
    PhysicalInsert, PhysicalUpdate, LocationScan, PhysicalDelete, PhysicalDDL, CommandOperator)

from imoocdb.sql.utils import table_exists, column_exists, function_exists


class SelectTransformer:
    @staticmethod
    def transform_clause_from(ast, query):
        # 表是不是存在的，要先验证一下，这个是逻辑计划生成的前提
        unchecked_tables = []
        if isinstance(ast.from_table, Identifier):
            unchecked_tables.append(ast.from_table.parts)
        elif isinstance(ast.from_table, Join):
            unchecked_tables.append(ast.from_table.left.parts)
            unchecked_tables.append(ast.from_table.right.parts)
        else:
            raise

        checked_tables = []
        for table_name in unchecked_tables:
            results = catalog_table.select(lambda r: r.table_name == table_name)
            # 卫语句，可以降低代码的嵌套逻辑复杂度
            if len(results) != 1:
                raise SQLLogicalPlanError(f'not found table {table_name}!')
            # 避免去写 else
            checked_tables.append(table_name)

        for table_name in checked_tables:
            query.scan_operators.append(ScanOperator(table_name))

    @staticmethod
    def transform_target_list(ast, query):
        """处理投影projection部分"""
        for target in ast.targets:
            target_list = []
            if isinstance(target, Star):
                # 这个过程完成的是 * 的解析
                for scan_operator in query.scan_operators:
                    table_name = scan_operator.table_name
                    results = catalog_table.select(lambda r: r.table_name == table_name)
                    # TableColumn 做一个小封装
                    for column in results[0].columns:
                        target_list.append(TableColumn(table_name, column))
            elif isinstance(target, Identifier):
                # 那就是一个表的具体列名
                # 一个小改进点：这个 target.parts 是可以包含 '.'的，表示 t1.name 这种形式
                # 我们不考虑这种场景，要求用户必须指定表名，否则报错
                # 如果用户不显性指定表名的话，在join同名字段的时候，会报错，麻烦
                full_name = target.parts
                if '.' not in full_name:
                    raise SQLLogicalPlanError('please set a specific table name.')
                # t1.name -> t1, name
                # select t1.name from t1;
                # select name from t1; <- 我们也可以自动来补充该 t1 表的信息，但是
                # 会增加代码量，其过程，就是从from后面所涉及的表中进行遍历列信息即可
                table_name, column = full_name.split('.')
                if not column_exists(table_name, column):
                    raise SQLLogicalPlanError(f'not found {full_name}.')

                # pair 对，用一个class来封装，其是也可以用list或者tuple来封装
                # 但是，class封装会增加表达力，指的是我们可以通过判断数据类型，来
                # 知道这是不是一个表的字段
                target_list.append(TableColumn(table_name, column))
            elif isinstance(target, FunctionOperation):
                # function 会麻烦一点，因为涉及到 function 内部还有些字段
                # e.g., select count(t1.a) from t1 group by t1.a;
                # target_list.append(Function)
                args = []
                for arg in target.args:
                    full_name = arg.parts
                    table_name, column = full_name.split('.')
                    if not column_exists(table_name, column):
                        raise SQLLogicalPlanError(f'not found {full_name}.')
                    args.append(TableColumn(table_name, column))
                function_name = target.op
                if not function_exists(function_name):
                    raise SQLLogicalPlanError(f'not found {function_name}.')
                target_list.append(FunctionColumn(function_name, *args))
            else:
                raise SQLLogicalPlanError(f'unknown target {target}.')
            query.project_columns.extend(target_list)

    @staticmethod
    def transform_clause_where(ast, query):
        """处理选择部分"""
        if not ast.where:
            return
        # todo: 暂时先不支持多个条件，先支持一个条件，后边有机会再增强
        condition = Condition(ast.where)
        query.where_condition = FilterOperator(condition)
        # 本节内容结尾“bug”: 是因为此处没有判断表中存在where子句中的列
        # 下面的代码补充该部分的实现：
        for node in (condition.left, condition.right):
            if (isinstance(node, TableColumn) and
                    not column_exists(node.table_name, node.column_name)):
                raise SQLLogicalPlanError(f'not found table column {node}.')

        # 后面，把这个filter 放到对应的scan算子的头上 -- done

    @staticmethod
    def transform_clause_join(ast, query):
        """处理连接部分"""
        # Join 语句出现在from_table字段中
        if not isinstance(ast.from_table, Join):
            return
        join_ast = ast.from_table

        # 做一个简单的检查，判断是否存在这个表
        # select t1.name, t2.age from t1 left join t2 on t1.id = t2.uid;
        left_table_name = join_ast.left.parts
        right_table_name = join_ast.right.parts
        if not table_exists(left_table_name):
            raise SQLLogicalPlanError(f'not found the table {left_table_name}.')
        if not table_exists(right_table_name):
            raise SQLLogicalPlanError(f'not found the table {right_table_name}.')

        # 验证一下Join condition中的字段是否合法
        # 提示：3.11 中课后修复此处bug, not column_exists() 条件中忘了 'not'
        join_condition = Condition(join_ast.condition)
        if (isinstance(join_condition.left, TableColumn) and
                not column_exists(join_condition.left.table_name,
                                  join_condition.left.column_name)):
            raise SQLLogicalPlanError(f'not found the column {join_condition.left}.')
        if (isinstance(join_condition.right, TableColumn) and
                not column_exists(join_condition.right.table_name,
                                  join_condition.right.column_name)):
            raise SQLLogicalPlanError(f'not found the column {join_condition.right}.')

        join_operator = JoinOperator(join_type=join_ast.join_type,
                                     left_table_name=left_table_name,
                                     right_table_name=right_table_name,
                                     join_condition=join_condition)
        query.join_operator = join_operator

        # 做完了吗？没有！
        # 思考：是否也可以放到后面一起组合算子树呢？
        # 因为Join的子节点是数据的来源，而数据的来源是Scan算子，因此，我们为
        # JoinOperator 添加ScanOperator子节点
        for scan_operator in query.scan_operators:
            # 问题思考：下面这两个 if 语句的顺序可以换吗？
            if scan_operator.table_name == join_operator.left_table_name:
                join_operator.add_child(scan_operator)
            # 提示: 3.11 课后修复bug, else -> 变为 elif 否则对于self-join会重复添加
            elif scan_operator.table_name == join_operator.right_table_name:
                join_operator.add_child(scan_operator)

    @staticmethod
    def transform_clause_order(ast, query):
        """处理排序部分"""
        if not ast.order_by:
            return
        full_name = ast.order_by.column.parts
        if '.' not in full_name:
            raise SQLLogicalPlanError(f'please set a table name for the column {full_name}.')
        table_name, column = full_name.split('.')

        # 检查一下列是否存在
        if not column_exists(table_name, column):
            raise SQLLogicalPlanError(f'not found the column {full_name}.')

        query.sort_operator = SortOperator(sort_column=TableColumn(table_name, column),
                                           asc=(ast.order_by.direction == 'ASC'))

    @staticmethod
    def transform_clause_group(ast, query):
        """处理聚合部分"""
        if not ast.group_by:
            return
        group_by_list = ast.group_by
        if len(group_by_list) != 1:
            raise NotImplementedError(
                f'only supported one column for the group by clause.'
            )
        full_name = group_by_list[0].parts

        if '.' not in full_name:
            raise SQLLogicalPlanError(f'please set a table name for the column {full_name}.')
        table_name, column = full_name.split('.')

        # 检查一下列是否存在
        if not column_exists(table_name, column):
            raise SQLLogicalPlanError(f'not found the column {full_name}.')
        query.group_by_column = TableColumn(table_name, column)

        # 检查一下是否存在聚合函数？在 transform_target_list() 中已经完成了检查
        for column in query.project_columns:
            if isinstance(column, FunctionColumn) and catalog_function.select(
                    lambda r: r.function_name == column.function_name and
                              r.agg_function):
                query.aggregate_columns.append(column)

        if len(query.aggregate_columns) > 1:
            raise NotImplementedError('not supported one more aggregation functions.')
        if query.aggregate_columns and len(query.aggregate_columns[0].args) != 1:
            raise NotImplementedError(
                f'aggregation function {query.aggregate_columns[0].function_name} '
                f'must have one column.')

    @staticmethod
    def rewrite(query):
        """逻辑计划的初步整理、优化"""
        # 我们组合各个query的属性，把他们按照顺序构建起一颗树来
        # 这个顺序正好是 transform 过程反过来

        all_seen_columns = {}  # key: 表名, value: 列数组
        building_node = query
        if query.group_by_column:
            operator = GroupOperator(
                group_by_column=query.group_by_column,
                aggregate_function_name=query.aggregate_columns[0].function_name,
                aggregate_column=query.aggregate_columns[0].args[0],
            )
            building_node = building_node.add_child(operator)
            # Keras 有类似的写法
            # x = Layer()(x)
        if query.sort_operator:
            building_node = building_node.add_child(query.sort_operator)
        if query.join_operator:
            building_node = building_node.add_child(query.join_operator)
        if query.where_condition:
            # 把 where 条件下推到对应的扫描算子上
            # 在我们的场景中，有两种比较典型的场景不能直接推:
            # eg1: select * from t1, t2 where t1.a > t2.b;
            # eg2: select * from t1 where 1 > 2;
            # eg3: select * from t1 where t1.a > 100;
            # eg1 可以此时改写为 inner join
            # eg2 可以改写为直接返回空结果集
            # eg3 可以把 filter 条件下推到扫描过程中
            # 思考：你还能想到哪些不能推的场景呢？

            filter_operator = query.where_condition
            # eg1 的场景：
            if (isinstance(filter_operator.condition.left, TableColumn) and
                    isinstance(filter_operator.condition.right, TableColumn)):
                # 判断一下是否合法
                table_names = (query.join_operator.left_table_name,
                               query.join_operator.right_table_name)
                if not (query.join_operator and
                        filter_operator.condition.left.table_name in table_names and
                        filter_operator.condition.right.table_name in table_names):
                    raise SQLLogicalPlanError(f'tables in where clause should be all seen in '
                                              f'the join clause.')

                # 不支持的场景：
                # select * from t1 left join t2 on t1.id = t2.id where t1.age > t2.age;
                if query.join_operator.join_type != JoinType.CROSS_JOIN:
                    raise NotImplementedError('not supported complex where clause.')

                query.join_operator.join_type = JoinType.INNER_JOIN
                query.join_operator.condition = filter_operator.condition
            # 其他场景，这里面我们没有针对eg2单独改写，但是你可以试试~
            else:
                table_column = None
                if isinstance(filter_operator.condition.left, TableColumn):
                    table_column = filter_operator.condition.left
                elif isinstance(filter_operator.condition.right, TableColumn):
                    table_column = filter_operator.condition.right
                else:
                    # todo: 此处是 eg2 的改写地方
                    pass

                if table_column:
                    for scan_operator in query.scan_operators:
                        if scan_operator.table_name == table_column.table_name:
                            scan_operator.condition = filter_operator.condition

        if not query.join_operator:
            assert len(query.scan_operators) == 1
            building_node.add_child(query.scan_operators[0])
        # todo: 现在可以做一些RBO的优化动作了，例如列裁剪

    @staticmethod
    def transform(ast: Select):
        query = Query(Query.SELECT)
        SelectTransformer.transform_clause_from(ast, query)
        SelectTransformer.transform_target_list(ast, query)
        SelectTransformer.transform_clause_where(ast, query)
        SelectTransformer.transform_clause_join(ast, query)
        SelectTransformer.transform_clause_order(ast, query)
        SelectTransformer.transform_clause_group(ast, query)

        # 到此为止，我们就可产生一个比较朴素的、原始的 Query
        # 此时，我们可以进一步套用逻辑查询优化（重写）机制来优化Query
        SelectTransformer.rewrite(query)
        return query


class DMLTransformer:
    @staticmethod
    def transform(ast):
        if isinstance(ast, Insert):
            table_name = ast.table.parts
            if not table_exists(table_name):
                raise SQLLogicalPlanError(f'not found the table {table_name}.')
            if ast.columns:
                columns = [
                    column.parts for column in ast.columns
                ]
            else:
                columns = catalog_table.select(
                    lambda r: r.table_name == table_name
                )[0].columns
            for column in columns:
                if not column_exists(table_name, column):
                    raise SQLLogicalPlanError(f'not found the column {column}.')
            values = []
            for value_list in ast.values:
                strip_value = []
                for value_constant in value_list:
                    strip_value.append(value_constant.value)
                values.append(strip_value)
            return InsertOperator(
                table_name=table_name,
                columns=[TableColumn(table_name, column) for column in columns],
                values=values
            )
        elif isinstance(ast, Update):
            table_name = ast.table.parts
            if not table_exists(table_name):
                raise SQLLogicalPlanError(f'not found the table {table_name}.')
            column_value_pair = [
                (column.parts, value_constant.value)
                for column, value_constant in ast.columns.items()
            ]
            for i in range(0, len(column_value_pair)):
                column, value = column_value_pair[i]
                # 如果是带有 . 符号的
                if '.' in column:
                    t, c = column.split('.')
                    if t != table_name:
                        raise SQLLogicalPlanError(
                            f'cannot match the table {t}.')
                    column = c
                    column_value_pair[i] = (c, value)
                if not column_exists(table_name, column):
                    raise SQLLogicalPlanError(f'not found the column {column}.')
            if ast.where:
                condition = Condition(ast.where)
                for node in (condition.left, condition.right):
                    if (isinstance(node, TableColumn) and
                            not column_exists(node.table_name, node.column_name)):
                        raise SQLLogicalPlanError(f'not found the table column {node}.')
            else:
                condition = None
            return UpdateOperator(
                table_name=table_name,
                columns=[
                    TableColumn(table_name, column)
                    for column, _ in column_value_pair
                ],
                values=[
                    value for _, value in column_value_pair
                ],
                condition=condition)
        elif isinstance(ast, Delete):
            table_name = ast.table.parts
            if not table_exists(table_name):
                raise SQLLogicalPlanError(f'not found the table {table_name}.')
            if ast.where:
                condition = Condition(ast.where)
                for node in (condition.left, condition.right):
                    if (isinstance(node, TableColumn) and
                            not column_exists(node.table_name, node.column_name)):
                        raise SQLLogicalPlanError(f'not found the table column {node}.')
            else:
                condition = None
            return DeleteOperator(table_name=table_name, condition=condition)
        else:
            raise NotImplementedError(f'not supported this AST {ast}.')


def query_logical_plan(ast: ASTNode) -> LogicalOperator:
    if isinstance(ast, Select):
        return SelectTransformer.transform(ast)
    elif (isinstance(ast, Insert) or
          isinstance(ast, Update) or
          isinstance(ast, Delete)):
        return DMLTransformer.transform(ast)
    elif (isinstance(ast, CreateTable) or
          isinstance(ast, CreateIndex)):
        return DDLOperator(ast)
    elif isinstance(ast, Command):
        # 直接返回这个command就行了
        return ast
    else:
        raise NotImplementedError('not supported non-select statement yet.')


###########################################

# implementation 表示将逻辑计划转换为物理计划
# select 表示针对SELECT语句
class SelectImplementation:
    @staticmethod
    def implement_scan(node) -> Union[TableScan, IndexScan, CoveredIndexScan]:
        # 返回的应该是 TableScan, IndexScan 或者 CoveredIndexScan的一个
        # 我们此处定义一个rule:
        # 有索引，优先用索引，如果这个索引可以用覆盖索引，那么优先用覆盖索引
        results = catalog_index.select(lambda r: r.table_name == node.table_name)
        if len(results) == 0 or not node.condition:
            # 用一个 “卫语句” 来返回一个case, 就是对应的表，没有索引的情况
            # 直接返回表扫描即可
            return TableScan(node.table_name, node.condition)

        # 大于0，代表有索引，我们开始尝试使用索引
        # 我们来判断下索引，能否使用
        # rule: 假如有多个索引，我们选择列数最少的那个使用
        # 但是，这里面有个前提，是列能符合最左匹配原则
        candidate_indexes = []
        condition_columns = []
        if isinstance(node.condition.left, TableColumn):
            condition_columns.append(node.condition.left)
        if isinstance(node.condition.right, TableColumn):
            condition_columns.append(node.condition.right)
        if len(condition_columns) >= 2:
            raise NotImplementedError(f'not supported multi-columns predicates.')

        # 这种边界场景: select t1.a from t1 where 1 > 2;
        if len(condition_columns) == 0:
            return TableScan(node.table_name, node.condition)
        # 下面，我们来找 condition_columns 与 results 中的索引列能匹配上的，
        # 如果匹配上了，则放到 candidate_indexes, 供我们后面进一步筛选
        for catalog_index_form in results:
            # 下面这块的逻辑，就是在验证最左匹配原则，
            # 例如索引包含了多个列 index(t1.a, t1.b, t1.c)
            # 而我们的谓词predicate在下面几个例子上，分别为：
            # 包含了 t1.a --> 符合
            # 包含了 t1.a, t1.b --> 符合
            # 包含了 t1.b, t1.a --> 不符合
            matched = True
            for condition_column in condition_columns:
                for index_column in catalog_index_form.columns:
                    if condition_column.column_name != index_column:
                        matched = False
                        break
            if matched:
                candidate_indexes.append(catalog_index_form)

        # 什么都匹配不到，没有可用的索引，那么要表扫描
        if not candidate_indexes:
            return TableScan(node.table_name, node.condition)

        # 接下来，从这里面挑选最合适的一个索引
        # 如果，这个query中所涉及到的列，都涵盖在这个索引里面了，那么我们
        # 使用覆盖索引，否则，找在candidate_indexes索引列最短的那个使用。
        for candidate_index in candidate_indexes:
            if len(node.columns) == len(candidate_index.columns):
                # 此时，应该使用覆盖索引
                return CoveredIndexScan(
                    index_name=candidate_index.index_name,
                    condition=node.condition
                )

        shortest_index = candidate_indexes[0]
        for candidate_index in candidate_indexes:
            # if shortest_index is None:
            #     shortest_index = candidate_index
            #     # continue 可写可不写
            if len(candidate_index.columns) < len(shortest_index.columns):
                shortest_index = candidate_index

        return IndexScan(index_name=shortest_index.index_name, condition=node.condition)

    @staticmethod
    def implement_sort(node) -> Sort:
        # 可以优化的点：
        # 默认是内排序，如果我们有参数设置的话，可以在此处做一个判断，如果超了预估
        # 的内存使用（排序行数），那么就使用外排序
        return Sort(
            node.sort_column,
            asc=node.asc
        )

    @staticmethod
    def implement_agg(node) -> HashAgg:
        return HashAgg(
            node.group_by_column,
            node.aggregate_function_name,
            node.aggregate_column
        )

    @staticmethod
    def implement_join(node) -> NestedLoopJoin:
        # 可以优化的点：
        # 虽然说，我们只支持两个节点进行Join, 但是它们的顺序（内外表）也是
        # 也是可以优化的点
        return NestedLoopJoin(
            node.join_type, node.left_table_name,
            node.right_table_name, node.join_condition
        )

    @staticmethod
    def implement(node):
        # 这里是通过先递归逻辑执行计划树，然后产生
        # 与之对应的物理执行计划树
        if node is None:
            return None

        if isinstance(node, ScanOperator):
            physical_node = SelectImplementation.implement_scan(node)
        elif isinstance(node, SortOperator):
            physical_node = SelectImplementation.implement_sort(node)
        elif isinstance(node, GroupOperator):
            physical_node = SelectImplementation.implement_agg(node)
        elif isinstance(node, JoinOperator):
            physical_node = SelectImplementation.implement_join(node)
        elif isinstance(node, Query):
            physical_node = PhysicalQuery()
            # 此时，是最终要输出的所有列
            # 在这里，要对下面返回来的列，做裁剪（投影，也就是删除没有的列）
            physical_node.columns = node.project_columns
        else:
            raise NotImplementedError(f'not supported this type of node {node}.')

        if len(node.children) == 0:
            return physical_node

        for child in node.children:
            physical_node.add_child(SelectImplementation.implement(child))

        return physical_node


def get_physical_scan_from_predicate(table_name, condition):
    logical_scan = ScanOperator(table_name=table_name)
    logical_scan.condition = condition
    # 此时，是一个trick, 相当于 把 update/delete 转换为 select 的部分功能
    # 再把 select 中的 scan 算子提取，复用
    return SelectImplementation.implement_scan(logical_scan)


def query_physical_plan(logical_plan: LogicalOperator) -> "PhysicalOperator":
    if isinstance(logical_plan, Query):
        if logical_plan.query_type == Query.SELECT:
            return SelectImplementation.implement(logical_plan)
        else:
            raise NotImplementedError(
                f'not supported this query type {logical_plan.query_type}.'
            )
    elif isinstance(logical_plan, InsertOperator):
        return PhysicalInsert(logical_plan)
    elif isinstance(logical_plan, UpdateOperator):
        physical_update = PhysicalUpdate(logical_plan)
        scan = LocationScan(
            get_physical_scan_from_predicate(
                logical_plan.table_name, logical_plan.condition)
        )
        physical_update.add_child(scan)
        return physical_update
    elif isinstance(logical_plan, DeleteOperator):
        physical_delete = PhysicalDelete(logical_plan)
        scan = LocationScan(
            get_physical_scan_from_predicate(
                logical_plan.table_name, logical_plan.condition)
        )
        physical_delete.add_child(scan)
        return physical_delete
    elif isinstance(logical_plan, DDLOperator):
        return PhysicalDDL(logical_plan)
    elif isinstance(logical_plan, Command):
        return CommandOperator(logical_plan.command,
                               logical_plan.args)
    else:
        raise NotImplementedError(
            'not supported this logical plan.'
        )


def query_plan(ast: ASTNode) -> "PhysicalOperator":
    logical_plan = query_logical_plan(ast)
    return query_physical_plan(logical_plan)

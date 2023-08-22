from imoocdb.common.fabric import TableColumn
from imoocdb.errors import SQLLogicalPlanError
from imoocdb.sql.parser.ast import *
from imoocdb.sql.logical_operator import *
from imoocdb.catalog import catalog_table


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
        """处理投影部分"""
        target_list = []
        for target in ast.targets:
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
                table_name, column = full_name.split('.')
                target_list.append(TableColumn(table_name, column))
            elif isinstance(target, FunctionOperation):
                # function 会麻烦一点，因为涉及到 function 内部还有些字段
                # e.g., select count(t1.a) from t1 group by t1.a;
                # target_list.append(Function)
                raise NotImplementedError('not supported function yet.')
            else:
                raise
            query.project_columns.extend(target_list)

    @staticmethod
    def transform_clause_where(ast, query):
        """处理选择部分"""
        pass

    @staticmethod
    def transform_clause_join(ast, query):
        """处理连接部分"""
        pass

    @staticmethod
    def transform_clause_order(ast, query):
        """处理排序部分"""
        pass

    @staticmethod
    def transform_clause_group(ast, query):
        """处理聚合部分"""
        pass

    @staticmethod
    def rewrite(ast, query):
        """逻辑计划的初步整理、优化"""
        pass

    @staticmethod
    def transform(ast: Select):
        query = Query([])
        SelectTransformer.transform_clause_from(ast, query)
        SelectTransformer.transform_target_list(ast, query)
        SelectTransformer.transform_clause_where(ast, query)
        SelectTransformer.transform_clause_join(ast, query)
        SelectTransformer.transform_clause_order(ast, query)
        SelectTransformer.transform_clause_group(ast, query)

        # 到此为止，我们就可产生一个比较朴素的、原始的 Query
        # 此时，我们可以进一步套用逻辑查询优化（重写）机制来优化Query
        SelectTransformer.rewrite(ast, query)
        return query


def query_logical_plan(ast: ASTNode) -> LogicalOperator:
    pass


def query_physical_plan(logical_plan: LogicalOperator) -> "PlanTree":
    pass


def query_plan(ast: ASTNode) -> "PlanTree":
    pass

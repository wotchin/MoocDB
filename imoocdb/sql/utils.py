from imoocdb.catalog import catalog_table, catalog_function


# 可以用于判断表或者列是否存在，用于检验输入SQL语句的合法性
# 我们的代码中有部分早期的实现并没有用到这两个函数，后面我们统一进行重构
def table_exists(table_name):
    return bool(catalog_table.select(lambda r: r.table_name == table_name))


def column_exists(table_name, column_name):
    tables = catalog_table.select(lambda r: r.table_name == table_name)
    if len(tables) == 1 and column_name in tables[0].columns:
        return True
    return False


def function_exists(function_name):
    results = catalog_function.select(
        lambda r: r.function_name == function_name and r.agg_function)
    if len(results) != 1:
        return False
    return True

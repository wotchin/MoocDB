from .table import CatalogTable, CatalogTableForm
from .index import CatalogIndex, CatalogIndexForm
from .function import CatalogFunction, CatalogFunctionForm

# 这些机制是 Mock 机制，即用来制造假数据，便于模块内部进行自测，或者单元测试等
# 常用的场景：单元测试、一些走网络或需要引入其他复杂组件餐能完成的测试
# （造假数据，避免真正组件的繁重开销）...
catalog_table = CatalogTable()
catalog_index = CatalogIndex()
catalog_function = CatalogFunction()


def init_catalog():
    catalog_table.load()
    catalog_index.load()
    # 添加内置函数
    catalog_function.insert(CatalogFunctionForm(
        function_name='count', arg_num=-1, callback=len, is_agg=True)
    )
    catalog_function.insert(CatalogFunctionForm(
        function_name='sum', arg_num=-1, callback=sum, is_agg=True)
    )
    catalog_function.insert(CatalogFunctionForm(
        function_name='max', arg_num=-1, callback=max, is_agg=True)
    )
    catalog_function.insert(CatalogFunctionForm(
        function_name='min', arg_num=-1, callback=min, is_agg=True)
    )
    catalog_function.insert(CatalogFunctionForm(
        function_name='avg', arg_num=-1, callback=(lambda x: sum(x) / len(x)), is_agg=True)
    )

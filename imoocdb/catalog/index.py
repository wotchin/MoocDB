from .basic import CatalogForm, CatalogBasic


class CatalogIndexForm(CatalogForm):
    def __init__(self, index_name, columns, table_name):
        # CREATE INDEX idx ON t1 (a, b) USING btree;
        # -> 某个索引包含的元信息都有：
        #   索引名: idx
        #   表名: t1
        #   列名: a, b -> 这个是个联合索引，多列索引，复合索引 ...
        #   索引的类型：可选参数，btree
        self.index_name = index_name
        self.columns = columns
        self.table_name = table_name
        # 我们不需要再定义 types 了，因为可以反向从 CatalogTable 中获取
        # self.types = types

    def __repr__(self):
        # 做一个美化效果，可以返回该表的schema (结构) 通过CREATE TABLE 语句
        return f'CREATE INDEX {self.index_name} ON {self.table_name}' \
               f' ({", ".join(self.columns)});'


class CatalogIndex(CatalogBasic):
    def __init__(self):
        super().__init__('index_information')

from .basic import CatalogForm, CatalogBasic


class CatalogTableForm(CatalogForm):
    def __init__(self, table_name, columns, types):
        self.table_name = table_name
        self.columns = columns
        self.types = types
        assert len(self.columns) == len(self.types)
        # 这里面没有包括约束条件：例如，是否可为null, 是否为主键，是否唯一 ...
        # 以及后面可能还会加其他信息，例如是不是用到索引了？表大约多少行数据?

    def __repr__(self):
        fields = []
        for column_name, type_name in zip(self.columns, self.types):
            fields.append(f'{column_name} {type_name.__name__}')
        # 做一个美化效果，可以返回该表的schema (结构) 通过CREATE TABLE 语句
        return f'CREATE TABLE {self.table_name} ({", ".join(fields)});'


class CatalogTable(CatalogBasic):
    def __init__(self):
        super().__init__('table_information')

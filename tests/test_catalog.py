from imoocdb.catalog import CatalogTable, CatalogIndex, CatalogFunction, CatalogIndexForm
from imoocdb.catalog import CatalogTableForm


def test_catalog_table():
    catalog_table = CatalogTable()
    catalog_table.insert(CatalogTableForm('t1', ['id', 'name'], [int, str]))
    catalog_table.insert(CatalogTableForm('t2', ['id', 'name', 'address'], [int, str, str]))
    catalog_table.dump()

    # 模拟数据库的catalog重新加载的过程
    catalog_table2 = CatalogTable()
    catalog_table2.load()

    assert str(catalog_table2.rows) == \
           "[CREATE TABLE t1 (id int, name str);, CREATE TABLE t2 (id int, name str, address str);]"


def test_catalog_index():
    catalog_index = CatalogIndex()
    catalog_index.insert(CatalogIndexForm('idx', ['id'], 't1'))
    catalog_index.dump()

    # 判断一下加载过程
    catalog_index2 = CatalogIndex()
    catalog_index2.load()

    assert str(catalog_index2.rows) == '[CREATE INDEX idx ON t1 (id);]'


test_catalog_index()

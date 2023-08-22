from .table import CatalogTable, CatalogTableForm


catalog_table = CatalogTable()

catalog_table.insert(CatalogTableForm('t1', ['id', 'name'], [int, str]))
catalog_table.insert(CatalogTableForm('t2', ['id', 'name', 'address'], [int, str, str]))

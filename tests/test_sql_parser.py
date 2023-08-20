from imoocdb.sql.parser.parser import query_parse


def test_parse_select_statement():
    ast = query_parse('select a, b from t1')
    assert str(
        ast) == '<Select> targets=[<Identifier> parts=a, <Identifier> parts=b] from_table=<Identifier> parts=t1 where=None group_by=None order_by=None'
    ast = query_parse('select a, b from t1 where a > c')
    ast = query_parse('select a, b from t1 where a > c order by b')
    ast = query_parse('select a, count(a) from t1 group by a where b > 100')
    try:
        query_parse('select a, b from t1 order by b order by a')
    except SyntaxError:
        pass
    else:
        raise AssertionError('should raise an exception.')


def test_parse_dml_statement():
    ast = query_parse('UPDATE t1 set a = 1 where a > 1')
    print(ast)


# test_parse_select_statement()
test_parse_dml_statement()

from imoocdb.sql.parser.parser import query_parse
from imoocdb.errors import NoticeError


def test_parse_select_statement():
    ast = query_parse('select a, b from t1')
    assert str(
        ast) == '<Select> targets=[<Identifier> parts=a, <Identifier> parts=b] from_table=<Identifier> parts=t1 where=None group_by=None order_by=None'
    ast = query_parse('select a, b from t1 where a > c')
    ast = query_parse('select a, b from t1 where a > c order by b')
    ast = query_parse('select a, count(a) from t1 group by a where b > 100')
    try:
        query_parse('select a, b from t1 order by b order by a')
    except NoticeError:
        pass
    else:
        raise AssertionError('should raise an exception.')


def test_parse_dml_statement():
    ast = query_parse('UPDATE t1 set a = 1 where a > 1')
    ast = query_parse('insert into t1 values (1,2,3)')
    assert str(ast) == '<Insert> table=<Identifier> parts=t1 columns=None values=[[<Constant> value=1, <Constant> value=2, <Constant> value=3]]'
    ast = query_parse('insert into t1(a,b,c) values (1,2,3)')
    assert str(ast) == '<Insert> table=<Identifier> parts=t1 columns=[<Identifier> parts=a, <Identifier> parts=b, <Identifier> parts=c] values=[[<Constant> value=1, <Constant> value=2, <Constant> value=3]]'
    ast = query_parse('delete from t1')
    assert str(ast) == '<Delete> table=<Identifier> parts=t1 where=None'
    ast = query_parse('delete from t1 where a > 100')
    assert str(ast) == '<Delete> table=<Identifier> parts=t1 where=<BinaryOperation> op=> args=[<Identifier> parts=a, <Constant> value=100]'


def test_parse_ddl_statement():
    ast = query_parse('CREATE TABLE t1 (id int, name text, gender int)')
    assert str(ast) == "<CreateTable> table=<Identifier> parts=t1 columns=[['id', 'int'], ['name', 'text'], ['gender', 'int']]"


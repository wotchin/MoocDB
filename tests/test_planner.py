from imoocdb.sql.optimizier.planner import query_logical_plan
from imoocdb.sql.parser.parser import query_parse


def explain(operator, indent=''):
    output_lines = []
    name = operator.name

    if indent:
        output_lines.append(f'{indent} -> {name}')
    else:
        # the first line
        output_lines.append(f'{name}')

    for i, child in enumerate(operator.children):
        child_indent = f'{indent}  '
        output_lines.extend(explain(child, child_indent))
    return output_lines


def test_logical_plan():
    ast = query_parse('select * from t1 where t1.id > 100 order by t1.id')
    # 可测试性：也需要代码的实现
    query = query_logical_plan(ast)
    assert explain(query) == ['Query', '   -> Sort', '     -> Scan']
    ast = query_parse('select t1.name, t1.id from t1 left join t1 on t1.name'
                      ' = t1.name where t1.id > 100 order by t1.id')
    # 可测试性：也需要代码的实现
    query = query_logical_plan(ast)
    assert explain(query) == ['Query', '   -> Sort', '     -> Join', '       -> Scan', '       -> Scan']

    ast = query_parse('select count(t1.id) from t1 group by t1.id')
    # 可测试性：也需要代码的实现
    query = query_logical_plan(ast)
    assert explain(query) == ['Query', '   -> Group', '     -> Scan']


test_logical_plan()

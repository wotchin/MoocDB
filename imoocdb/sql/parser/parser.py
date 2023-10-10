import re
import sly

from imoocdb.errors import NoticeError


# select a, b from t1;
# select a, b from t1 where a > c;
# select a, b from t1 where a > c order by b;
# select a, count(a) from t1 group by a where b > 100;
class SQLLexer(sly.Lexer):
    ignore = ' \t\n\r'
    reflags = re.IGNORECASE

    tokens = {
        # DDL
        CREATE, DROP,
        DATABASE, TABLE, INDEX,

        # others
        EXPLAIN,

        # select
        SELECT, STAR, FROM, WHERE, GROUP_BY, ORDER_BY, ASC, DESC,
        JOIN, FULL, INNER, OUTER, LEFT, RIGHT, ON,

        # DML: INSERT, UPDATE, DELETE
        INSERT, DELETE, INTO, VALUES, UPDATE, SET,

        # punctuation
        DOT, COMMA, LPAREN, RPAREN,

        # operators
        EQ, NE, GT, GEQ, LT, LEQ,
        AND, OR, NOT,

        # data type
        ID,
        INTEGER, QUOTE_STRING, DQUOTE_STRING, NULL,

        # command
        CHECKPOINT, SHOW
    }

    CREATE = 'CREATE'
    DROP = 'DROP'
    DATABASE = 'DATABASE'
    TABLE = 'TABLE'
    INDEX = 'INDEX'

    # others
    EXPLAIN = 'EXPLAIN'

    # select
    SELECT = 'SELECT'
    STAR = r'\*'
    FROM = 'FROM'
    WHERE = 'WHERE'
    GROUP_BY = 'GROUP BY'
    ORDER_BY = 'ORDER BY'
    ASC = 'ASC'
    DESC = 'DESC'
    JOIN = 'JOIN'
    FULL = 'FULL'
    INNER = 'INNER'
    OUTER = 'OUTER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    ON = 'ON'

    # DML: INSERT, UPDATE, DELETE
    INSERT = 'INSERT'
    DELETE = 'DELETE'
    INTO = 'INTO'
    VALUES = 'VALUES'
    UPDATE = 'UPDATE'
    SET = 'SET'

    # command
    CHECKPOINT = 'CHECKPOINT'
    SHOW = 'SHOW'

    # punctuation
    DOT = r'\.'
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'

    # operators
    EQ = r'='
    NE = r'!='
    GT = r'>'
    GEQ = r'>='
    LT = r'<'
    LEQ = r'<='
    AND = r'\bAND\b'
    OR = r'\bOR\b'
    NOT = r'\bNOT\b'

    INTEGER = r'\d+'

    @_(r'[a-zA-Z_][a-zA-Z0-9_\.]*')
    def ID(self, t):
        return t

    QUOTE_STRING = r"'[^']*'"
    DQUOTE_STRING = r'"[^"]*"'


# select a, b from t1;
# select a, b from t1 where a > c;
# select a, b from t1 where a > c order by b;
# select a, count(a) from t1 group by a where b > 100;
from imoocdb.sql.parser.ast import *


class SQLParser(sly.Parser):
    tokens = SQLLexer.tokens

    # main entrance
    # 保证入口点的单一
    @_('select',
       'update',
       'insert',
       'delete',
       'create_table',
       'create_index',
       'command')
    def query(self, p):
        return p[0]

    # command 解析
    @_('CHECKPOINT',
       'SHOW expr_list')
    def command(self, p):
        if len(p) > 1:
            return Command(p[0], p[1])
        else:
            return Command(p[0])

    # select * ...;  -> Select([Star()]) ...
    @_('SELECT target_columns')
    def select(self, p):
        return Select(targets=p.target_columns)

    @_('target_columns COMMA target_column')
    def target_columns(self, p):
        p.target_columns.append(p.target_column)
        return p.target_columns

    @_('target_column')
    def target_columns(self, p):
        return [p.target_column]

    @_('star')
    def target_column(self, p):
        return p.star

    @_('STAR')
    def star(self, p):
        return Star()

    @_('expr',
       'function')
    def target_column(self, p):
        return p[0]

    # ===== 上面是，带领大家一起实现的部分 =====
    # ===== 由于时间关系，下面是补充其他类似的部分 ====
    @_('id LPAREN expr_list_or_nothing RPAREN')
    def function(self, p):
        args = p.expr_list_or_nothing
        if not args:
            args = []
        return FunctionOperation(op=p.id, args=args)

    # from 子句
    @_('select FROM from_table',
       'select FROM cross_join_tables',
       'select FROM join_tables')
    def select(self, p):
        select = p.select
        if select.from_table:
            raise SyntaxError('duplicated from clause.')

        select.from_table = p[2]
        return select

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    # where 子句
    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        if select.where:
            raise SyntaxError('duplicated where clause.')
        if not select.from_table:
            raise SyntaxError('not set from clause.')

        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise SyntaxError(
                f"Require an operation for WHERE clause.")
        select.where = where_expr
        return select

    # order by 子句
    @_('select ORDER_BY ordering_term')
    def select(self, p):
        select = p.select
        if select.order_by:
            raise SyntaxError('duplicated order by clause.')
        if not select.from_table:
            raise SyntaxError('not set from clause.')
        select.order_by = p.ordering_term
        return select

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(column=p.identifier, direction='DESC')

    @_('identifier',
       'identifier ASC')
    def ordering_term(self, p):
        return OrderBy(column=p.identifier, direction='ASC')

    # Group by 子句
    @_('select GROUP_BY expr_list')
    def select(self, p):
        select = p.select
        if select.group_by:
            raise SyntaxError('duplicated group by clause.')
        if not select.from_table:
            raise SyntaxError('not set from clause.')

        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        select.group_by = group_by
        return select

    # Join 子句
    # 提示：3.11 课后修复该处bug, 语法解析中缺乏ON关键字
    @_('from_table join_clause from_table ON expr')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause,
                    condition=p.expr)

    @_(JoinType.LEFT_JOIN,
       JoinType.RIGHT_JOIN,
       JoinType.INNER_JOIN,
       JoinType.FULL_JOIN,
       )
    def join_clause(self, p):
        return f'{p[0]} {p[1]}'

    # 也是隐式的join类型，如 select * from t1, t2;
    @_('from_table COMMA from_table')
    def cross_join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=JoinType.CROSS_JOIN)

    # 二元表达式
    @_('expr EQ expr',
       'expr NE expr',
       'expr GEQ expr',
       'expr GT expr',
       'expr LEQ expr',
       'expr LT expr',
       'expr AND expr',
       'expr OR expr', )
    def expr(self, p):
        return BinaryOperation(op=p[1], args=(p.expr0, p.expr1))

    # 表达式
    @_('expr_list')
    def expr_list_or_nothing(self, p):
        return p.expr_list

    @_('enum')
    def expr_list(self, p):
        return p.enum

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('enum COMMA expr')
    def enum(self, p):
        return p.enum + [p.expr]

    @_('expr COMMA expr')
    def enum(self, p):
        return [p.expr0, p.expr1]

    @_('identifier')
    def expr(self, p):
        return p.identifier

    @_('constant')
    def expr(self, p):
        return p.constant

    # 常量的规则
    @_('NULL')
    def constant(self, p):
        return Constant(value=None)

    @_('integer')
    def constant(self, p):
        return Constant(value=int(p.integer))

    @_('string')
    def constant(self, p):
        return Constant(value=str(p[0]))

    @_('identifier DOT identifier')
    def identifier(self, p):
        p.identifier0.parts += p.identifier1.parts
        return p.identifier0

    @_('id')
    def identifier(self, p):
        return Identifier(p[0])

    @_('ID')
    def id(self, p):
        return p[0]

    @_('quote_string',
       'dquote_string')
    def string(self, p):
        return p[0]

    @_('INTEGER')
    def integer(self, p):
        return int(p[0])

    @_('QUOTE_STRING')
    def quote_string(self, p):
        return p[0].strip('\'')

    @_('DQUOTE_STRING')
    def dquote_string(self, p):
        return p[0].strip('\"')

    @_('')
    def empty(self, p):
        pass

    # update 语句的规则实现
    @_('UPDATE identifier SET update_parameter_list',
       'UPDATE identifier SET update_parameter_list WHERE expr')
    def update(self, p):
        where = getattr(p, 'expr', None)
        return Update(
            table=p.identifier,
            columns=p.update_parameter_list,
            where=where
        )

    # update t1 set a = 1, b = 2, ...
    @_('update_parameter',  # 可以透传到该规则上
       'update_parameter_list COMMA update_parameter')
    def update_parameter_list(self, p):
        params = getattr(p, 'update_parameter_list', {})
        params.update(p.update_parameter)
        return params

    # update t1 set a = 1;
    @_('expr EQ expr')
    def update_parameter(self, p):
        return {p.expr0: p.expr1}

    # delete 语句
    @_('DELETE FROM from_table WHERE expr',
       'DELETE FROM from_table')
    def delete(self, p):
        where = getattr(p, 'expr', None)

        if where and not isinstance(where, Operation):
            raise SyntaxError(
                f"WHERE clause must contain boolean condition not: {str(where)}")

        return Delete(table=p.from_table, where=where)

    # insert 语句
    @_('INSERT INTO from_table LPAREN target_columns RPAREN VALUES expr_list_set',
       'INSERT INTO from_table VALUES expr_list_set')
    def insert(self, p):
        columns = getattr(p, 'target_columns', None)
        return Insert(table=p.from_table, columns=columns, values=p.expr_list_set)

    @_('expr_list_set COMMA expr_list_set')
    def expr_list_set(self, p):
        return p.expr_list_set0 + p.expr_list_set1

    @_('LPAREN expr_list RPAREN')
    def expr_list_set(self, p):
        return [p.expr_list]

    # DDL
    # create table
    # e.g., CREATE TABLE t1 (id int, name text, gender int)
    @_('defined_columns COMMA defined_column')
    def defined_columns(self, p):
        p.defined_columns.append(p.defined_column)
        return p.defined_columns

    @_('defined_column')
    def defined_columns(self, p):
        return [p.defined_column]

    @_('id id')
    def defined_column(self, p):
        return [p.id0, p.id1]

    @_('CREATE TABLE identifier LPAREN defined_columns RPAREN')
    def create_table(self, p):
        return CreateTable(p.identifier, p.defined_columns)

    @_('CREATE INDEX identifier ON identifier LPAREN target_columns RPAREN')
    def create_index(self, p):
        return CreateIndex(
            index=p.identifier0, table=p.identifier1,
            columns=p.target_columns
        )

    def error(self, p):
        if p:
            raise SyntaxError(f'Syntax error at token {p.type}: "{p.value}".')
        else:
            raise SyntaxError("Syntax error at end of file.")


lexer = SQLLexer()
parser = SQLParser()


def query_parse(sql_stmt):
    try:
        tokens = lexer.tokenize(sql_stmt)
        return parser.parse(tokens)
    except (sly.lex.LexError, SyntaxError) as e:
        raise NoticeError(e)

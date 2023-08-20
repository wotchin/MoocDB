import re
import sly


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
        DATABASE, TABLE, INDEX

        # others
        EXPLAIN

        # select
        SELECT, STAR, FROM, WHERE, GROUP_BY, ORDER_BY,
        JOIN, FULL, INNER, OUTER, LEFT, RIGTH, ON,

        # DML: INSERT, UPDATE, DELETE
        INSERT, DELETE, INTO, VALUES, UPDATE, SET,

        # punctuation
        DOT, COMMA, LPAREN, RPAREN,

        # operators
        EQ, NE, GT, GEQ, LT, LEQ,
        AND, OR, NOT,

        # data type
        ID,
        INTERGER, QUOTE_STRING, DQUOT_STRING, NULL,
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

    @_(r'[a-zA-Z_][a-zA-Z0-9_]*')
    def ID(self, t):
        return t

    QUOTE_STRING = r"'[^']*'"
    DQUOT_STRING = r'"[^"]*"'


# select a, b from t1;
# select a, b from t1 where a > c;
# select a, b from t1 where a > c order by b;
# select a, count(a) from t1 group by a where b > 100;
from .ast import Select, Star

class SQLParser(sly.Parser):
    # select *;  -> Select([Star()])

    @_('SELECT target_columns')
    def select(self, p):
        return Select(targets=p.target_columns)

    @_('target_columns COMMA target_column')
    def target_columns(self, p):
        p.target_columns.append(p.target_column)

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
        return p.p[0]

    @_('')
    def expr(self, p):
        pass


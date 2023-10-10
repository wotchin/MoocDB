class ASTNode:
    def __init__(self):
        pass

    def __repr__(self):
        # Java toString() 方法是类似的
        fields = []
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            fields.append(f'{k}={v}')
        return f'<{self.__class__.__name__}> {" ".join(fields)}'


class Select(ASTNode):
    def __init__(self, targets):
        super().__init__()
        self.targets = targets
        self.from_table = None
        # 下面这些子句 (clause)，也是继承了ASTNode
        self.where = None
        self.group_by = None
        self.order_by = None
        # 如果我们后面想要支持更多语法元素，在下面添加新的字段（属性）就可以了
        # 例如，self.distinct = True


class OrderBy(ASTNode):
    def __init__(self, column, direction):
        super().__init__()
        self.column = column
        self.direction = direction


class JoinType:
    LEFT_JOIN = 'LEFT JOIN'
    RIGHT_JOIN = 'RIGHT JOIN'
    INNER_JOIN = 'INNER JOIN'
    FULL_JOIN = 'FULL JOIN'
    CROSS_JOIN = 'CROSS JOIN'  # 笛卡尔积


class Join(ASTNode):
    def __init__(self, left, right, join_type, condition=None):
        super().__init__()
        self.left = left
        self.right = right
        self.join_type = join_type
        self.condition = condition


class Identifier(ASTNode):
    def __init__(self, parts=None):
        super().__init__()
        self.parts = parts


class Star(ASTNode):
    pass


class Constant(ASTNode):
    def __init__(self, value=None):
        super().__init__()
        self.value = value


class Operation(ASTNode):
    def __init__(self, op, args):
        super().__init__()
        self.op = op.lower()  # TODO: 最好后面实现把结果表达更干净，删除空格之类的
        self.args = list(args)


class BinaryOperation(Operation):
    pass


class FunctionOperation(Operation):
    pass


class Update(ASTNode):
    def __init__(self, table, columns, where):
        super().__init__()

        # update t1 set a = 1 where b > 100;
        self.table = table
        assert isinstance(columns, dict)

        self.columns = columns
        self.where = where


class Insert(ASTNode):
    def __init__(self, table, columns, values):
        # 不支持类似语句：
        # 'insert into t1 select * from t2;'
        super().__init__()
        self.table = table
        self.columns = columns
        self.values = values


class Delete(ASTNode):
    def __init__(self, table, where):
        super().__init__()
        self.table = table
        self.where = where


class CreateTable(ASTNode):
    def __init__(self, table, columns):
        super().__init__()
        self.table = table
        self.columns = columns


class CreateIndex(ASTNode):
    def __init__(self, index, table, columns):
        super().__init__()
        self.index = index
        self.table = table
        self.columns = columns


class Command(ASTNode):
    def __init__(self, command, args=None):
        super().__init__()
        self.command = command.strip().upper()
        self.args = args


class Explain(ASTNode):
    def __init__(self, sql):
        super().__init__()
        self.sql = sql

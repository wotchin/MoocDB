class TableColumn:
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name

    def __lt__(self, other):
        if not isinstance(other, TableColumn):
            raise TypeError('error type')
        if self.table_name != other.table_name:
            return self.table_name.__lt__(other.table_name)
        else:
            return self.column_name.__lt__(other.column_name)

    def __repr__(self):
        # toString()
        return f'{self.table_name}.{self.column_name}'

    def __eq__(self, other):
        # equals()
        if not isinstance(other, TableColumn):
            return False
        # 下面的比较过程，会直接调用 __repr__，相当于进行了字符串比较
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.table_name, self.column_name))


class FunctionColumn:
    def __init__(self, function_name, *args):
        self.function_name = function_name
        self.args = tuple(args)
        for arg in self.args:
            assert isinstance(arg, TableColumn)

    def __eq__(self, other):
        # equals()
        if not isinstance(other, FunctionColumn):
            return False
        # 下面的比较过程，会直接调用 __repr__，相当于进行了字符串比较
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.function_name,) + self.args)

    def __repr__(self):
        # toString()
        return f'{self.function_name}{self.args}'

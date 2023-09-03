import prettytable


class Result:
    def __init__(self, target_columns=None):
        self.rows = []  # 其是就是返回个各个tuples
        # 为我们未来返回一些连带的信息留空位
        # 所以我们用一个对象来返回结果，而不是直接返回一个 list of tuple
        self.target_columns = target_columns

    def add_row(self, row):
        self.rows.append(row)

    def to_pretty_string(self):
        if not self.target_columns:
            return f'(Rows {len(self.rows)})'
        field_names = [str(column) for column in self.target_columns]
        p = prettytable.PrettyTable(field_names=field_names)
        for r in self.rows:
            p.add_row(r)
        return f'{str(p)}\n(Rows {len(self.rows)})'

    def __repr__(self):
        return self.to_pretty_string()


def exec_plan(physical_plan) -> Result:
    physical_plan.open()
    result = Result(physical_plan.columns)
    for row in physical_plan.next():
        result.add_row(row)
    physical_plan.close()

    return result

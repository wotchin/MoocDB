class CatalogForm:
    pass


class CatalogBasic:

    def __init__(self, name):
        self.name = name
        self.rows = []

    def insert(self, row: CatalogForm):
        self.rows.append(row)
        # todo: 是否rows里面的结果需要排序呢？

    def delete(self, lambda_condition):
        # 在列表中删除元素，应该怎么写代码呢？
        # 这样写行不行？不行，大家可以试试
        # for i, r in enumerate(self.rows):
        #     if lambda_condition(r):
        #         self.rows.pop(i)

        i = 0
        while i < len(self.rows):
            if lambda_condition(self.rows[i]):
                self.rows.pop(i)
                i -= 1
            i += 1

    def select(self, lambda_condition):
        results = []
        for r in self.rows:
            if lambda_condition(r):
                results.append(r)
        return results

    def update(self, lambda_condition):
        pass


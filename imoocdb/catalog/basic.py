import pickle
import os

from imoocdb.constant import CATALOG_DIRECTORY


class CatalogForm:
    pass


class CatalogBasic:

    def __init__(self, name):
        self.name = name
        self.rows = []

    def insert(self, row: CatalogForm):
        self.rows.append(row)
        # todo: 是否rows里面的结果需要排序呢？
        self.dump()

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
        self.dump()

    def select(self, lambda_condition):
        results = []
        for r in self.rows:
            if lambda_condition(r):
                results.append(r)
        return results

    def load(self):
        filename = os.path.join(CATALOG_DIRECTORY, self.name)
        if not os.path.exists(filename):
            return
        
        data = bytearray()
        with open(filename, 'rb') as f:
            while True:
                buff = f.read(256)
                if not buff:
                    break
                data += buff
        self.rows = pickle.loads(data)

    def dump(self):
        if not os.path.exists(CATALOG_DIRECTORY):
            os.mkdir(CATALOG_DIRECTORY)

        filename = os.path.join(CATALOG_DIRECTORY, self.name)
        with open(filename, 'w+b') as f:
            f.write(pickle.dumps(self.rows))
            # 虽然有代价，但是一定要保证数据能够完全刷到磁盘上
            os.fsync(f.fileno())

import pickle
import os

from imoocdb.constant import UNDOLOG_DIRECTORY

LITTLE_ORDER = 'little'


class UndoOperation:
    BEGIN = 0
    TABLE_INSERT = 1
    TABLE_DELETE = 2
    TABLE_UPDATE = 3
    COMMIT = 4
    ABORT = 5
    INDEX_INSERT = 6
    INDEX_DELETE = 7
    # 其他的，还可以包括：
    # table schema 表结构的变化
    # ...


class UndoRecord:
    def __init__(self, xid, operation, relation, location, data: bytes):
        # undo 的关键信息三元组 <T, X, v>
        # 意思是，如果事务T改变了数据库元素X，
        # 上述元组必须在新值写到磁盘之前写到磁盘
        # undo的commit 信息要在所有必要数据写到磁盘之后再写
        self.xid = xid
        self.operation = operation
        self.relation = relation
        self.location = location
        self.data = data

        tup = (xid, operation, relation, location, data)
        self._bytes = pickle.dumps(tup)

    def to_bytes(self):
        # 序列化
        # content_size 是固定的8字节，用于表示对应 buffer 读取的长度
        # 否则，多个record 序列化之后，你就分不清要读多长的bytes进行反序列化了
        content_size = len(self._bytes) + 8
        return (
                int.to_bytes(content_size, 8, LITTLE_ORDER, signed=False) +
                self._bytes
        )

    @staticmethod
    def from_bytes(buff):
        content_size = int.from_bytes(buff[:8], LITTLE_ORDER, signed=False)
        tup = pickle.loads(buff[8:])
        assert content_size == len(buff)
        return UndoRecord(*tup)

    def __len__(self):
        return len(self._bytes) + 8

    def __repr__(self):
        return f'<Undo: {self.xid} {self.operation}>'


class UndoLogManager:
    def __init__(self, file_directory=UNDOLOG_DIRECTORY):
        self.file_directory = file_directory
        self.active_transactions = {}

    def write(self, record: UndoRecord):
        xid = record.xid
        assert xid in self.active_transactions
        self.active_transactions[xid].append(record)

    def flush(self, xid):
        if not os.path.exists(self.file_directory):
            os.mkdir(self.file_directory)

        filename = os.path.join(self.file_directory, str(xid))
        with open(filename, 'ab') as f:
            # undo log 也是可以做到批量刷新来提高性能的
            for record in self.active_transactions[xid]:
                f.write(record.to_bytes())
            os.fsync(f.fileno())
        self.active_transactions[xid].clear()

    def start_transaction(self, xid):
        self.active_transactions[xid] = [
            UndoRecord(xid,
                       operation=UndoOperation.BEGIN,
                       relation=None, location=None, data=b'')
        ]

    def commit_transaction(self, xid):
        undo_record = UndoRecord(
            xid,
            operation=UndoOperation.COMMIT,
            relation=None, location=None, data=b''
        )
        self.write(undo_record)
        self.flush(xid)
        del self.active_transactions[xid]

    def abort_transaction(self, xid):
        undo_record = UndoRecord(
            xid,
            operation=UndoOperation.ABORT,
            relation=None, location=None, data=b''
        )
        self.write(undo_record)
        self.flush(xid)
        del self.active_transactions[xid]

    def parse_record(self, xid):
        filename = os.path.join(self.file_directory, str(xid))
        undo_records = []
        with open(filename, 'rb') as f:
            while True:
                buff = f.read(8)
                if len(buff) == 0:
                    break
                content_size = int.from_bytes(buff, LITTLE_ORDER, signed=False)
                buff += f.read(content_size - 8)
                record = UndoRecord.from_bytes(buff)
                undo_records.append(record)
        # [1,2,3] --reverse --> [3,2,1]
        undo_records.reverse()

        for record in undo_records:
            yield record


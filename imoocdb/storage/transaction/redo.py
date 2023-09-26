import os
import pickle

from imoocdb.constant import REDOLOG_FILENAME

LITTLE_ORDER = 'little'
REDOLOG_BUFFER_SIZE = 10


class RedoAction:
    BEGIN = 0
    COMMIT = 1
    ABORT = 2
    TABLE_INSERT = 3
    TABLE_DELETE = 4
    TABLE_UPDATE = 5
    INDEX_INSERT = 6
    INDEX_DELETE = 7
    INDEX_UPDATE = 8

    # 其他的
    CHECKPOINT = 9
    # undo log 的操作
    # ...
    # 系统表/数据字典的修改 catalog


class RedoRecord:
    def __init__(self, xid, action, relation, location, data: bytes):
        self.xid = xid
        self.action = action
        self.relation = relation
        self.location = location
        self.data = data

        tup = (xid, action, relation, location, data)
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
        return RedoRecord(*tup)

    def __len__(self):
        # len(RedoRecord)
        return len(self._bytes) + 8

    def __repr__(self):
        # str(RedoRecord)
        return f'<Redo: {self.xid} {self.action}>'


class RedoLogManager:
    def __init__(self, filename=REDOLOG_FILENAME):
        self.log_filename = filename
        # 重要：redo log buffer, 用来缓存 RedoRecord 的
        self.log_buffer = []
        # 所有事务写入LSN，包括内存 (log_buffer) 中的部分
        self.write_lsn = 0
        # 只统计，落到磁盘里面的LSN
        self.flush_lsn = 0

    def max_lsn(self):
        return self.write_lsn

    def write(self, record: RedoRecord):
        # todo: 加个锁，防止并发问题
        self.log_buffer.append(record)
        self.write_lsn += len(record)

        # 刷log buffer
        if (record.action == RedoAction.COMMIT or
                len(self.log_buffer) > REDOLOG_BUFFER_SIZE
        ):
            self.flush()

        return self.write_lsn

    def flush(self):
        with open(self.log_filename, 'ab') as f:
            for record in self.log_buffer:
                f.write(record.to_bytes())
                self.flush_lsn += len(record)
            # 要确保redo log 真的都落到磁盘里面
            # f.flush() 不行
            os.fsync(f.fileno())
        self.log_buffer.clear()

    @staticmethod
    def replay(filename=REDOLOG_FILENAME, start_lsn=0):
        # 用于解析 filename 文件，并产生对应的 RedoRecord
        # 从 start_lsn 开始
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                f.seek(start_lsn)

                while True:
                    # 先读取8个字节，用来判断，我们接下来要读
                    # 的字节流长度
                    buff = f.read(8)
                    # 如果读取不出来，那么就意味着，到达文件尾部了
                    # 退出
                    if len(buff) == 0:
                        break

                    content_size = int.from_bytes(buff, LITTLE_ORDER, signed=False)
                    buff += f.read(content_size - 8)
                    record = RedoRecord.from_bytes(buff)
                    yield record



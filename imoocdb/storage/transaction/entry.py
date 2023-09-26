import os
import threading

from imoocdb.storage.bplus_tree import BPlusTree, load_root_node, BPlusTreeTuple
from imoocdb.storage.common import table_tuple_get_page, get_index_filename, sync_table_page
from imoocdb.storage.lru import buffer_pool
from imoocdb.storage.transaction.redo import RedoLogManager, RedoRecord, RedoAction
from imoocdb.storage.transaction.undo import UndoLogManager, UndoOperation

INVALID_XID = -1


def checkpoint():
    # 注意：checkpoint 过程的时候，数据页不能进行修改
    # 不然，我们checkpoint 落到磁盘中的数据，会存在中间态
    # checkpoint 要有锁
    # todo: 实现锁
    transaction_mgr.redo_mgr.write(
        RedoRecord(
            INVALID_XID, RedoAction.CHECKPOINT, None, None, b''
        )
    )

    # 接着，我们要把脏页识别出来，然后把他们刷到磁盘中
    for key, page in buffer_pool.get_all_dirty_pages():
        relation, pageno = key
        sync_table_page(relation, pageno, page)
        buffer_pool.unmark_dirty(key)
        if key in buffer_pool.lru_cache.evicted:
            pass


class TransactionManager:
    def __init__(self):
        self.redo_mgr = RedoLogManager()
        self.undo_mgr = UndoLogManager()
        self.current_xid = 0
        # 线程的 thread local 变量
        self.thread_local = threading.local()
        self.allocation_mutex = threading.Lock()

    def session_xid(self):
        if not hasattr(self.thread_local, "xid"):
            return INVALID_XID

        return self.thread_local.xid

    def recovery(self):
        # 先读取 redo log的内容，找最后一次进行 checkpoint 的位置
        # 从checkpoint 的位置，进行重放(replay), 重放完之后，会有
        # 没有提交的事务被我们发现，
        # 我们就可以利用 undo log 对这些没有提交的事务进行回滚了
        if not os.path.exists(self.redo_mgr.log_filename):
            flush_lsn = 0
        else:
            flush_lsn = os.stat(self.redo_mgr.log_filename).st_size
        self.redo_mgr.flush_lsn = flush_lsn
        self.redo_mgr.write_lsn = flush_lsn

        checkpoint_lsn = 0
        replay_lsn = 0
        for redo_record in self.redo_mgr.replay():
            # 我们是先加的 LSN，意味着，拿到的这个LSN
            # 对应的是 redo record 的 tail 位置
            replay_lsn += len(redo_record)
            if redo_record.action == RedoAction.CHECKPOINT:
                checkpoint_lsn = replay_lsn

        # 此时，我们要么，找到一个最后的 checkpoint_lsn，要么没有找到
        # 没有找到，意味着一次 checkpoint 都没有做，那 checkpoint_lsn 本身
        # 就是0
        replay_lsn = checkpoint_lsn
        transactions = []
        for redo_record in self.redo_mgr.replay(start_lsn=checkpoint_lsn):
            replay_lsn += len(redo_record)

            xid = redo_record.xid
            action = redo_record.action
            relation = redo_record.relation
            location = redo_record.location
            data = redo_record.data

            if action == RedoAction.BEGIN:
                self.current_xid = xid
                transactions.append(xid)
            elif action == RedoAction.TABLE_INSERT:
                pageno, sid = location
                page = table_tuple_get_page(relation, pageno)
                # 重要：比较header中的LSN 大小，来判断是否应用该redo log
                # 如果该 redo log 的 LSN 比该page的大，那么有资格应用到
                # 该page上，否则，意味着该page本身就不旧于该redo log
                if page.page_header.lsn < replay_lsn:
                    new_sid = page.insert(data)
                    page.set_header(replay_lsn)
                    assert new_sid == sid
            elif action == RedoAction.TABLE_DELETE:
                pageno, sid = location
                page = table_tuple_get_page(relation, pageno)
                if page.page_header.lsn < replay_lsn:
                    page.delete(sid)
                    page.set_header(replay_lsn)
            elif action == RedoAction.TABLE_UPDATE:
                pageno, sid = location
                page = table_tuple_get_page(relation, pageno)
                if page.page_header.lsn < replay_lsn:
                    page.update(sid, data)
                    page.set_header(replay_lsn)
            elif action == RedoAction.ABORT:
                self.perform_undo(xid, replay_lsn)
            elif action == RedoAction.COMMIT:
                transactions.remove(xid)

        # redo 日志都重放完之后，存在一部分事务没有提交的场景，也就是
        # 这些redo 日志没有写 commit 标记，那么，我们应该把这些事务回滚
        for xid in transactions:
            lsn = self.redo_mgr.write(RedoRecord(xid,
                                                 RedoAction.ABORT,
                                                 None, None, b''))
            self.perform_undo(xid, lsn)

    def start_transaction(self) -> int:
        with self.allocation_mutex:
            self.current_xid += 1
            self.thread_local.xid = self.current_xid
            self.undo_mgr.start_transaction(self.thread_local.xid)
            self.redo_mgr.write(RedoRecord(self.thread_local.xid, RedoAction.BEGIN,
                                           None, None, b''))
            return self.current_xid

    def commit_transaction(self, xid):
        # 这个写入顺序是有讲究的：
        # 1. 先把 undo log 刷盘
        # 2. 再确保文件修改的内容写出 (redo log)
        # 3. 最后，再把 undo 的 commit 信息写出

        self.undo_mgr.flush(xid)
        self.redo_mgr.write(RedoRecord(self.thread_local.xid, RedoAction.COMMIT,
                                       None, None, b''))
        self.undo_mgr.commit_transaction(xid)

    def abort_transaction(self, xid):
        lsn = self.redo_mgr.write(RedoRecord(xid, RedoAction.ABORT,
                                             None, None, b''))
        self.undo_mgr.flush(xid)

        # 数据页在内存中回滚
        self.perform_undo(xid, lsn)
        self.undo_mgr.abort_transaction(xid)

    def get_current_lsn(self):
        return self.redo_mgr.max_lsn()

    def perform_undo(self, xid, lsn):
        # 这些record本身就已经是从文件尾部往前读取的了，因为做过了reverse
        for undo_record in self.undo_mgr.parse_record(xid):
            if undo_record.operation == UndoOperation.TABLE_DELETE:
                pageno, sid = undo_record.location
                page = table_tuple_get_page(undo_record.relation, pageno)
                page.delete(sid)
                page.set_header(lsn)
                # todo: buffer 标记为脏页
                buffer_pool.mark_dirty((undo_record.relation, pageno))
            elif undo_record.operation == UndoOperation.TABLE_INSERT:
                pageno, sid = undo_record.location
                page = table_tuple_get_page(undo_record.relation, pageno)
                page.insert(undo_record.data)
                page.set_header(lsn)
                buffer_pool.mark_dirty((undo_record.relation, pageno))
            elif undo_record.operation == UndoOperation.TABLE_UPDATE:
                pageno, sid = undo_record.location
                page = table_tuple_get_page(undo_record.relation, pageno)
                page.update(sid, undo_record.data)
                page.set_header(lsn)
                buffer_pool.mark_dirty((undo_record.relation, pageno))
            elif undo_record.operation == UndoOperation.INDEX_INSERT:
                index_name = undo_record.relation
                key = undo_record.data
                value = undo_record.location
                filename = get_index_filename(index_name)
                tree = BPlusTree(filename, load_root_node(filename))
                tree.insert(BPlusTreeTuple(key), value)
                tree.serialize()
            elif undo_record.operation == UndoOperation.INDEX_DELETE:
                index_name = undo_record.relation
                key = undo_record.data
                value = undo_record.location
                filename = get_index_filename(index_name)
                tree = BPlusTree(filename, load_root_node(filename))
                tree.delete(key, value)
                tree.serialize()


transaction_mgr = TransactionManager()

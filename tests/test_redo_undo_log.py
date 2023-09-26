import os

from imoocdb.constant import REDOLOG_FILENAME, UNDOLOG_DIRECTORY
from imoocdb.storage.transaction.redo import RedoRecord, RedoAction, RedoLogManager
from imoocdb.storage.transaction.undo import UndoLogManager, UndoOperation, UndoRecord


def test_redo():
    if os.path.exists(REDOLOG_FILENAME):
        os.unlink(REDOLOG_FILENAME)
    redo_manager = RedoLogManager()
    redo_manager.write(RedoRecord(0, RedoAction.BEGIN, None, None, b''))
    redo_manager.write(RedoRecord(1, RedoAction.BEGIN, None, None, b''))
    redo_manager.write(RedoRecord(0, RedoAction.TABLE_INSERT, 't1', (0, 1), b'hello'))
    redo_manager.write(RedoRecord(1, RedoAction.TABLE_UPDATE, 't1', (0, 1), b'foo'))
    redo_manager.write(RedoRecord(0, RedoAction.TABLE_INSERT, 't1', (0, 2), b'hello'))
    redo_manager.write(RedoRecord(0, RedoAction.COMMIT, None, None, b''))
    redo_manager.write(RedoRecord(1, RedoAction.COMMIT, None, None, b''))

    redo_manager.flush()

    records = list(redo_manager.replay())
    assert str(records) == \
           '[<Redo: 0 0>, <Redo: 1 0>, <Redo: 0 3>, <Redo: 1 5>, <Redo: 0 3>, <Redo: 0 1>, <Redo: 1 1>]'


def test_undo():
    if os.path.exists(UNDOLOG_DIRECTORY):
        import shutil
        shutil.rmtree(UNDOLOG_DIRECTORY)

    undo_manager = UndoLogManager()
    undo_manager.start_transaction(0)
    undo_manager.write(UndoRecord(0, UndoOperation.TABLE_DELETE, 't1', (0, 1),  b'hello'))
    undo_manager.start_transaction(1)
    undo_manager.write(UndoRecord(0, UndoOperation.INDEX_INSERT, 't1', (0, 1), b'hello'))
    undo_manager.commit_transaction(0)
    undo_manager.abort_transaction(1)
    assert (str(list(undo_manager.parse_record(0)))) == '[<Undo: 0 4>, <Undo: 0 6>, <Undo: 0 2>, <Undo: 0 0>]'
    assert (str(list(undo_manager.parse_record(1)))) == '[<Undo: 1 5>, <Undo: 1 0>]'

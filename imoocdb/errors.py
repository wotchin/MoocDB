class RollbackError(Exception):
    pass


class NoticeError(Exception):
    pass


class SQLLogicalPlanError(NoticeError):
    pass


class PageError(RollbackError):
    pass


class LRUError(RollbackError):
    pass


class BPlusTreeError(RollbackError):
    pass


class ExecutorCheckError(NoticeError):
    pass


class LockConflictError(RollbackError):
    pass

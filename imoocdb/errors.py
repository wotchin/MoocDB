class RollbackError(Exception):
    pass


class NoticeError(Exception):
    pass


class SQLLogicalPlanError(NoticeError):
    pass


class ExecutorCheckError(NoticeError):
    pass


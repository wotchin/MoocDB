import threading
import time

from imoocdb.errors import LockConflictError

lock_wait_timeout = 1  # unit: second


def if_only_duplicated_elements(l, e):
    return set(l) == {e}


class LockManager:
    def __init__(self):
        self.locks = {}
        self.lock_mutex = threading.Lock()

    def _acquire_lock(self, resource, xid, mode):
        with self.lock_mutex:
            if resource not in self.locks:
                self.locks[resource] = {'lock_mode': mode, 'holders': [xid]}
                success = True
            elif self.locks[resource]['lock_mode'] == 's' and mode == 's':
                self.locks[resource]['holders'].append(xid)
                success = True
            elif self.locks[resource]['lock_mode'] == 's' and mode == 'x':
                if if_only_duplicated_elements(self.locks[resource]['holders'], xid):
                    self.locks[resource]['lock_mode'] = 'x'
                    self.locks[resource]['holders'].append(xid)
                    success = True
                else:
                    success = False
            elif self.locks[resource]['lock_mode'] == 'x' and mode == 's':
                if if_only_duplicated_elements(self.locks[resource]['holders'], xid):
                    self.locks[resource]['holders'].append(xid)
                    success = True
                else:
                    success = False
            else:
                success = False
            return success

    def acquire_lock(self, resource, xid, mode):
        assert mode in ('x', 's')
        # "锁"是对二元组 (xid, resource) 进行跟踪的

        # 已经持有    希望加锁     能否成功
        #   s           s            Y
        #   s           x          如果是事务自己Y, 但是锁升级，其他事务N
        #   x           s          事务自己Y，其他N
        #   x           x          N
        success = False
        try_count = 0

        while not success and try_count < 2:
            # 尝试加锁
            success = self._acquire_lock(resource, xid, mode)

            if not success:
                time.sleep(lock_wait_timeout)
                try_count += 1

        if not success:
            raise LockConflictError(f'lock conflicts while {xid} '
                                    f'wants to get {resource} with {mode}.')

    def release_lock(self, resource, xid):
        with self.lock_mutex:
            if resource in self.locks and xid in self.locks[resource]['holders']:
                self.locks[resource]['holders'].remove(xid)
                if not self.locks[resource]['holders']:
                    del self.locks[resource]


lock_manager = LockManager()

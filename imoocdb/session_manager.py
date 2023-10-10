import threading

thread_local = threading.local()


def set_session_parameter(k, v):
    setattr(thread_local, k, v)


def reset_session_parameter(k):
    delattr(thread_local, k)


def get_session_parameter(k):
    return getattr(thread_local, k, None)


def get_current_session_id():
    session_id = get_session_parameter('id')
    if session_id is None:
        return -1
    return session_id


lsn = 0


def get_current_lsn():
    # mock
    global lsn
    lsn += 1
    return lsn


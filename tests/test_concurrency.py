import threading
import time

from imoocdb.main import exec_imoocdb_query


def test_concurrency():
    exec_imoocdb_query('create table t3 (id int, name text)')
    threads = []
    for i in range(10):
        threads.append(
            threading.Thread(target=exec_imoocdb_query,
                             args=(f'insert into t3 values ({i}, {str(int(time.time()))})',))
        )
    for i in range(5):
        threads.append(
            threading.Thread(target=exec_imoocdb_query,
                             args=(f'delete from t3 where t3.id = {i}',))
        )
    for i in range(20):
        threads.append(
            threading.Thread(target=exec_imoocdb_query,
                             args=(f'select * from t3 where t3.id = {i}',))
        )

    for i in range(10):
        threads.append(
            threading.Thread(target=exec_imoocdb_query,
                             args=(f"update t3 set t3.name = 'updated' where t3.id = {i}",))
        )

    for th in threads:
        th.start()

    for th in threads:
        th.join(2)

    print(exec_imoocdb_query('select * from t3').rows)
    assert len(exec_imoocdb_query('select * from t3').rows) > 0

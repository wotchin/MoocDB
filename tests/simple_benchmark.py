import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
import subprocess

from imoocdb.main import start_simple_imoocdb_process

db_thread = None

sql_statements = [
    "insert into t1 values (1, 'a'), (2, 'b');",
    "select * from t1;",
    "select t1.name from t1 where t1.name != 'a';",
    "update t1 set t1.name = 'c' where t1.id > 1;",
    "select t1.name from t1 left join t2 on t1.id = t2.id where t2.name = 'a';",
    "delete from t1 where t1.name = 'a';",
    "delete from t1 where t1.name = 'c';"
]


def execute_query(sql):
    psql_command = [
        # 如果是windows，则使用下面的命令，这是默认安装路径
        # "C:\\Program Files\\PostgreSQL\\15\\bin\\psql.exe",  # psql 命令
        # 如果是MacOS或者是Linux，如果 psql 在环境变量里面，则可以直接调用
        "/Applications/Postgres.app/Contents/Versions/latest/bin/psql",
        "host=localhost port=54321 user=abc password=abcd dbname=imoocdb"
    ]
    p = subprocess.Popen(psql_command,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, )
    out, err = p.communicate(input=sql.encode())
    print(out, err)
    p.terminate()


def startup_benchmark():
    if os.path.exists('benchmark_tmp'):
        shutil.rmtree('benchmark_tmp')
    os.mkdir('benchmark_tmp')
    os.chdir('benchmark_tmp')

    # 运行数据库的主流程，让数据库服务可用
    global db_thread
    db_thread = threading.Thread(target=start_simple_imoocdb_process)
    db_thread.setDaemon(True)
    db_thread.start()

    execute_query('create table t1 (id int, name text);')
    execute_query('create table t2 (id int, name text);')
    execute_query("insert into t2 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');")


def teardown_benchmark():
    pass


def run_benchmark(workers=100, iteration=1):
    executor = ThreadPoolExecutor(max_workers=workers)
    for i in range(iteration):
        futures = [executor.submit(execute_query, sql) for sql in sql_statements]
        start_time = time.monotonic()
        wait(futures)
        end_time = time.monotonic()
        print(f'iteration: {i} QPS: {len(sql_statements) / (end_time - start_time)}')
        # clear up
        execute_query('delete from t1;')
        execute_query('checkpoint;')


if __name__ == '__main__':
    startup_benchmark()
    run_benchmark(iteration=1)
    teardown_benchmark()

import os
import pickle

from imoocdb.constant import DATA_DIRECTORY
from imoocdb.storage.lru import buffer_pool
from imoocdb.storage.slotted_page import PAGE_SIZE, Page


def get_table_filename(table_name):
    if not os.path.exists(DATA_DIRECTORY):
        os.mkdir(DATA_DIRECTORY)
    return os.path.join(DATA_DIRECTORY, table_name + '.tbl')


def table_tuple_get_disk_pages(table_name):
    filename = get_table_filename(table_name)
    if not os.path.exists(filename):
        return 0

    if os.stat(filename).st_size > 0:
        assert os.stat(filename).st_size % PAGE_SIZE == 0
    return os.stat(filename).st_size // PAGE_SIZE


def table_tuple_get_pages(table_name):
    disk_pages = table_tuple_get_disk_pages(table_name)
    # pageno 从 0 开始，因此，衡量具体page数量的时候，要加1
    memory_pages = buffer_pool.find_max_pageno(table_name) + 1
    return max(memory_pages, disk_pages)


def table_tuple_get_page(table_name, pageno):
    key = (table_name, pageno)
    if key not in buffer_pool:
        # 是否磁盘里面已经包含了数据页，但是没有加载到内存中
        if pageno < table_tuple_get_disk_pages(table_name):
            # 意味着磁盘里面已经有该数据页了，需要先从磁盘里面加载
            # 到内存中
            filename = get_table_filename(table_name)
            if not os.path.exists(filename):
                return None

            with open(filename, 'rb') as f:
                f.seek(pageno * PAGE_SIZE)
                buff = f.read(PAGE_SIZE)
                page = Page.deserialize(buff)
            # 把数据页装载到磁盘里面
            buffer_pool[key] = page
        else:
            # 此时意味着，磁盘里面找不到对应的 pageno, 我们要创建出新的页来
            # 同时，别忘记了他是一个脏页
            buffer_pool[key] = Page()
            buffer_pool.mark_dirty(key)

    page = buffer_pool[key]
    return page


def tuple_to_bytes(tup):
    return pickle.dumps(tup)


def bytes_to_tuple(bytes_):
    if len(bytes_) == 0:
        return ()
    return pickle.loads(bytes_)


def get_index_filename(index_name):
    if not os.path.exists(DATA_DIRECTORY):
        os.mkdir(DATA_DIRECTORY)
    return os.path.join(DATA_DIRECTORY, index_name + '.idx')


def sync_table_page(table_name, pageno, page):
    filename = get_table_filename(table_name)
    with open(filename, 'ab') as f:
        f.seek(pageno * PAGE_SIZE)
        f.write(page.serialize())
        os.fsync(f.fileno())

from imoocdb.storage.slotted_page import PageHeader, Page


def test_page_header():
    page_header = PageHeader()
    page_header.flags = 0xff
    page_header.reserved = 1
    page_header.lsn = 123

    buff = page_header.serialize()

    page_header2 = PageHeader.deserialize(buff)
    assert page_header == page_header2


def test_slotted_page():
    page = Page()
    sid = page.insert(b'hello')
    page.set_header(1)
    assert sid == 0
    assert page.select(sid) == b'hello'
    sid = page.insert(b'world')
    page.set_header(2)
    assert page.select(sid) == b'world'

    page.delete(sid)
    page.set_header(3)
    assert page.select(sid) == b''

    new_sid = page.update(0, b'a')
    page.set_header(4)
    assert new_sid == 0
    assert page.select(new_sid) == b'a'

    sid = page.insert(b'b')
    page.set_header(2)
    assert page.select(sid) == b'b'
    new_sid = page.update(sid, b'xxxxxxxxxxxxxxx')
    page.set_header(2)
    assert new_sid > sid
    assert page.select(sid) == b''
    assert page.select(new_sid) == b'xxxxxxxxxxxxxxx'

    records = []
    for sid in range(0, len(page.slot_directory)):
        records.append((sid, page.select(sid)))

    buff = page.serialize()
    page2 = Page.deserialize(buff)

    records2 = []
    for sid in range(0, len(page2.slot_directory)):
        records2.append((sid, page2.select(sid)))

    assert records == records2
    assert buff == page2.serialize()


test_slotted_page()

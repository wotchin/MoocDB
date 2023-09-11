from imoocdb.errors import PageError

PAGE_SIZE = 8 * 1024  # 8kb
LITTLE_ORDER = 'little'


def uint8_to_bytes(value):
    return int.to_bytes(value, 8, LITTLE_ORDER, signed=False)


def bytes_to_uint8(buff):
    return int.from_bytes(buff, LITTLE_ORDER, signed=False)


class BaseStructure:
    def serialize(self) -> bytes:
        buff = bytearray()
        for field_name in self.serializable_fields():
            buff += (uint8_to_bytes(getattr(self, field_name)))
        return bytes(buff)

    @classmethod
    def size(cls):
        # 先这么写，避免添加字段后的霰弹式修改
        return len(cls.serializable_fields()) * 8

    @classmethod
    def serializable_fields(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, buff):
        assert len(buff) == cls.size()
        page_header = cls()
        position = 0
        for field_name in cls.serializable_fields():
            value = bytes_to_uint8(buff[position: position + 8])
            setattr(page_header, field_name, value)
            position += 8
        return page_header

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        for field_name in self.serializable_fields():
            if getattr(self, field_name) != getattr(other, field_name):
                return False
        return True

    def __hash__(self):
        values = tuple(
            getattr(self, field_name)
            for field_name in self.serializable_fields()
        )
        return hash(values)


class PageHeader(BaseStructure):
    def __init__(self):
        """Header 是定长的！！！"""
        # 如果不显性写明 PageNo，也可以通过 总的文件大小 / PAGE_SIZE 来获取
        # 我们后面再考虑是否显性地增加这个字段

        # self.pageno = 0
        # 来自 WAL (redo)，用于表示哪个WAL最后修改了这个页
        self.lsn = 0
        # 可能还有些其他字段，根据需要再添加
        # 如，crc 校验码之类的
        # 预留的标志字段
        self.flags = 0
        # 预留字段，为某些个性化信息，提供承载的地方
        self.reserved = 0

        # slotted page 可能会用到这两个字段，用于表示
        # 最后一个record和第一个记录的位置
        self.free_space_start = 0  # free space start: Postgres lower
        self.free_space_end = 0  # free space end: Postgres upper

    @classmethod
    def serializable_fields(cls):
        return 'lsn', 'flags', 'reserved', 'free_space_start', 'free_space_end'


class RecordState:
    UNUSED = 0
    NORMAL = 1
    DEAD = 2


class Slot(BaseStructure):
    def __init__(self):
        """Slot 是定长的！！！"""
        self.offset = 0
        self.length = 0
        self.state = 0  # 例如该state可以实现标记清除

    @classmethod
    def serializable_fields(cls):
        return 'offset', 'length', 'state'


class Page:
    def __init__(self, header=None):
        if header:
            self.page_header = header
        else:
            self.page_header = PageHeader()
        self.slot_directory = list()
        self.records = bytearray()  # 用于存放数据元组 tuple，或者index的key

    @property
    def total_record_size(self):
        return len(self.records)

    @property
    def total_slot_directory_size(self):
        return len(self.slot_directory) * Slot.size()

    def allocate_slot(self, record):
        # 我们在 allocate_slot 这个方法里面，没有修改任何状态
        # 调用者，别忘了自己来修改Page的状态！
        current_slot_num = len(self.slot_directory)
        total_slot_size = (current_slot_num + 1) * Slot.size()
        total_record_size = self.total_record_size + len(record)
        total_page_size = (self.page_header.size() +
                           total_slot_size +
                           total_record_size
                           )
        if total_page_size >= PAGE_SIZE:
            return None
        slot = Slot()
        # 这里面记录逻辑位置和物理位置都可以，
        # 我们暂时先用物理位置，因为，在Page这一层，
        # 我们还不知道 record 具体是什么对象类型，只知道他是一个 bytes
        # self.total_record_size 意思是当前 record 写入的起始位置
        slot.offset = self.total_record_size
        slot.length = len(record)
        slot.state = RecordState.UNUSED
        return slot

    def set_header(self, lsn):
        self.page_header.lsn = lsn
        self.page_header.free_space_start = (self.page_header.size() +
                                             self.total_slot_directory_size)
        self.page_header.free_space_end = PAGE_SIZE - len(self.records)

    def insert(self, record: bytes) -> int:
        slot = self.allocate_slot(record)
        if not slot:
            raise PageError('out of space in the page.')
        self.slot_directory.append(slot)
        self.records += record
        # 严格意义上，slotted page 的 新record 是加在最前面的，即：
        # self.records = (record + self.records)
        # 但是，我们可以通过total_size这个机制，实现逻辑等价
        slot.state = RecordState.NORMAL
        # 返回 slot 的下标，对于 堆表 来说，可以作为唯一的id，
        # 即 tid (tuple id)
        return len(self.slot_directory) - 1

    def delete(self, sid) -> bool:
        if sid >= len(self.slot_directory):
            raise PageError('invalid sid.')
        slot: Slot
        slot = self.slot_directory[sid]
        # 用到的是标记清除法，如果原地删除，对于我们的Page来讲，很简单
        # 但是，有一个场景会很麻烦：索引的更新，例如
        # 我们有一个元组 tid = 1, 那么，其他的元组id 可能是 2,3,4, ...
        # 如果说，直接把 tid = 1 的元组删了，空间页回收了，那么，其他的
        # 该元组后面的元组的 tid 也要对应 -1, 即 1,2,3, ...
        # 所以这样，对索引的更新就会工作量非常大
        slot.state = RecordState.DEAD
        return True

    def select(self, sid) -> bytes:
        if sid >= len(self.slot_directory):
            raise PageError('invalid sid.')
        slot: Slot
        slot = self.slot_directory[sid]
        # 由于我们采用了标记清除的机制，所以，我们此时要判断一下该标记
        if slot.state != RecordState.NORMAL:
            return bytes()
        record = bytes(self.records[slot.offset: slot.offset + slot.length])
        return record

    def update(self, sid, record: bytes) -> int:
        # 有两种实现方法：
        # 一种是先删除，再新增
        # 另一种是直接覆盖
        # 第二种：先判断一下，是否有足够的空间，用于覆盖
        slot = self.slot_directory[sid]
        if len(record) <= slot.length:
            self.records[slot.offset: slot.offset + len(record)] = record
            slot.length = len(record)
            slot.state = RecordState.NORMAL
            # 第二种方法，虽然做到了原地更新，但是还没有做完善，
            # 还有会一些空洞
            # 我们可以在此时，进行后续元素的整体搬移，也可以后续批量去做组织
            return sid
        # 第一种：
        slot = self.slot_directory[sid]
        old_state = slot.state
        try:
            self.delete(sid)
            new_sid = self.insert(record)
        except PageError as e:
            slot.state = old_state
            raise e
        return new_sid

    def serialize(self) -> bytes:
        free_space_size = (self.page_header.free_space_end -
                           self.page_header.free_space_start)
        assert PAGE_SIZE == (self.page_header.size() +
                             self.total_slot_directory_size +
                             free_space_size +
                             self.total_record_size)
        slot_directory_bytes = bytearray()
        for slot in self.slot_directory:
            slot_directory_bytes += slot.serialize()
        return (
                self.page_header.serialize() +
                bytes(slot_directory_bytes) +
                bytes(free_space_size) +
                bytes(self.records)
        )

    @staticmethod
    def deserialize(buff) -> "Page":
        header = PageHeader.deserialize(buff[:PageHeader.size()])
        page = Page(header)
        for slot_offset in range(header.size(), header.free_space_start, Slot.size()):
            slot = Slot.deserialize(buff[slot_offset: slot_offset + Slot.size()])
            page.slot_directory.append(slot)

        page.records = bytearray(buff[header.free_space_end:])
        return page

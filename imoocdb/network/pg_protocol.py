import io
import logging
import threading
import socketserver
import struct
from enum import Enum

# 实现兼容PG协议
# 基本上，网络协议都包括两大类部分
# 信令 (message type)：表示接下来的数据包是做哪个动作的  --- 谓语，什么动作
# 内容 (length + content)：该信令下的具体操作内容详情        --- 做了什么，宾语
# 例如说：吃 饭，吃 肉


from imoocdb.errors import NoticeError, RollbackError


class FeMessageType(Enum):
    PASSWORD_MESSAGE = b'p'
    QUERY = b'Q'
    TERMINATION = b'X'


class IOBuffer:
    def __init__(self, buffer=None):
        if not buffer:
            # 用 bytearray() 也ok
            buffer = io.BytesIO()
        self.buffer = buffer

    def read_byte(self):
        return self.buffer.read(1)

    def read_bytes(self, n):
        data = self.buffer.read(n)
        if not data:
            raise IOError('cannot read from buffer.')
        return data

    def read_int32(self):
        data = self.read_bytes(4)
        return struct.unpack('!i', data)[0]

    def read_int16(self):
        data = self.read_bytes(2)
        return struct.unpack('!H', data)[0]

    def read_parameters(self, n):
        data = self.read_bytes(n)
        return data.split(b'\x00')

    def write_bytes(self, value: bytes):
        self.buffer.write(value)
        self.buffer.flush()

    def write_int32(self, v):
        data = struct.pack('!i', v)
        self.write_bytes(data)

    def write_int16(self, v):
        data = struct.pack('!h', v)
        self.write_bytes(data)

    def write_string(self, v: '[str | bytes]'):
        if isinstance(v, str):
            bytes_content = v.encode()
        elif isinstance(v, bytes):
            bytes_content = v
        else:
            raise
        self.write_bytes(bytes_content)
        # bug点
        self.write_bytes(b'\x00')

    def to_bytes(self):
        return self.buffer.getvalue()

    def __bytes__(self):
        # bytes(x)
        return self.to_bytes()


# 参考下述连接，实现协议：
# https://www.postgresql.org/docs/current/protocol-flow.html
# https://www.postgresql.org/docs/current/protocol-message-formats.html
class Message:
    def __init__(self, buffer: IOBuffer):
        self.buffer = buffer

    def read(self, **kwargs):
        pass

    def write(self, **kwargs):
        pass


class StartupMessage(Message):
    def read(self):
        length = self.buffer.read_int32()
        version = self.buffer.read_int32()
        major, minor = version >> 16, version & 0xffff
        parameters = self.buffer.read_parameters(length - 8)
        return (major, minor), parameters


class SSLRequest(Message):
    def read(self, **kwargs):
        msglen = self.buffer.read_int32()
        sslcode = self.buffer.read_int32()
        return sslcode


class ErrorResponse(Message):
    def write(self, severity, code, message):
        buf = IOBuffer()
        buf.write_bytes(b'S')
        buf.write_string(severity)
        buf.write_bytes(b'C')
        buf.write_string(code)
        buf.write_bytes(b'M')
        buf.write_string(message)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(b'E')
        self.buffer.write_int32(4 + len(bytes_) + 1)
        self.buffer.write_string(bytes_)


class ClearPassword(Message):
    def read(self, **kwargs):
        length = self.buffer.read_int32()
        password = self.buffer.read_bytes(length - 4)
        return password


class AuthenticationOk(Message):
    def write(self, **kwargs):
        self.buffer.write_bytes(
            struct.pack("!cii", b'R', 8, 0)
        )


class AuthenticationCleartextPassword(Message):
    def write(self, **kwargs):
        self.buffer.write_bytes(
            struct.pack("!cii", b'R', 8, 3)
        )


class AuthenticationMD5Password(Message):
    pass


class ReadyForQuery(Message):
    def write(self, idle=True, failed=False):
        status_indicators = {
            (True, False): b'I',
            (True, True): b'I',
            (False, True): b'T',
            (False, False): b'E',
        }
        self.buffer.write_bytes(
            struct.pack("!cic", b'Z', 5, status_indicators[(idle, failed)])
        )


class NoticeResponse(Message):
    def write_none(self):
        self.buffer.write_bytes(b'N')

    def write(self, severity, code, message):
        buf = IOBuffer()
        buf.write_bytes(b'S')
        buf.write_string(severity)
        buf.write_bytes(b'C')
        buf.write_string(code)
        buf.write_bytes(b'M')
        buf.write_string(message)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(b'N')
        self.buffer.write_int32(4 + len(bytes_) + 1)
        self.buffer.write_string(bytes_)


class CommandComplete(Message):
    def write(self, tag: bytes):
        self.buffer.write_bytes(
            struct.pack("!ci", b'C', 4 + len(tag))
        )
        self.buffer.write_bytes(tag)


class RowDescription(Message):
    def write(self, fields):
        buf = IOBuffer()
        for field in fields:
            buf.write_string(field.name)
            buf.write_int32(0)
            buf.write_int16(0)
            buf.write_int32(field.oid)
            buf.write_int16(field.type_len)
            buf.write_int32(-1)
            buf.write_int16(0)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(
            struct.pack('!ciH', b'T', 6 + len(bytes_), len(fields))
        )
        self.buffer.write_bytes(bytes_)


class DataRow(Message):
    @staticmethod
    def _encode(v):
        if v is None:
            return b'null'
        return str(v).encode()

    def write(self, rows):
        for row in rows:
            buf = IOBuffer()
            for v in row:
                value_bytes = self._encode(v)
                # print(value_bytes)
                buf.write_int32(len(value_bytes))
                buf.write_bytes(value_bytes)
            bytes_ = buf.to_bytes()
            # print(bytes_, len(bytes_), len(row))
            self.buffer.write_bytes(
                struct.pack('!ciH', b'D', 4 + 2 + len(bytes_), len(row))
            )
            self.buffer.write_bytes(bytes_)


class QueryMessage(Message):
    def read(self, **kwargs):
        msglen = self.buffer.read_int32()
        sql = self.buffer.read_bytes(msglen - 4)
        return sql


class Field:
    def __init__(self, name, oid, type_len):
        self.name = name
        self.oid = oid
        self.type_len = type_len


class Int8Field(Field):
    def __init__(self, name):
        super().__init__(name, 20, 8)


class TextField(Field):
    def __init__(self, name):
        super().__init__(name, 25, -1)


class QueryResult(Message):
    def write(self, fields, rows):
        RowDescription(self.buffer).write(fields)
        DataRow(self.buffer).write(rows)
        CommandComplete(self.buffer).write(b'SELECT\x00')


class PGHandler(socketserver.StreamRequestHandler):
    def set_session_info(self, parameters):
        print('parameters', parameters)

    def check_password(self, password):
        print('password', password)
        return True

    def query(self, sql):
        # mock测试数据
        print('sql', sql)
        fields = [Int8Field('a'), TextField('b')]
        rows = [[1, 'a'], [3, None], [5, 'c']]
        return fields, rows

    def handle(self):
        r = IOBuffer(self.rfile)  # 来自用户的数据流
        w = IOBuffer(self.wfile)  # 返回给用户的数据流

        try:
            # SSL的请求响应实现有其他实现方法
            sslcode = SSLRequest(r).read()
            NoticeResponse(w).write_none()

            # 正式读取用户信息
            version, parameters = StartupMessage(r).read()
            assert version == (3, 0)
            self.set_session_info(parameters)

            AuthenticationCleartextPassword(w).write()
            message_type = r.read_byte()
            print('password message type', message_type)
            if message_type != FeMessageType.PASSWORD_MESSAGE.value:
                ErrorResponse(w).write('FATAL', '12345', 'invalid authorization')
                return

            password = ClearPassword(r).read()
            if self.check_password(password):
                AuthenticationOk(w).write()
            else:
                ErrorResponse(w).write('FATAL', '28000', 'invalid user/password')
                return

            # 如果执行到这里了，代表密码已经通过校验了，我们可以处理请求了
            while True:
                ReadyForQuery(w).write()

                message_type = r.read_byte()
                print('message type', message_type)

                if message_type == FeMessageType.QUERY.value:
                    sql = QueryMessage(r).read()
                    try:
                        fields, rows = self.query(sql)
                        QueryResult(w).write(fields, rows)
                    except RollbackError as e:
                        ErrorResponse(w).write('ERROR', '00001', str(e))
                    except NoticeError as e:
                        NoticeResponse(w).write('NOTICE', '00002', str(e))

                elif message_type == FeMessageType.TERMINATION.value:
                    break
                elif message_type == b'':
                    pass
                else:
                    raise NotImplementedError(f'unsupported messag type {message_type}')
        except ConnectionAbortedError as e:
            pass
        except ConnectionResetError as e:
            print('client exits.')
        except Exception as e:
            logging.exception(e)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_server(host, port, handler=PGHandler):
    server = Server((host, port), handler)
    server.serve_forever()


if __name__ == '__main__':
    start_server('localhost', 54321)

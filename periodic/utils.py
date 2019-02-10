import struct

NOOP           = b'\x00'
# for job
GRAB_JOB       = b'\x01'
SCHED_LATER    = b'\x02'
JOB_DONE       = b'\x03'
JOB_FAIL       = b'\x04'
JOB_ASSIGN     = b'\x05'
NO_JOB         = b'\x06'
# for func
CAN_DO         = b'\x07'
BROADCAST      = b'\x15'
CANT_DO        = b'\x08'
# for test
PING           = b'\x09'
PONG           = b'\x0A'
# other
SLEEP          = b'\x0B'
UNKNOWN        = b'\x0C'
# client command
SUBMIT_JOB     = b'\x0D'
STATUS         = b'\x0E'
DROP_FUNC      = b'\x0F'
REMOVE_JOB     = b'\x11'

RUN_JOB        = b'\x19'

ACQUIRED       = b'\x1A'
ACQUIRE        = b'\x1B'
RELEASE        = b'\x1C'

SUCCESS        = b'\x10'

MAGIC_REQUEST  = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# client type

TYPE_CLIENT    = b'\x01'
TYPE_WORKER    = b'\x02'


def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, 'utf-8')
    else:
        return bytes(str(s), 'utf-8')

def to_str(s):
    if isinstance(s, bytes):
        return str(s, 'utf-8')
    elif isinstance(s, str):
        return s
    else:
        return str(s)

def to_int(s):
    return int(s)


class ConnectionError(Exception):
    pass


class BaseClient(object):
    def __init__(self, sock):
        self._sock = sock
        self.msgid = None

    def recive(self):
        magic = self._sock.recv(4)
        if magic != MAGIC_RESPONSE:
            raise Exception('Magic not match.')

        head = self._sock.recv(4)
        length = decode_int32(head)

        payload = self._sock.recv(length)

        msgid = payload[0:4]

        if self.msgid != u:
            raise Exception('msgid not match')

        return payload[4:]

    def send(self, payload):
        if isinstance(payload, list):
            payload = [to_bytes(p) for p in payload]
            payload = b''.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')

        if self.msgid:
            payload = self.msgid + payload

        payload = encode_str32(payload)
        self._sock.send(MAGIC_REQUEST + payload)

    def close(self):
        self._sock.close()


def encode_str8(data = b''):
    return encode_int8(len(data)) + data

def encode_str32(data = b''):
    return encode_int32(len(data)) + data

def encode_int8(n = 0):
    return struct.pack('>B', n)

def encode_int16(n = 0):
    return struct.pack('>H', n)

def encode_int32(n = 0):
    return struct.pack('>I', n)

def encode_int64(n = 0):
    return struct.pack('>Q', n)

def decode_int8(n):
    return struct.unpack('>B', n)

def decode_int16(n):
    return struct.unpack('>H', n)

def decode_int32(n):
    return struct.unpack('>I', n)

def decode_int64(n):
    return struct.unpack('>Q', n)

def encode_job(job):
    return ''.join([
        encode_str8(job['func']),
        encode_str8(job['name']),
        encode_str32(job.get('workload', b'')),
        encode_int64(job.get('sched_at', 0)),
        encode_int32(job.get('count', 0))
    ])

def decode_job(payload):
    job = {}

    h = decode_int8(payload[0:1])
    job['func'] = payload[1:h + 1]

    payload = payload[h + 1:]

    h = decode_int8(payload[0:1])
    job['name'] = payload[1:h + 1]

    payload = payload[h + 1:]

    h = decode_int32(payload[0:4])
    job['workload'] = payload[4:h+4]
    payload = payload[h+4:]

    job['sched_at'] = decode_int64(payload[0:8])

    job['count'] = decode_int16(payload[8:12])
    return job

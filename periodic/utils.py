NOOP        = b"\x00"
# for job
GRAB_JOB    = b"\x01"
SCHED_LATER = b"\x02"
JOB_DONE    = b"\x03"
JOB_FAIL    = b"\x04"
WAIT_JOB    = b"\x05"
NO_JOB      = b"\x06"
# for func
CAN_DO      = b"\x07"
CANT_DO     = b"\x08"
# for test
PING        = b"\x09"
PONG        = b"\x0A"
# other
SLEEP       = b"\x0B"
UNKNOWN     = b"\x0C"
# client command
SUBMIT_JOB = b"\x0D"
STATUS = b"\x0E"
DROP_FUNC = b"\x0F"
REMOVE_JOB = b'\x11'

SUCCESS = b"\x10"

NULL_CHAR = b"\x00\x01"

MAGIC_REQUEST   = b"\x00REQ"
MAGIC_RESPONSE  = b"\x00RES"

# client type

TYPE_CLIENT = b"\x01"
TYPE_WORKER = b"\x02"

import uuid


def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, "utf-8")
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


def parseHeader(head):
    length = head[0] << 24 | head[1] << 16 | head[2] << 8 | head[3]
    length = length & ~0x80000000

    return length


def makeHeader(data):
    header = [0, 0, 0, 0]
    length = len(data)
    header[0] = chr(length >> 24 & 0xff)
    header[1] = chr(length >> 16 & 0xff)
    header[2] = chr(length >> 8 & 0xff)
    header[3] = chr(length >> 0 & 0xff)
    return bytes(''.join(header), 'utf-8')


class ConnectionError(Exception):
    pass


class BaseClient(object):
    def __init__(self, sock):
        self._sock = sock
        self.uuid = None

    def recive(self):
        print('recive start')
        magic = self._sock.recv(4)
        print('recive magic >>>:', magic)
        if magic != MAGIC_RESPONSE:
            raise Exception("Magic not match.")

        head = self._sock.recv(4)
        print('recive head >>>:', head)
        length = parseHeader(head)

        payload = self._sock.recv(length)
        print('recive payload >>>:', payload[1])
        payload = payload.split(NULL_CHAR, 1)
        u = uuid.UUID(bytes=payload[0])
        if self.uuid != u:
            raise Exception('msg id not match')
        print('recive >>>:', payload[1])
        print('recive end')
        return payload[1]

    def send(self, payload):
        if isinstance(payload, list):
            payload = [to_bytes(p) for p in payload]
            payload = NULL_CHAR.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')

        if self.uuid:
            u = self.uuid.bytes
            payload = u + NULL_CHAR + payload

        header = makeHeader(payload)
        self._sock.send(MAGIC_REQUEST)
        self._sock.send(header)
        self._sock.send(payload)

    def close(self):
        self._sock.close()

def encodeJob(job):
    ret = [to_bytes(job['func']), to_bytes(job['name'])]
    if job.get('workload') or job.get('count', 0) > 0 or job.get('sched_at', 0) > 0:
        ret.append(to_bytes(job.get('sched_at', 0)))
    if job.get('workload') or job.get('count', 0) > 0:
        ret.append(to_bytes(job.get('count', 0)))
    if job.get('workload'):
        ret.append(to_bytes(job.get('workload', b'')))
    return NULL_CHAR.join(ret)

def decodeJob(payload):
    parts = payload.split(NULL_CHAR, 4)
    size = len(parts)

    job = {
        'func': to_str(parts[0]),
        'name': to_str(parts[1]),
    }
    if size > 2:
      job['sched_at'] = to_int(parts[2])
    if size > 3:
      job['count'] = to_int(parts[3])
    if size > 4:
      job['workload'] = parts[4]
    return job

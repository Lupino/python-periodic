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

NULL_CHAR = b"\x01"

MAGIC_REQUEST   = b"\x00REQ"
MAGIC_RESPONSE  = b"\x00RES"

# client type

TYPE_CLIENT = b"\x01"
TYPE_WORKER = b"\x02"


def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, "utf-8")
    else:
        return bytes(s)


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
        magic = self._sock.recv(4)
        if magic != MAGIC_RESPONSE:
            raise Exception("Magic not match.")

        head = self._sock.recv(4)
        length = parseHeader(head)

        payload = self._sock.recv(length)
        payload = payload.split(NULL_CHAR, 1)
        uuid = uuid.UUID(bytes=payload[0])
        if self.uuid != uuid:
            raise Exception('msg id not match')
        return payload[1]

    def send(self, payload):
        if isinstance(payload, list):
            payload = [to_bytes(p) for p in payload]
            payload = NULL_CHAR.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')

        if self.uuid:
            uuid = self.uuid.bytes
            payload = uuid + NULL_CHAR + payload

        header = makeHeader(payload)
        self._sock.send(MAGIC_REQUEST)
        self._sock.send(header)
        self._sock.send(payload)

    def close(self):
        self._sock.close()

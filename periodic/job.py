from . import utils


class Job(object):

    def __init__(self, payload, client):
        self.payload = utils.decode_job(payload)
        self.job_handle = utils.encode_str8(self.payload.func) + utils.encode_str8(self.payload.name)
        self.client = client

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def done(self, buf = b''):
        self.client.send([utils.JOB_DONE, self.job_handle, buf])

    def sched_later(self, delay):
        self.client.send([utils.SCHED_LATER, self.job_handle, utils.encode_int64(delay)], utils.encode_int16(0))

    def fail(self):
        self.client.send([utils.JOB_FAIL, self.job_handle])

    def acquire(self, name, count):
        self.client.send([utils.ACQUIRE, utils.encode_str8(name), utils.encode_int16(count), self.job_handle]
        payload = self.client.recive()
        if payload[0:1] == cmd.ACQUIRED:
            if payload[1] == 1:
                return True
        return False

    def release(self, name):
        self.client.send([utils.RELEASE, utils.encode_str8(name), self.job_handle]

    def with_lock(self, name, count, task):
        acquired = self.acquire(name, count)
        if acquired:
            task()
            self.release(name)

    @property
    def func_name(self):
        return self.payload['func']

    @property
    def name(self):
        return self.payload.get('name')

    @property
    def sched_at(self):
        return self.payload['sched_at']

    @property
    def count(self):
        return self.payload.get('count', 0)

    @property
    def workload(self):
        return self.payload.get('workload')

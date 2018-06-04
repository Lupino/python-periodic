from . import utils


class Job(object):

    def __init__(self, payload, client):
        h = utils.decode_int8(payload[0:1])
        self.job_handle = payload[0:h + 1]

        payload = payload[h+1:]

        self.payload = utils.decode_job(payload)
        self.client = client

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def done(self):
        self.client.send([utils.JOB_DONE, self.job_handle])

    def sched_later(self, delay):
        self.client.send([utils.SCHED_LATER, self.job_handle, utils.encode_int64(delay)], utils.encode_int16(0))

    def data(self, dat):
        self.client.send([utils.SCHED_LATER, self,job_handle, dat])

    def fail(self):
        self.client.send([utils.JOB_FAIL, self.job_handle])

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

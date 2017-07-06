from . import utils


class Job(object):

    def __init__(self, payload, client):
        payload = payload.split(utils.NULL_CHAR, 2)
        self.payload = utils.decodeJob(payload[2])
        self.job_handle = utils.to_str(payload[1])
        self.client = client

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def done(self):
        self.client.send([utils.JOB_DONE, self.job_handle])

    def sched_later(self, delay):
        self.client.send([utils.SCHED_LATER, self.job_handle, str(delay)])

    def fail(self):
        self.client.send([utils.JOB_FAIL, self.job_handle])

    @property
    def func_name(self):
        return self.payload['func']

    @property
    def name(self):
        return self.payload.get("name")

    @property
    def sched_at(self):
        return self.payload["sched_at"]

    @property
    def count(self):
        return self.payload.get("count", 0)

    @property
    def workload(self):
        return self.payload.get("workload")

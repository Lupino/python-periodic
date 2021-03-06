from .utils import BaseClient
from . import utils
import socket
import os

class Client(object):
    def __init__(self):
        self._agent = None
        self.connected = False

    def _connect(self):
        if self._entryPoint.startswith('unix://'):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._entryPoint.split('://')[1])
        else:
            host_port = self._entryPoint.split('://')[1]
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host_port = host_port.split(':')
            host = host_port[0]
            port = 5000
            if len(host_port) == 2:
                port = int(host_port[1])
            sock.connect((host, port))

        if self._agent:
            try:
                self._agent.close()
            except Exception:
                pass
        self._agent = BaseClient(sock)
        self._agent.send(utils.TYPE_CLIENT)
        self._agent.msgid = os.urandom(4)
        self.connected = True
        return True

    def add_server(self, entryPoint):
        self._entryPoint = entryPoint

    def connect(self):
        try:
            ret = self.ping()
            if ret:
                self.connected = True
                return True
        except Exception:
            pass

        print('Try to reconnecting %s'%(self._entryPoint))
        connected = self._connect()
        return connected

    def ping(self):
        self._agent.send(utils.PING)
        payload = self._agent.recive()
        if payload == utils.PONG:
            return True
        return False

    def submit_job(self, job):
        self._agent.send([utils.SUBMIT_JOB, utils.encode_job(job)])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def run_job(self, job):
        self._agent.send([utils.RUN_JOB, utils.encode_job(job)])
        return self._agent.recive()

    def remove_job(self, job):
        self._agent.send([utils.REMOVE_JOB, utils.encode_job(job)])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def drop_func(self, func):
        self._agent.send([utils.DROP_FUNC, utils.encode_str8(func)])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def close(self):
        self._agent.close()

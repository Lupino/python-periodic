from .job import Job
from .utils import BaseClient
from . import utils
import socket
import os

class Worker(object):
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
        self._agent.send(utils.TYPE_WORKER)
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

    def grabJob(self):
        self._agent.send(utils.GRAB_JOB)
        payload = self._agent.recive()
        if payload == utils.NO_JOB:
            return None

        return Job(payload, self._agent)

    def add_func(self, func):
        self._agent.send([utils.CAN_DO, utils.encode_str8(func)])

    def brodcast(self, func):
        self._agent.send([utils.BROADCAST, utils.encode_str8(func)])

    def remove_func(self, func):
        self._agent.send([utils.CANT_DO, utils.encode_str8(func)])

    def close(self):
        self._agent.close()

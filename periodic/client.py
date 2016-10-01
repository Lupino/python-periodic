from .utils import BaseClient
from . import utils
import json
import socket
import uuid

class Client(object):
    def __init__(self):
        self._agent = None
        self.connected = False

    def _connect(self):
        if self._entryPoint.startswith("unix://"):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._entryPoint.split("://")[1])
        else:
            host_port = self._entryPoint.split("://")[1]
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host_port = host_port.split(":")
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
        self._agent.uuid = uuid.v1()
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

        print("Try to reconnecting %s"%(self._entryPoint))
        connected = self._connect()
        return connected

    def ping(self):
        self._agent.send(utils.PING)
        payload = self._agent.recive()
        if payload == utils.PONG:
            return True
        return False

    def submitJob(self, job):
        self._agent.send([utils.SUBMIT_JOB, json.dumps(job)])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def removeJob(self, job):
        self._agent.send([utils.REMOVE_JOB, json.dumps(job)])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def status(self):
        self._agent.send([utils.STATUS])
        payload = self._agent.recive()

        return json.loads(str(payload, "utf-8"))

    def dropFunc(self, func):
        self._agent.send([utils.DROP_FUNC, func])
        payload = self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def close(self):
        self._agent.close()

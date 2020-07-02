import json
import socket
import threading
import time
from queue import Queue
import numpy as np
import select
import random
import pickle


class Sender(threading.Thread):
    def __init__(self, randnums):
        super(Sender, self).__init__()
        self.randnums = randnums
        self.neighbors = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._stopped = threading.Event()

    def send(self, msg, node):
        UID, ip, port, delay = node
        time.sleep(delay)
        data = pickle.dumps(msg)
        self.socket.sendto(data, (ip, port))

    def stop(self):
        self._stopped.set()

    def is_stopped(self):
        return self._stopped.is_set()

    def run(self):
        init_time = time.time()

        while time.time() - init_time < 60:
            start = time.time()
            node = random.choice(self.neighbors)
            self.send(self.randnums, node)
            time.sleep(0.2 - (time.time() - start))

        self.socket.close()


class Process(threading.Thread):
    def __init__(self, UID, ip, port):
        super(Process, self).__init__()
        self.UID = UID
        self.ip = ip
        self.port = port
        self.neighbors = []
        self.randnums = np.random.exponential(1., (100,))
        self.sender = Sender(self.randnums)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

    def add_neighbor(self, node):
        self.neighbors.append(node)
        self.sender.neighbors.append(node)

    def run(self):
        self.sender.start()
        init_time = time.time()

        while time.time() - init_time < 60 + 2:
            self.socket.setblocking(0)
            ready = select.select([self.socket], [], [], 60 + 2 - time.time() + init_time)
            if ready[0]:
                data, address = self.socket.recvfrom(4096)
                a = pickle.loads(data)
                self.sender.randnums = self.randnums = np.minimum(a, self.randnums)
                
        self.socket.close()

        print(self.UID, ' ', int(np.rint(1 / np.mean(self.randnums))))

        self.sender.stop()
        self.sender.join()


nodes = {}
inp = input()

while inp and not inp.isspace():
    xid, yid, delay = inp.split()
    xid = int(xid)
    yid = int(yid)
    delay = float(delay) / 1000

    if xid not in nodes:
        nodes[xid] = Process(xid, 'localhost', 3000 + xid)

    if yid not in nodes:
        nodes[yid] = Process(yid, 'localhost', 3000 + yid)

    x = nodes[xid]
    y = nodes[yid]
    x.add_neighbor((yid, y.ip, y.port, delay))
    y.add_neighbor((xid, x.ip, x.port, delay))
    inp = input()

for node in nodes.values():
    node.start()

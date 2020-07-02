import json
import socket
import threading
import time
from queue import Queue


class Message:
    SEARCH = 0
    PARENT = 1
    CONVERGECAST = 2

    def __init__(self, type, value = None):
        self.type = type
        self.value = value

class Sender(threading.Thread):
    def __init__(self, lock, buffer):
        super(Sender, self).__init__()
        self.lock = lock
        self.buffer = buffer
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._stopped = threading.Event()

    def send(self, msg, node):
        UID, ip, port, delay = node
        time.sleep(delay)
        data = json.dumps(msg.__dict__).encode("ANSI")
        self.socket.sendto(data, (ip, port))

    def stop(self):
        self._stopped.set()

    def is_stopped(self):
        return self._stopped.is_set()

    def run(self):
        while not self.is_stopped():
            if self.buffer.empty():
                continue

            self.lock.acquire()
            msg, nodes = self.buffer.get()
            self.lock.release()

            for node in nodes:
                self.send(msg, node)

        self.socket.close()

class Process(threading.Thread):
    def __init__(self, UID, ip, port, is_root = False):
        super(Process, self).__init__()
        self.UID = UID
        self.ip = ip
        self.port = port
        self.root = self.mark = is_root
        self.size = 1
        self.neighbors = []
        self.children = 0
        self.parent = None
        self.lock = threading.Lock()
        self.buffer = Queue()
        self.sender = Sender(self.lock, self.buffer)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

    def process(self, data):
        received_data = json.loads(data, encoding="ANSI")
        return Message(**received_data)

    def send_to_all(self, data):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for node in self.neighbors:
            s.sendto(data, node)
        s.close()

    def put_message(self, msg, nodes):
        self.lock.acquire()
        self.buffer.put((msg, nodes))
        self.lock.release()

    def find_node(self, UID):
        for node in self.neighbors:
            if node[0] == UID:
                return node

    def log(self, msg):
        if msg.type == Message.SEARCH:
            print('Node {} received a search message from node {}'.format(self.UID, msg.value))
        elif msg.type == Message.PARENT:
            if msg.value:
                print('Node {} received a parent message'.format(self.UID))
            else:
                print('Node {} received a non-parent message'.format(self.UID))
        else:
            print('Node {} received a converge cast message with value {}'.format(self.UID, msg.value))

    def run(self):
        self.sender.start()
        non_parents = 0
        conv_casts = 0

        if self.root:
            new_msg = Message(Message.SEARCH, self.UID)
            self.put_message(new_msg, self.neighbors)

        while True:
            data, address = self.socket.recvfrom(1024)
            msg = self.process(data)
            self.log(msg)

            if msg.type == Message.SEARCH:
                node = self.find_node(msg.value)

                if self.mark:
                    new_msg = Message(Message.PARENT, False)
                    self.put_message(new_msg, [node])

                else:
                    self.parent = node
                    self.mark = True
                    msg.value = self.UID
                    new_msg = Message(Message.PARENT, True)
                    self.put_message(new_msg, [node])
                    self.put_message(msg, self.neighbors)

            elif msg.type == Message.PARENT:
                if msg.value:
                    self.children += 1

                else:
                    non_parents += 1
                    if non_parents == len(self.neighbors):
                        break

            else:
                conv_casts += 1
                self.size += msg.value
                if conv_casts == self.children:
                    break

        self.socket.close()

        if self.root:
            print("Number of nodes is", self.size)

        else:
            new_msg = Message(Message.CONVERGECAST, self.size)
            self.put_message(new_msg, [self.parent])


        while not self.buffer.empty():
            continue

        self.sender.stop()
        self.sender.join()

nodes = {}
is_root = True
inp = input()

while inp and not inp.isspace():
    xid, yid, delay = inp.split()
    xid = int(xid)
    yid = int(yid)
    delay = int(delay)

    if xid not in nodes:
        nodes[xid] = Process(xid, 'localhost', 3000 + xid, is_root)
        is_root = False

    if yid not in nodes:
        nodes[yid] = Process(yid, 'localhost', 3000 + yid)

    x = nodes[xid]
    y = nodes[yid]
    x.neighbors.append((yid, y.ip, y.port, delay))
    y.neighbors.append((xid, x.ip, x.port, delay))
    inp = input()

for node in nodes.values():
    node.start()

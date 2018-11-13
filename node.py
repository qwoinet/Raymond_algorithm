#!/usr/bin/env python
import pika
import sys
from threading import Timer
from collections import deque

EXCHANGE_NAME = 'central_communicator' # no more used
NODE_NAME_PREFIX = "node_"

MSG_INIT = "INITIALIZE"
MSG_PRIVILEGE = "PRIVILEGE"
MSG_REQ = "REQUEST"
MSG_RESTART = "RESTART"
MSG_ADVISE = "ADVISE"

NODE = None

class Node:
    def __init__(
            self, 
            node_number,
            neighbors):
        self.number=node_number
        self.name = "node_%i" % node_number
        self.neighbors = neighbors
        self.holder = None
        self.using = False
        self.request_Q = deque()
        self.asked = False

        self.channel = None

    def create_channel(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.name)

    def initialize(self):
        for n in self.neighbors :
            self.channel.queue_declare(queue=NODE_NAME_PREFIX + str(n))
        if self.number == 0:
            self.holder = 0
            for n in self.neighbors:
                self.send_msg(n, MSG_INIT)

    def consume(self):
        self.channel.basic_consume(
                self.process_msg,
                queue=self.name,
                no_ack=True,
                )
        self.channel.start_consuming()

    def enter_critical_section(self):
        self.request_Q.append(self.number)
        self.assign_privilege()
        self.make_request()

    def quit_critical_section(self):
        self.using = False
        print("QUITTING CRITICAL SECTION")
        self.assign_privilege()
        self.make_request()

    def assign_privilege(self):
        if self.holder == self.number and not self.using and len(self.request_Q) != 0:
            self.holder = self.request_Q.popleft()
            self.asked = False
            if(self.holder == self.number):
                self.using = True
                print("ENTERING CRITICAL SECTION")
                timer = Timer(3, self.quit_critical_section, [])
                timer.start()
                #print('using', self.using)
            else:
                self.send_msg(self.holder, MSG_PRIVILEGE)

    def make_request(self):
        if self.holder != self.number and len(self.request_Q) != 0 and not self.asked:
            self.send_msg(self.holder, MSG_REQ)
            self.asked = True


    def received_init(self, msg_tuple):
        self.holder = int(msg_tuple[1])
        self.request_Q = deque()
        self.using = False
        self.asked = False
        for n in self.neighbors:
            if n != self.holder:
                self.send_msg(n, MSG_INIT)

    def received_privilege(self, msg_tuple):
        self.holder = self.number
        self.assign_privilege()
        self.make_request()

    def received_req(self, msg_tuple):
        self.request_Q.append(int(msg_tuple[1]))
        self.assign_privilege()
        self.make_request()

    def received_restart(self, msg_tuple):
        pass

    def received_advise(self, msg_tuple):
        pass

    @staticmethod
    def process_msg(channel, method, properties, body):
        #print('channel', channel)
        #print('method', method)
        #print('properties', properties)
        print('body', body)

        msg_tuple = body.split()
        msg_type = msg_tuple[0]

        if(msg_type == MSG_INIT):
            NODE.received_init(msg_tuple)
        elif(msg_type == MSG_PRIVILEGE):
            NODE.received_privilege(msg_tuple)
        elif(msg_type == MSG_REQ):
            NODE.received_req(msg_tuple)
        elif(msg_type == MSG_RESTART):
            NODE.received_restart(msg_tuple)
        elif(msg_type == MSG_ADVISE):
            NODE.received_advise(msg_tuple)
        else:
            print("unkown message type : ", msg_type)

        
    def send_msg(self, dest, msg_type, body=''):
        self.channel.basic_publish(
                exchange='', 
                routing_key=NODE_NAME_PREFIX + str(dest),
                body = ("%s %s %s" % (msg_type, self.number, body)).strip()
                )



def send_init_msg(channel, src, dest):
    #channel.
    pass




if __name__ == '__main__':
    NODE = Node(
            node_number=int(sys.argv[1]),
            neighbors=[int(n) for n in sys.argv[2:]],
            )

    NODE.create_channel()
    NODE.initialize()
    timer = Timer(10, NODE.enter_critical_section, [])
    timer.start()
    NODE.consume()
    

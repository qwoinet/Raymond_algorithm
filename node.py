#!/usr/bin/env python
# coding: utf-8

import pika
import sys
from threading import Timer
from collections import deque
import time
import random

EXCHANGE_NAME = "central_communicator"  # no more used
NODE_NAME_PREFIX = "node_"

NODE_RESTART_SLEEP_TIME = 5  # seconds

MSG_INIT = "INITIALIZE"
MSG_PRIVILEGE = "PRIVILEGE"
MSG_REQ = "REQUEST"
MSG_RESTART = "RESTART"
MSG_ADVISE = "ADVISE"

ADVISE_1 = "1"
ADVISE_2 = "2"
ADVISE_3 = "3"
ADVISE_4 = "4"

MEAN_TIME_CS_ENTER = 15
MEAN_TIME_CS_QUIT = 7
MEAN_TIME_CRASH = 18


class Node:
    def __init__(self, node_number, neighbors):
        self.number = node_number
        self.name = "node_%i" % node_number
        self.neighbors = neighbors
        self.holder = None
        self.using = False
        self.request_Q = deque()
        self.asked = False

        self.iaskedforprivilege = False ##############################
        self.recovering = False
        self.advise_answers = []

        self.critical_section_timer = None
        self.crash_timer = None

        self.channel = None

    def create_channel(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.name)

    def initialize(self):
        for n in self.neighbors:
            self.channel.queue_declare(queue=NODE_NAME_PREFIX + str(n))
        if self.number == 0:
            self.holder = 0
            for n in self.neighbors:
                self.send_msg(n, MSG_INIT)

    def restart(self):
        print("%s: CRASHED... :(" % self.name)
        self.holder = None
        self.using = False
        self.request_Q = deque()
        self.asked = False
        self.iaskedforprivilege = False ###############################
        if self.critical_section_timer:
            self.critical_section_timer.cancel()
        print("%s: BEGIN RECOVERY" % self.name)
        self.recovering = True
        self.advise_answers = []

        time.sleep(NODE_RESTART_SLEEP_TIME)

        for n in self.neighbors:
            self.send_msg(n, MSG_RESTART)

    def consume(self):
        self.channel.basic_consume(self.process_msg, queue=self.name, no_ack=True)
        self.channel.start_consuming()

    def enter_critical_section(self):
        self.request_Q.append(self.number)
        self.iaskedforprivilege = True #####################################
        self.assign_privilege()
        self.make_request()

    def quit_critical_section(self):
        self.using = False
        print("%s: QUITTING CRITICAL SECTION" % self.name)
        self.critical_section_timer = create_timer(
            cs_entering_law, self.enter_critical_section
        )
        self.assign_privilege()
        self.make_request()

    def assign_privilege(self):
        # time.sleep(.5)
        if (
            not self.recovering
            and self.holder == self.number
            and not self.using
            and len(self.request_Q) != 0
        ):
            self.holder = self.request_Q.popleft()
            self.asked = False
            if self.holder == self.number:
                self.iaskedforprivilege = False #############################
                self.using = True
                print("%s: ENTERING CRITICAL SECTION" % self.name)
                self.critical_section_timer = create_timer(
                    cs_quitting_law, self.quit_critical_section
                )
                # print('using', self.using)
            else:
                self.send_msg(self.holder, MSG_PRIVILEGE)
                print("%s: SEND PRIVILEGE TO %d" % (self.name, self.holder))

    def make_request(self):
        if (
            not self.recovering
            and self.holder != self.number
            and len(self.request_Q) != 0
            and not self.asked
        ):
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
        n = int(msg_tuple[1])
        if self.holder == n:
            if self.asked:
                self.send_msg(n, MSG_ADVISE, ADVISE_2)
            else:
                self.send_msg(n, MSG_ADVISE, ADVISE_1)
        else:
            if n in self.request_Q:
                self.send_msg(n, MSG_ADVISE, ADVISE_4)
            else:
                self.send_msg(n, MSG_ADVISE, ADVISE_3)

    def received_advise(self, msg_tuple):
        n = int(msg_tuple[1])
        self.advise_answers.append((n, msg_tuple[2]))
        # TODO : check recovery id (if double recovery)
        if self.recovering and len(self.advise_answers) == len(self.neighbors):
            # determine holder and asked
            holder_list = [
                (n, x) for (n, x) in self.advise_answers if x in (ADVISE_3, ADVISE_4)
            ]
            if self.holder is None:
                if len(holder_list) == 0:
                    self.holder = self.number
                    self.asked = False
                else:
                    self.holder = holder_list[0][0]
                    self.asked = holder_list[0][1] == ADVISE_4
            # rebuild request_Q
            for (n, x) in self.advise_answers:
                if x == ADVISE_2:
                    self.request_Q.append(n)

            self.recovering = False
            self.critical_section_timer = create_timer(
                cs_entering_law, self.enter_critical_section
            )
            print("%s: FINISHED RECOVERY" % self.name)
            self.assign_privilege()
            self.make_request()

    def process_msg(self, channel, method, properties, body):
        # print('channel', channel)
        # print('method', method)
        # print('properties', properties)
        print("%s: RECEIVED '%s'" % (self.name, body))

        msg_tuple = body.split()
        msg_type = msg_tuple[0]

        if msg_type == MSG_INIT:
            self.received_init(msg_tuple)
        elif msg_type == MSG_PRIVILEGE:
            self.received_privilege(msg_tuple)
        elif msg_type == MSG_REQ:
            self.received_req(msg_tuple)
        elif msg_type == MSG_RESTART:
            self.received_restart(msg_tuple)
        elif msg_type == MSG_ADVISE:
            self.received_advise(msg_tuple)
        else:
            print("%s: unkown message type: '%s'" % (self.name, msg_type))

    def send_msg(self, dest, msg_type, body=""):
        self.channel.basic_publish(
            exchange="",
            routing_key=NODE_NAME_PREFIX + str(dest),
            body=("%s %s %s" % (msg_type, self.number, body)).strip(),
        )


def create_timer(law, callback):
    return None
    l = law()
    timer = Timer(l, callback, [])
    timer.start()
    print("\tcall function %s in %fsec" % (callback.__name__, l))
    return timer


def cs_entering_law():
    return max(random.normalvariate(MEAN_TIME_CS_ENTER, 1), 0)


def cs_quitting_law():
    return max(random.normalvariate(MEAN_TIME_CS_QUIT, 0.4), 0)


def crash_law():
    return max(random.normalvariate(MEAN_TIME_CRASH, 1), 0)


def main(node_number, neighbors):
    node = Node(node_number, neighbors)
    node.create_channel()
    node.initialize()
    return node


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: node.py node_number [neighbor1] [neighbor2] ...")
    node = main(int(sys.argv[1]), [int(n) for n in sys.argv[2:]])

    node.critical_section_timer = create_timer(
        cs_entering_law, node.enter_critical_section
    )
    if node.number == 0:
        node.crash_timer = create_timer(crash_law, node.restart)

    node.consume()

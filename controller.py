#!/usr/bin/env python
# coding: utf-8

from threading import Thread
import time
import random
import pika

import networkx as nx
import matplotlib.pyplot as plt

from node import Node


class NodeThread(Thread):
    def __init__(self, node_number, neighbors):
        super(NodeThread, self).__init__()
        self.node = Node(node_number, neighbors)

    def run(self):
        self.node.create_channel()
        self.node.initialize()
        self.node.consume()


class VisualizationThread(Thread):
    def __init__(self, nodes):
        super(VisualizationThread, self).__init__()
        self.nodes = nodes

        self.graph = nx.DiGraph()
        self.graph.add_nodes_from([node.node.number for node in self.nodes])
        self.create_edges()
        self.graph_layout = nx.spring_layout(self.graph)

        self.colors = []

    def unoriented_edges(self):
        for node in self.nodes:
            for n in node.node.neighbors:
                yield (node.node.number, n)

    def create_edges(self):
        for (source, dest, is_oriented) in self.edges():
            if is_oriented:
                self.graph.add_edge(source, dest)
            else:
                self.graph.add_edge(source, dest)
                self.graph.add_edge(dest, source)

    def edges(self):
        for node in self.nodes:
            node = node.node
            if node.holder == node.number:
                pass
            elif node.holder is None:
                for n in node.neighbors:
                    yield (node.number, n, False)
            else:
                yield (node.number, node.holder, True)

    def update_graph(self):
        for src, dst in list(self.graph.edges()):
            self.graph.remove_edge(src, dst)
        self.create_edges()
        self.colors = []
        for g in self.graph.nodes():
            node = next(n for n in self.nodes if n.node.number == g)
            self.colors.append(self.node_color(node))

    def node_color(self, node):
        node = node.node
        if node.using:
            return "red"
        elif node.holder == node.number:
            return "green"
        elif node.asked:
            return "blue"
        elif node.holder is None:
            return "black"
        elif node.recovering:
            return "yellow"
        else:
            return "gray"

    def update_draw(self):
        plt.clf()
        nx.draw_networkx_nodes(self.graph, self.graph_layout, node_color=self.colors)
        nx.draw_networkx_labels(
            self.graph,
            self.graph_layout,
            {n.node.number: n.node.number for n in self.nodes},
            font_size=10,
        )
        nx.draw_networkx_edges(
            self.graph, self.graph_layout, arrowstyle="->", arrowsize=20
        )
        plt.pause(0.1)

    def run(self):
        while True:
            self.update_graph()
            self.update_draw()


class ControlThread(Thread):
    def __init__(self, nodes):
        super(ControlThread, self).__init__()
        self.nodes = nodes
        self.next_crash = [random.randrange(0, 10000) for _ in nodes]
        self.next_critical_action = [random.randrange(0, 1000) for _ in nodes]
        self.channel = None

    def create_channel(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.channel = self.connection.channel()
        # self.channel.queue_declare(queue=self.name)

    def run(self):
        self.create_channel()
        time.sleep(1)
        while True:
            for i in range(len(nodes)):
                self.next_crash[i] -= 1
                self.next_critical_action[i] -= 1

                if self.next_crash[i] <= 0:
                    self.nodes[i].node.restart();
                    self.next_crash[i] = random.randrange(0, 2000)
                if self.next_critical_action[i] <= 0 and not self.nodes[i].node.recovering:
                    if self.nodes[i].node.using:
                        self.nodes[i].node.quit_critical_section()
                        self.next_critical_action[i] = random.randrange(0, 1000)
                    elif not self.nodes[i].node.iaskedforprivilege:
                        self.nodes[i].node.enter_critical_section()
                        self.next_critical_action[i] = random.randrange(0, 100)


            time.sleep(random.random() * .01)
            # node = self.nodes[random.randrange(0, len(self.nodes))]

            # if node.node.recovering or node.node.iaskedforprivilege:
            #     continue
            # if random.random() < 0.9:
            #     node.node.enter_critical_section()
            #     while not node.node.using:
            #         time.sleep(0.1)
            #     time.sleep(1 + random.random() * 3)
            # #     print("\tnode %d quits critical section _ %d" % (node.node.number, rval))
            #     node.node.quit_critical_section()
            # elif not node.node.using:
            #     node.node.restart()


            # rval = random.randrange(100000, 1000000)
            # if node.node.recovering:
            #     continue
            # if random.random() < 0.9:
            #     print("\tnode %d enter critical section _ %d" % (node.node.number, rval))
            #     node.node.enter_critical_section()
            #     while not node.node.using:
            #         time.sleep(0.1)
            #     time.sleep(1 + random.random() * 3)
            #     print("\tnode %d quits critical section _ %d" % (node.node.number, rval))
            #     node.node.quit_critical_section()
            # elif not node.node.using:
            #     node.node.restart()

def create_graph(nbr_nodes):
    tree = nx.generators.random_tree(nbr_nodes)
    nodes = []
    for i in range(nbr_nodes):
        nodes.append(NodeThread(i, list(tree.neighbors(i))))
    return nodes


if __name__ == "__main__":
    nodes = create_graph(50)
    
    # nodes = [
    #     NodeThread(0, [1, 2]),
    #     NodeThread(1, [0]),
    #     NodeThread(2, [0, 3, 4, 5]),
    #     NodeThread(3, [2]),
    #     NodeThread(4, [2]),
    #     NodeThread(5, [2, 6]),
    #     NodeThread(6, [5]),
    # ]
    controls = [
        ControlThread(nodes),
    ]
    viz = VisualizationThread(nodes)

    for node in nodes:
        node.start()

    for control in controls:
        control.start()

    viz.run()  # run because we want to stay in the main thread

from utils.ip_operation import *
from utils.errors import *
from vsys_graph import Graph

import numpy as np
from node import Node

from utils.api_operation import *

import networkx as nx
import matplotlib.pyplot as plt


# basic setting of error
set_throw_on_error()

# set ip address of root and default ports
ip_address = '54.147.255.148'
default_ports = ['9922']

# set graph name
graph_name = 'vsys'
system_application_name = 'V SYSTEMSM'

# set hostname, db name and pwd
hostname = 'localhost'
user_name = 'aaronyu'
password = 'pwd'


class NodeAnalysis:
    def __init__(self, name, application_name, ip, ports):
        self.ip = ip
        self.ports = ports
        self.new_graph = Graph(name, application_name)

    def construct_graph(self):
        self.new_graph.traversal_graph_dfs(self.ip)
        graph_network = self.new_graph.construct_graph_network()

        print("all nodes: ", graph_network)
        return graph_network

    def get_graph_asymmetric_matrix(self, graph_network=None):
        if not graph_network:
            graph_network = self.construct_graph()
            return self.new_graph.get_graph_asymmetric_matrix(graph_network)
        else:
            return self.new_graph.get_graph_asymmetric_matrix(graph_network)

    def get_graph_symmetric_matrix(self, graph_network=None):
        if not graph_network:
            graph_network = self.construct_graph()
            return self.new_graph.get_graph_symmetric_matrix(graph_network)
        else:
            return self.new_graph.get_graph_symmetric_matrix(graph_network)

    def plot_snapshot_matrix(self, matrix=None):
        if not matrix:
            matrix = self.get_graph_symmetric_matrix()
        plt.figure()
        plt.matshow(matrix)
        plt.savefig('1.png', bbox_inches='tight')

    def plot_snapshot_graph(self):
        if not self.new_graph.vertex_snapshot:
            msg = "Snapshot is empty!"
            throw_error(msg, InvalidInputException)
        nodes = list(self.new_graph.vertex_snapshot.values())
        nodes_graph = nx.DiGraph()
        nodes_graph.add_nodes_from(nodes)
        color_list = ['blue'] * len(self.new_graph.graph)

        graph_network = self.construct_graph()

        for node_id in self.new_graph.graph:
            node = self.new_graph.graph[node_id]
            if not node.status:
                color_list[node_id] = 'red'
            if node_id in graph_network:
                for peer in graph_network[node_id]:
                    nodes_graph.add_edge(node_id, peer)

        plt.figure()
        nx.draw(nodes_graph, node_color=color_list, with_labels=True)
        plt.draw()
        plt.savefig('2.png', bbox_inches='tight')

    def get_all_nodes_info(self, matrix=None):
        if not matrix:
            matrix = self.get_graph_symmetric_matrix()

        return self.new_graph.get_nodes_detail(matrix)

# vsys_asymmetric_matrix = vsys.get_graph_asymmetric_matrix(vsys_network)
# print("asymmetric matrix: ", vsys_asymmetric_matrix)
#
# vsys_symmetric_matrix = vsys.get_graph_symmetric_matrix(vsys_network)
# print("symmetric matrix: ", vsys_symmetric_matrix)
#
# plt.figure()
# plt.matshow(vsys_symmetric_matrix)
# plt.savefig('1.png', bbox_inches='tight')
#
# status = vsys.check_graph_outdated()
# print("status", status)
#
# vsys_nodes = list(vsys.vertex_snapshot.values())
#
# vsys_node_network = nx.DiGraph()
# vsys_node_network.add_nodes_from(vsys_nodes)
# color_list = ['blue'] * len(vsys.graph)
#
# for node_id in vsys.graph:
#     node = vsys.graph[node_id]
#     if not node.status:
#         color_list[node_id] = 'red'
#     if node_id in vsys_network:
#         for peer in vsys_network[node_id]:
#             vsys_node_network.add_edge(node_id, peer)
#
# plt.figure()
# nx.draw(vsys_node_network, node_color=color_list, with_labels=True)
# plt.draw()
# plt.savefig('2.png', bbox_inches='tight')
#
# print("all nodes info")
# all_nodes_info = vsys.get_nodes_detail(vsys_symmetric_matrix)
#
# vsys.output_graph_by_number_peers(all_nodes_info)
#
#
# plt.show()


if __name__ == "__main__":
    vsys_node_analysis = NodeAnalysis(graph_name, system_application_name, ip_address, default_ports)
    vsys_node_analysis.new_graph.initialize_db(hostname, user_name, password)
    # vsys_node_analysis.construct_graph()


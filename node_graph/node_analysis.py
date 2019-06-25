from utils.ip_operation import *
import logging
from utils.errors import *
from vsys_graph import Graph
import timeit
import time

import numpy as np
from node import Node

from utils.api_operation import *

import networkx as nx
import matplotlib.pyplot as plt


# basic setting of error
set_throw_on_error()


logging.basicConfig(level=logging.ERROR)


# set ip address of root and default ports
ip_address = '54.147.255.148' # '54.147.255.148'
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
        self.wait_time = 0

    def wait_time_after_analysis_finished(self):
        if self.wait_time < 0:
            msg = "Need to increase time gap for successive analysis!"
            throw_error(msg, InvalidInputException)
        else:
            time.sleep(self.wait_time)

    def successive_node_analysis(self, rounds=1, time_gap=600, non_stop=False):
        self.new_graph.initialize_db(hostname, user_name, password)

        vsys_node_analysis.new_graph.graph_db.start_db()
        try:
            while rounds:
                current_round_start_time = timeit.default_timer()

                vsys_node_analysis.new_graph.get_current_timestamp()
                current_timestamp = vsys_node_analysis.new_graph.current_timestamp
                next_time_id = vsys_node_analysis.new_graph.get_next_time_id_in_db()

                vsys_node_analysis.new_graph.add_timestamp_to_table_time(current_timestamp, next_time_id)
                vsys_node_analysis.new_graph.traversal_graph_dfs(self.ip)
                nodes_and_matrix = vsys_node_analysis.new_graph.get_graph_symmetric_matrix()
                vsys_node_analysis.new_graph.get_nodes_detail(nodes_and_matrix, current_timestamp)

                current_round_stop_time = timeit.default_timer()

                self.wait_time = time_gap - int(current_round_stop_time - current_round_start_time)

                self.wait_time_after_analysis_finished()
                rounds -= 1
                if non_stop:
                    rounds += 1
        except NodeAnalysisException:
            msg = "Some error in successive node analysis!"
            throw_error(msg, NodeAnalysisException)
        finally:
            vsys_node_analysis.new_graph.graph_db.close_db()

    def construct_graph(self):
        self.new_graph.traversal_graph_dfs(self.ip)
        graph_network = self.new_graph.construct_graph_network()

        print("all nodes: ", graph_network)
        return graph_network

    # def get_graph_asymmetric_matrix(self, graph_network=None):
    #     if not graph_network:
    #         graph_network = self.construct_graph()
    #         return self.new_graph.get_graph_asymmetric_matrix(graph_network)
    #     else:
    #         return self.new_graph.get_graph_asymmetric_matrix(graph_network)
    #
    # def get_graph_symmetric_matrix(self, graph_network=None):
    #     if not graph_network:
    #         graph_network = self.construct_graph()
    #         return self.new_graph.get_graph_symmetric_matrix(graph_network)
    #     else:
    #         return self.new_graph.get_graph_symmetric_matrix(graph_network)

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
    vsys_node_analysis.successive_node_analysis()


    # vsys_node_analysis.new_graph.initialize_db(hostname, user_name, password)
    # vsys_node_analysis.new_graph.graph_db.start_db()
    #
    # next_time_id = vsys_node_analysis.new_graph.get_next_time_id_in_db()
    # print(next_time_id)
    # vsys_node_analysis.new_graph.get_current_timestamp()
    # timestamp = vsys_node_analysis.new_graph.current_timestamp
    # print(timestamp)
    # vsys_node_analysis.new_graph.add_timestamp_to_table_time(timestamp, next_time_id)
    # vsys_node_analysis.new_graph.graph_db.close_db()


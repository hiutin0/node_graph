from utils.ip_operation import *
from utils.errors import *
from vsys_graph import Graph

import numpy as np
from node import Node

from utils.api_operation import *

import networkx as nx
import matplotlib.pyplot as plt

set_throw_on_error()

ip = '54.147.255.148'
default_ports = ['9922']

# TODO construct a graph by a node

graph_name = 'vsys'

vsys = Graph(graph_name)
vsys.traversal_graph_dfs(ip)
vsys_network = vsys.construct_graph_network()

print("all nodes: ", vsys_network)

vsys_asymmetric_matrix = vsys.get_graph_asymmetric_matrix(vsys_network)
print("asymmetric matrix: ", vsys_asymmetric_matrix)

vsys_symmetric_matrix = vsys.get_graph_symmetric_matrix(vsys_network)
print("symmetric matrix: ", vsys_symmetric_matrix)

plt.figure()
plt.matshow(vsys_symmetric_matrix)
plt.savefig('1.png', bbox_inches='tight')

status = vsys.check_graph_outdated()
print("status", status)

vsys_nodes = list(vsys.vertex_snapshot.values())

vsys_node_network = nx.DiGraph()
vsys_node_network.add_nodes_from(vsys_nodes)
color_list = ['blue'] * len(vsys.graph)

for node_id in vsys.graph:
    node = vsys.graph[node_id]
    if not node.status:
        color_list[node_id] = 'red'
    if node_id in vsys_network:
        for peer in vsys_network[node_id]:
            vsys_node_network.add_edge(node_id, peer)

plt.figure()
nx.draw(vsys_node_network, node_color=color_list, with_labels=True)
plt.draw()
plt.savefig('2.png', bbox_inches='tight')

print("all nodes info")
all_nodes_info = vsys.get_nodes_detail(vsys_symmetric_matrix)

vsys.output_graph_by_number_peers(all_nodes_info)


plt.show()





# TODO save data to local folder


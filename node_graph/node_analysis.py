from utils.ip_operation import *
from utils.errors import *
from vsys_graph import Graph
import csv
import numpy as np
from node import Node

from utils.api_operation import *

import networkx as nx
import matplotlib.pyplot as plt

set_throw_on_error()

ip = '54.147.255.148'
default_ports = ['9922']

# TODO construct a graph by a node

vsys_graph = Graph('vsys')
vsys_graph.traversal_graph_dfs(ip)
vsys_network = vsys_graph.construct_graph_network()

print("all nodes: ", vsys_network)

vsys_asymmetric_matrix = vsys_graph.get_graph_asymmetric_matrix(vsys_network)
print("asymmetric matrix: ", vsys_asymmetric_matrix)

vsys_symmetric_matrix = vsys_graph.get_graph_symmetric_matrix(vsys_network)
print("symmetric matrix: ", vsys_symmetric_matrix)

vsys_nodes = list(vsys_graph.vertex_snapshot.values())

vsys_node_network = nx.DiGraph()
vsys_node_network.add_nodes_from(vsys_nodes)
color_list = ['blue'] * len(vsys_graph.graph)

for node_id in vsys_graph.graph:
    node = vsys_graph.graph[node_id]
    if not node.status:
        color_list[node_id] = 'red'
    if node_id in vsys_network:
        for peer in vsys_network[node_id]:
            vsys_node_network.add_edge(node_id, peer)

nx.draw(vsys_node_network, node_color=color_list, with_labels=True)
plt.draw()

print("all nodes info")
all_nodes_info = vsys_graph.get_nodes_detail(vsys_symmetric_matrix)

active_nodes = {}
inactive_nodes = {}
rank_list = []
for key in all_nodes_info:
    if all_nodes_info[key]['status']:
        active_nodes.update({key: all_nodes_info[key]})
    else:
        inactive_nodes.update({key: all_nodes_info[key]})

active_nodes_sort = sorted(active_nodes.items(), key=lambda x: x[1]['number_peers'], reverse=True)
inactive_nodes_sort = sorted(inactive_nodes.items(), key=lambda x: x[1]['number_peers'], reverse=True)
rank_list = active_nodes_sort + inactive_nodes_sort

with open('nodes_details.csv', 'w') as csvfile:
    fieldnames = ['vertex_id', 'ip_address', 'status', 'link', 'number_peers', 'address', 'height', 'version', 'location']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in rank_list:
        vertex_id = item[0].split('-')[1]
        row_info = dict()
        row_info.update({fieldnames[0]: vertex_id})
        row_info.update(item[1])
        writer.writerow(row_info)

csvfile.close()

plt.show()



# TODO update adjacent link

# TODO save data to local folder


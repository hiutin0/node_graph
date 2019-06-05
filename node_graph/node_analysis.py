from utils.ip_operation import *
from utils.errors import *
from vsys_graph import Graph
from node import Node

import networkx as nx
import matplotlib.pyplot as plt

set_throw_on_error()

ip = '54.147.255.148'
default_ports = ['9922']

# TODO construct a graph by a node

# g = nx.DiGraph()
# g.add_nodes_from([1,2,3,4,5])
# g.add_edge(1,2)
# g.add_edge(4,2)
# g.add_edge(3,5)
# g.add_edge(2,3)
# g.add_edge(5,4)
#
# nx.draw(g,with_labels=True)
# plt.draw()
# plt.show()

vsys_graph = Graph('vsys')
vsys_graph.traversal_graph_dfs(ip)
vsys_network = vsys_graph.construct_graph_network()

vsys_nodes = list(vsys_graph.vertex_snapshot.values())

vsys_node_network = nx.DiGraph()
vsys_node_network.add_nodes_from(vsys_nodes)
for node in vsys_network:
    for peer in vsys_network[node]:
        vsys_node_network.add_edge(node, peer)

nx.draw(vsys_node_network, with_labels=True)
plt.draw()
plt.show()

print(vsys_network)

# TODO get full info of nodes

# TODO update adjacent matrix

# TODO update adjacent link

# TODO save data to local folder


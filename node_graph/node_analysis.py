from utils.ip_operation import *
from utils.errors import *
from graph import Graph
from node import Node

set_throw_on_error()

ip = '54.147.255.148'
default_ports = ['9922']

# TODO construct a graph by a node
vsys_graph = Graph('vsys')
vsys_graph.traversal_graph_dfs(ip)
vsys_network = vsys_graph.construct_graph_network()
print(vsys_network)

# TODO get full info of nodes

# TODO update adjacent matrix

# TODO update adjacent link

# TODO save data to local folder


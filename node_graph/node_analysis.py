from ip_operation import *
from utils.errors import *
from graph import Graph

set_throw_on_error()

node = '54.147.255.140'
DEFAULT_PORT = ['9922']


a = ip_to_hex_string(node)
b = hex_string_to_ip(a)

# TODO construct a graph by a node
vsys_graph = Graph('vsys')
vsys_graph.construct_graph(node)

# TODO get full info of nodes

# TODO update adjacent matrix

# TODO update adjacent link

# TODO save data to local folder


print(a)
print(b)

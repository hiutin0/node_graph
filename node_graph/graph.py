
from utils.api_operation import *


class Graph:
    def __init__(self, name):
        self.graph_name = name

    def construct_graph(self, node):
        ports = api_default_port()
        peers = get_peer_nodes(node, ports)
        pass

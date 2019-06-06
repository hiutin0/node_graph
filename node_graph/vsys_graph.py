
from utils.api_operation import *
from node import *
from lib.stack import Stack
import timeit
import numpy as np


class Graph:
    def __init__(self, name):
        self.graph_name = name
        self.application_name = 'V SYSTEMSM'
        self.vertex_snapshot = {}
        self.graph = {}
        self.time_traversal_graph = 0

    def traversal_graph_dfs(self, node):
        vertex_id = 0
        default_ports = set_api_default_port()

        root = Node(vertex_id, node, default_ports)
        self.vertex_snapshot.update({ip_to_hex_string(node): vertex_id})
        self.graph.update({vertex_id: root})

        node_checker = Stack()
        node_checker.push(ip_to_hex_string(node))

        start_time = timeit.default_timer()

        while not node_checker.is_empty():
            _vertex_hex = node_checker.pop()
            _vertex_id = self.vertex_snapshot[_vertex_hex]

            if not self.graph[_vertex_id].visited:
                self.graph[_vertex_id].visited = True
                if self.graph[_vertex_id].status:
                    url = self.graph[_vertex_id].link
                    peers = get_peer_nodes(url)
                    if peers:
                        if peers[0]['applicationName'] != self.application_name:
                            self.graph[_vertex_id].status = False
                            self.graph[_vertex_id].link = 'link with wrong application'
                            continue
                    ips = get_ips_info(peers)
                    for ip_port in ips:
                        ip_port_list = ip_port.split(':')
                        ip = ip_port_list[0]
                        port = ip_port_list[1]
                        _vertex_hex = ip_to_hex_string(ip)
                        if _vertex_hex not in self.vertex_snapshot:
                            vertex_id += 1
                            default_ports.append(port)
                            new_node = Node(vertex_id, ip, default_ports)
                            self.vertex_snapshot.update({_vertex_hex: vertex_id})
                            self.graph.update({vertex_id: new_node})
                            default_ports.pop()

                        _vertex_id = self.vertex_snapshot[_vertex_hex]
                        if not self.graph[_vertex_id].visited:
                            node_checker.push(_vertex_hex)

        stop_time = timeit.default_timer()
        self.time_traversal_graph = stop_time - start_time
        print("time of traversing the graph: ", self.time_traversal_graph)
        print("total number of vertex in the graph: ", len(self.vertex_snapshot))
        print(self.vertex_snapshot)

    def construct_graph_network(self):
        network = {}
        for vertex_hex in self.vertex_snapshot:
            vertex_id = self.vertex_snapshot[vertex_hex]
            if self.graph[vertex_id].status:
                url = self.graph[vertex_id].link
                peers = get_peer_nodes(url)
                ips = get_ips_info(peers)
                peers_id = []
                for ip_port in ips:
                    ip_port_list = ip_port.split(':')
                    ip = ip_port_list[0]
                    _vertex_hex = ip_to_hex_string(ip)
                    if _vertex_hex == vertex_hex:
                        continue
                    try:
                        _vertex_id = self.vertex_snapshot[_vertex_hex]
                        peers_id.append(_vertex_id)
                    except:
                        pass  # TODO add the check status of graph
                unique_peers = list(dict.fromkeys(peers_id))
                network.update({vertex_id: unique_peers})
        return network

    def get_graph_asymmetric_matrix(self, network):
        matrix_row = len(self.vertex_snapshot)
        matrix = np.zeros([matrix_row, matrix_row], dtype=np.int8)
        for node in network:
            matrix[node, network[node]] = 1
        return matrix

    def get_graph_symmetric_matrix(self, network):
        matrix_row = len(self.vertex_snapshot)
        matrix = np.zeros([matrix_row, matrix_row], dtype=np.int8)
        for node in network:
            matrix[node, network[node]] = 1
            for end_node in network[node]:
                matrix[end_node, node] = 1
        return matrix

    def get_nodes_detail(self, adjacent_matrix):
        info = ['ip_address', 'status', 'link', 'number_peers', 'address', 'height', 'version', 'location']
        nodes_detail = {}
        for node_id in self.graph:
            node = self.graph[node_id]
            key = node.ip_hex() + '-' + str(node.id)
            node_info = dict()
            node_info.update({info[0]: node.ip_address})
            node_info.update({info[1]: node.status})
            node_info.update({info[2]: node.link})
            node_info.update({info[3]: np.sum(adjacent_matrix[node_id, :])})

            if node.status:
                node_info.update({info[4]: get_node_wallet_address(node.link)})
                node_info.update({info[5]: get_node_height(node.link)})
                node_info.update({info[6]: get_node_version(node.link)})
            else:
                node_info.update({info[4]: None})
                node_info.update({info[5]: None})
                node_info.update({info[6]: None})

            node_info.update({info[7]: get_location_ip(node.ip_address)})
            nodes_detail.update({key: node_info})
            print("node %s:  %s" % (key, nodes_detail[key]))

        return nodes_detail


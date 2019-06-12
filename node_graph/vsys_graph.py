from lib.graph_operations import *
from lib.stack import Stack
from utils.api_operation import *
from node import *
import timeit
import numpy as np


class Graph:
    def __init__(self, name):
        self.graph_name = name
        self.application_name = 'V SYSTEMSM'
        self.vertex_snapshot = {}
        self.graph = {}
        self.time_traversal_graph = 0

    def add_node_in_snapshot_graph(self, node_info, ip_address, ports):
        vertex_id = node_info[0]
        vertex_name = node_info[1]
        vertex_nonce = node_info[2]
        new_node = Node(vertex_id, ip_address, ports, vertex_name, vertex_nonce)
        self.vertex_snapshot.update({ip_to_hex_string(ip_address): vertex_id})
        self.graph.update({vertex_id: new_node})

    def update_root_name_nonce(self, vertex_id, peer_name, peer_nonce):
        if vertex_id == 0:
            if not self.graph[vertex_id].node_name:
                self.graph[vertex_id].node_name = peer_name
            if not self.graph[vertex_id].node_nonce:
                self.graph[vertex_id].node_nonce = peer_nonce

    def traversal_graph_dfs(self, ip_address):
        vertex_id = 0
        root_name = ''
        root_nonce = ''
        default_ports = set_api_default_port()

        root_info = [vertex_id, root_name, root_nonce]
        self.add_node_in_snapshot_graph(root_info, ip_address, default_ports)

        node_stack = Stack()
        node_stack.push(ip_to_hex_string(ip_address))

        start_time = timeit.default_timer()

        while not node_stack.is_empty():
            _vertex_hex = node_stack.pop()
            _vertex_id = self.vertex_snapshot[_vertex_hex]

            if not self.graph[_vertex_id].visited:
                self.graph[_vertex_id].visited = True
                if self.graph[_vertex_id].status:
                    node_start_time = timeit.default_timer()
                    url = self.graph[_vertex_id].link
                    peers = get_peer_nodes(url)
                    if peers:
                        if peers[0]['applicationName'] != self.application_name:
                            self.graph[_vertex_id].status = False
                            self.graph[_vertex_id].link = 'wrong application'
                            continue
                    peers_id = []
                    for item in peers:
                        [ip, port, peer_name, peer_nonce] = parse_ip_port_name_nonce(item)
                        _peer_hex = ip_to_hex_string(ip)
                        if _peer_hex not in self.vertex_snapshot:
                            vertex_id += 1
                            vertex_info = [vertex_id, peer_name, peer_nonce]
                            self.add_node_in_snapshot_graph(vertex_info, ip, default_ports + [port])

                        _peer_id = self.vertex_snapshot[_peer_hex]
                        if _peer_hex != _vertex_hex:
                            peers_id.append(_peer_id)

                        self.update_root_name_nonce(_peer_id, peer_name, peer_nonce)

                        if not self.graph[_peer_id].visited:
                            node_stack.push(_peer_hex)

                    self.graph[_vertex_id].peers = list(dict.fromkeys(peers_id))
                    node_stop_time = timeit.default_timer()
                    self.graph[_vertex_id].time_init = node_stop_time - node_start_time

        stop_time = timeit.default_timer()
        self.time_traversal_graph = stop_time - start_time
        print("time of traversing the graph: ", self.time_traversal_graph)
        print("total number of vertex in the graph: ", len(self.vertex_snapshot))

    def construct_graph_network(self):
        network = {}

        for vertex_hex in self.vertex_snapshot:
            vertex_id = self.vertex_snapshot[vertex_hex]
            if self.graph[vertex_id].peers:
                network.update({vertex_id: np.copy(self.graph[vertex_id].peers)})
        return network

    def check_graph_outdated(self):
        for _vertex_hex in self.vertex_snapshot:
            _vertex_id = self.vertex_snapshot[_vertex_hex]

            if self.check_node_outdated(_vertex_id):
                return True
            else:
                continue
        return False

    def check_node_outdated(self, vertex_id):
        if self.graph[vertex_id].status:
            url = self.graph[vertex_id].link
            peers = get_peer_nodes(url)
            for item in peers:
                [ip, _] = parse_ip_port(item)
                _peer_hex = ip_to_hex_string(ip)
                if _peer_hex not in self.vertex_snapshot:
                    return True
                else:
                    continue
        return False

    def get_graph_asymmetric_matrix(self, network):
        matrix_row_dim = len(self.vertex_snapshot)
        matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)
        for node in network:
            matrix[node, network[node]] = 1
        return matrix

    def get_graph_symmetric_matrix(self, network):
        matrix_row_dim = len(self.vertex_snapshot)
        matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)
        for node in network:
            matrix[node, network[node]] = 1
            for end_node in network[node]:
                matrix[end_node, node] = 1
        return matrix

    def get_nodes_detail(self, adjacent_matrix):
        info = ['ip_address', 'status', 'node_name', 'node_nonce', 'link', 'number_peers',
                'address', 'height', 'version', 'location', 'time_init', 'time_get_details']
        nodes_detail = {}
        for node_id in self.graph:

            node = self.graph[node_id]
            key = node.ip_hex() + '-' + str(node.id)

            node_start_time = timeit.default_timer()
            ip_address = node.ip_address
            node_name = node.node_name
            node_nonce = node.node_nonce
            link = node.link

            node_info = dict()
            node_info.update({info[0]: ip_address})
            node_info.update({info[1]: node.status})
            node_info.update({info[2]: node_name})
            node_info.update({info[3]: node_nonce})
            node_info.update({info[4]: link})
            node_info.update({info[5]: np.sum(adjacent_matrix[node_id, :])})

            if node.status:
                node_info.update({info[6]: get_node_wallet_address(link)})
                node_info.update({info[7]: get_node_height(link)})
                node_info.update({info[8]: get_node_version(link)})
            else:
                node_info.update({info[6]: None})
                node_info.update({info[7]: None})
                node_info.update({info[8]: None})

            node_info.update({info[9]: get_location_ip(ip_address)})

            node_stop_time = timeit.default_timer()
            node_info.update({info[10]: node.time_init})

            node.time_get_details = node_stop_time - node_start_time
            node_info.update({info[11]: node.time_get_details})

            nodes_detail.update({key: node_info})
            print("node %s:  %s" % (key, nodes_detail[key]))

        return [info, nodes_detail]


    def output_graph_by_number_peers(self, all_nodes_info):

        headers = ['vertex_id'] + all_nodes_info[0]
        nodes_info = all_nodes_info[1]

        [active_nodes, inactive_nodes] = get_nodes_with_status(nodes_info)
        active_nodes_sort = sort_nodes_by_number_peers(active_nodes)
        inactive_nodes_sort = sort_nodes_by_number_peers(inactive_nodes)

        output_items_to_csv_file(headers, active_nodes_sort + inactive_nodes_sort)



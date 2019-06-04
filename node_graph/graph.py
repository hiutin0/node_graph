
from utils.api_operation import *
from node import *
from lib.stack import Stack
import timeit


class Graph:
    def __init__(self, name):
        self.graph_name = name
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

    # def construct_graph(self, node):
    #     graph = {}
    #     vertex_number = 0
    #
    #     default_ports = set_api_default_port()
    #
    #     root = Node(node, default_ports)
    #     key = generate_a_key(node, vertex_number)
    #     graph.update({key: root})
    #     root.status = 'gray'
    #
    #     port = check_node_request(ip=node, ports=default_ports)
    #
    #     if port:
    #         url = get_node_link(node, port)
    #         peers = get_peer_nodes(url)
    #         root.status = 'black'
    #         ips = get_ip_info(peers)
    #         for ip_port in ips:
    #             ip_port_list = ip_port.split(':')
    #             ip = ip_port_list[0]
    #             port = ip_port_list[1]
    #             if ip != root.ip:
    #                 vertex_number += 1
    #                 key = generate_a_key(ip, vertex_number)
    #                 graph.update({key})
    #
    #         print(ips)

from lib.graph_operations import *
from lib.stack import Stack
from utils.api_operation import *
from node import *
from utils.errors import *
import timeit
import numpy as np
from graph_db import GraphDB
from time import gmtime, strftime
import utils.db_meta as db_meta


class Graph:
    def __init__(self, name, application_name):
        self.graph_name = name
        self.application_name = application_name
        self.vertex_snapshot = {}
        self.graph = {}
        self.current_timestamp = None
        self.time_traversal_graph = 0
        self.graph_db = None
        self.root_update = False

        self.db_running_status = False
        self.db_conn_status = False
        self.db_cursor = None
        self.db_conn = None

    def initialize_db(self, hostname, user_name, password, clear_old_db=False):
        db_name = self.graph_name

        self.graph_db = GraphDB(hostname, db_name, user_name, password)
        self.graph_db.start_db()

        try:
            if clear_old_db:
                self.graph_db.drop_db(db_name)

            if not self.graph_db.check_db():
                self.graph_db.create_db()

            if not self.graph_db.check_table(db_meta.table_time.name):
                self.graph_db.create_table(db_meta.table_time.name, db_meta.table_time.get_headers_with_desc())

            if not self.graph_db.check_table(db_meta.table_nodes_id.name):
                self.graph_db.create_table(db_meta.table_nodes_id.name, db_meta.table_nodes_id.get_headers_with_desc())

            self.graph_db.add_extension_timescale()
            if not self.graph_db.check_table(db_meta.hypertable_nodes_all.name):
                self.graph_db.create_hypertable(db_meta.hypertable_nodes_all.name,
                                                db_meta.hypertable_nodes_all.get_headers_with_desc())
        except InvalidInputException:
            msg = "Some errors in initialization of database!"
            throw_error(msg, InvalidInputException)
        finally:
            self.graph_db.close_db()

    def get_current_timestamp(self):
        self.current_timestamp = strftime("%Y-%m-%d %H:%M", gmtime()) + ':00'

    def get_next_time_id_in_db(self):
        get_number_of_timestamps_command = "SELECT COUNT(*) FROM " + db_meta.table_time.name + ";"
        number = self.graph_db.query_items_with_command(get_number_of_timestamps_command)
        return number[0][0]

    def add_timestamp_to_table_time(self, timestamp, time_id):
        headers = "(" + db_meta.table_time.get_all_headers() + ")"
        db_meta.table_time.set_value_in_a_row(timestamp, column_id=0)
        db_meta.table_time.set_value_in_a_row(time_id, column_id=1)
        values = "(" + db_meta.table_time.get_all_values() + ")"
        self.graph_db.insert_item(headers, values, db_meta.table_time.name)

    def traversal_graph_dfs(self, ip_address):
        if not self.graph_db:
            msg = "Traversal graph by dfs requires initialization of a graph database!"
            throw_error(msg, TraversalGraphException)

        self.graph = {}
        self.graph_db.start_db()

        try:
            next_vertex_id = self.get_next_vertex_id()

            default_ports = set_api_default_port()
            root_id = self.get_vertex_id_with_ip(ip_address)

            if not root_id:
                if root_id != 0:
                    root_id = next_vertex_id
                    next_vertex_id += 1
                    root_basic_info = [ip_address, root_id, default_ports[0]]
                    self.add_vertex_to_table_nodes_id(root_basic_info)

            root_name = ''
            root_nonce = ''
            root = Node(root_id, ip_address, default_ports, root_name, root_nonce)
            self.root_update = False

            self.graph.update({ip_address: root})
            node_stack = Stack()
            node_stack.push(ip_address)

            start_time = timeit.default_timer()

            while not node_stack.is_empty():
                _vertex_ip = node_stack.pop()
                _vertex = self.graph[_vertex_ip]

                if not _vertex.visited:
                    _vertex.visited = True
                    if _vertex.status:
                        node_start_time = timeit.default_timer()
                        url = _vertex.link
                        peers = get_peer_nodes(url)
                        if peers:
                            if peers[0]['applicationName'] != self.application_name:
                                _vertex.status = False
                                _vertex.link = 'wrong application'
                                continue
                        peers_id = []
                        for item in peers:
                            [_peer_ip, port, peer_name, peer_nonce] = parse_ip_port_name_nonce(item)
                            _peer_id = self.get_vertex_id_with_ip(_peer_ip)
                            if not _peer_id:
                                if _peer_id != 0:
                                    _peer_id = next_vertex_id
                                    new_node = Node(_peer_id, _peer_ip, default_ports + [port], peer_name, peer_nonce)
                                    new_node_basic_info = [_peer_ip, _peer_id, new_node.port]

                                    self.add_vertex_to_table_nodes_id(new_node_basic_info)
                                    next_vertex_id += 1
                                    if _peer_ip not in self.graph:
                                        self.graph.update({_peer_ip: new_node})
                                else:
                                    if _peer_ip not in self.graph:
                                        new_node = Node(_peer_id, _peer_ip, default_ports + [port], peer_name, peer_nonce)
                                        self.graph.update({_peer_ip: new_node})
                            else:
                                if _peer_ip not in self.graph:
                                    new_node = Node(_peer_id, _peer_ip, default_ports + [port], peer_name, peer_nonce)
                                    self.graph.update({_peer_ip: new_node})

                            _peer = self.graph[_peer_ip]

                            if _peer_ip != _vertex_ip:
                                peers_id.append(_peer_id)

                            self.update_root_name_nonce(root, _peer_id, peer_name, peer_nonce)

                            if not _peer.visited:
                                node_stack.push(_peer_ip)

                            _peer.peers = list(dict.fromkeys(peers_id))
                            node_stop_time = timeit.default_timer()
                            _peer.time_get_basic_info = node_stop_time - node_start_time

            stop_time = timeit.default_timer()
            self.time_traversal_graph = stop_time - start_time

        except TraversalGraphException:
            msg = "Traversal graph by dfs has error!"
            throw_error(msg, TraversalGraphException)

        finally:
            self.graph_db.close_db()

        print("time of traversing the current graph: ", self.time_traversal_graph)
        print("total number of vertex in the current graph: ", len(self.graph))

    def get_next_vertex_id(self):
        get_number_of_nodes_command = "SELECT COUNT(*) FROM " + db_meta.table_nodes_id.name + ";"
        number = self.graph_db.query_items_with_command(get_number_of_nodes_command)
        return number[0][0]

    def get_vertex_id_with_ip(self, ip_address):
        check_vertex_command = "SELECT * FROM " + db_meta.table_nodes_id.name + " WHERE " + \
                               db_meta.table_nodes_id_header_node_ip['name'] + "='" + ip_address + "';"
        vertex_info = self.graph_db.query_items_with_command(check_vertex_command)
        if vertex_info:
            return vertex_info[0][1]
        else:
            return None

    def add_vertex_to_table_nodes_id(self, node_basic_info):
        ip_address = node_basic_info[0]
        vertex_id = node_basic_info[1]
        port = node_basic_info[2]

        headers = "(" + db_meta.table_nodes_id.get_all_headers() + ")"
        db_meta.table_nodes_id.set_value_in_a_row(ip_address, column_id=0)
        db_meta.table_nodes_id.set_value_in_a_row(vertex_id, column_id=1)
        db_meta.table_nodes_id.set_value_in_a_row(port, column_id=2)
        values = "(" + db_meta.table_nodes_id.get_all_values() + ")"
        self.graph_db.insert_item(headers, values, db_meta.table_nodes_id.name)

    def update_root_name_nonce(self, root_node, vertex_id, peer_name, peer_nonce):
        if not self.root_update:
            if vertex_id == root_node.id:
                if not root_node.node_name:
                    root_node.node_name = peer_name
                if not root_node.node_nonce:
                    root_node.node_nonce = peer_nonce
                if root_node.node_name and root_node.node_nonce:
                    self.root_update = True

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

    def get_graph_asymmetric_matrix(self):
        matrix_row_dim = len(self.graph)
        matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)
        nodes_sequence = []

        dim_count = 0
        for node in self.graph:
            nodes_sequence.append([node])

            peers = self.graph[node].peers
            matrix[dim_count, peers] = 1
            dim_count += 1

        return [nodes_sequence, matrix]

    def get_graph_symmetric_matrix(self):
        matrix_row_dim = len(self.graph)
        matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)
        nodes_sequence = []

        dim_count = 0
        for node in self.graph:
            nodes_sequence.append(node)
            peers = self.graph[node].peers
            matrix[dim_count, peers] = 1
            if peers:
                for end_node in peers:
                    matrix[end_node, dim_count] = 1
            dim_count += 1
        return [nodes_sequence, matrix]

    def get_nodes_detail(self, node_matrix, timestamp):
        node_sequence = node_matrix[0]
        adjacent_matrix = node_matrix[1]

        self.graph_db.start_db()
        headers = '(' + db_meta.hypertable_nodes_all.get_all_headers() + ')'
        headers_list = db_meta.hypertable_nodes_all.get_all_headers().split(', ')

        for node_dim in range(len(node_sequence)):
            ip_address = node_sequence[node_dim]
            node = self.graph[ip_address]

            node_start_time = timeit.default_timer()
            node_name = node.node_name
            node_nonce = node.node_nonce
            port = node.port
            link = node.link

            node_info = dict()
            node_info.update({db_meta.hypertable_nodes_all_header_time['name']: timestamp})
            node_info.update({db_meta.hypertable_nodes_all_header_vertex_id['name']: node.id})
            node_info.update({db_meta.hypertable_nodes_all_header_ip_address['name']: ip_address})
            node_info.update({db_meta.hypertable_nodes_all_header_status['name']: node.status})
            node_info.update({db_meta.hypertable_nodes_all_header_node_name['name']: node_name})
            node_info.update({db_meta.hypertable_nodes_all_header_node_nonce['name']: node_nonce})
            node_info.update({db_meta.hypertable_nodes_all_header_port['name']: port})
            node_info.update({db_meta.hypertable_nodes_all_header_number_peers['name']: np.sum(adjacent_matrix[node_dim, :])})

            if node.status:
                node_info.update({db_meta.hypertable_nodes_all_header_wallet_address['name']: get_node_wallet_address(link)})
                node_info.update({db_meta.hypertable_nodes_all_header_height['name']: get_node_height(link)})
                node_info.update({db_meta.hypertable_nodes_all_header_version['name']: get_node_version(link)})
            else:
                node_info.update({db_meta.hypertable_nodes_all_header_wallet_address['name']: None})
                node_info.update({db_meta.hypertable_nodes_all_header_height['name']: None})
                node_info.update({db_meta.hypertable_nodes_all_header_version['name']: None})

            node_info.update({db_meta.hypertable_nodes_all_header_location['name']: get_location_ip(ip_address)})

            node_stop_time = timeit.default_timer()
            node_info.update({db_meta.hypertable_nodes_all_header_time_basic['name']: node.time_get_basic_info})

            node.time_get_details = node_stop_time - node_start_time
            node_info.update({db_meta.hypertable_nodes_all_header_time_details['name']: node.time_get_details})

            for header in headers_list:
                db_meta.hypertable_nodes_all.set_value_in_a_row(node_info[header], column_name=header)
            values = "(" + db_meta.hypertable_nodes_all.get_all_values() + ")"
            self.graph_db.insert_item(headers, values, db_meta.hypertable_nodes_all.name)

        self.graph_db.close_db()

    def output_graph_by_number_peers(self, all_nodes_info):

        headers = ['vertex_id'] + all_nodes_info[0]
        nodes_info = all_nodes_info[1]

        [active_nodes, inactive_nodes] = get_nodes_with_status(nodes_info)
        active_nodes_sort = sort_nodes_by_number_peers(active_nodes)
        inactive_nodes_sort = sort_nodes_by_number_peers(inactive_nodes)

        output_items_to_csv_file(headers, active_nodes_sort + inactive_nodes_sort)

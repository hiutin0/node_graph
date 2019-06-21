from lib.graph_operations import *
from lib.stack import Stack
from utils.api_operation import *
from node import *
from utils.errors import *
import timeit
import numpy as np
from graph_db import GraphDB
from time import gmtime, strftime


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

        self.db_table_time = 'timestamp_to_time_id'
        self.db_table_time_headers_1 = 'time'
        self.db_table_time_headers_2 = 'time_id'
        self.db_table_time_headers = '(time, time_id)'
        self.db_table_time_headers_complete = [['time', 'timestamp', 'not null'],
                                               ['time_id', 'bigint', 'not null']]

        self.db_table_nodes_id = 'nodes_id'
        self.db_table_nodes_id_headers_1 = 'node_ip'
        self.db_table_nodes_id_headers_2 = 'vertex_id'
        self.db_table_nodes_id_headers_3 = 'port'
        self.db_table_nodes_id_headers_complete = [['node_ip', 'inet', 'primary key'],
                                                   ['vertex_id', 'bigint', 'not null'],
                                                   ['port', 'int', None]]

        self.db_hypertable_nodes_all = 'nodes_all'
        self.db_hypertable_nodes_all_headers = ''
        self.db_hypertable_nodes_all_headers_complete = [['time', 'timestamp', 'not null'],
                                                         ['vertex_id', 'bigint', 'not null'],
                                                         ['ip_address', 'inet', 'not null'],
                                                         ['port', 'int', None],
                                                         ['status', 'boolean', 'not null'],
                                                         ['node_name', 'varchar(256)', 'not null'],
                                                         ['node_nonce', 'int', 'not null'],
                                                         ['number_peers', 'int', 'not null'],
                                                         ['address', 'varchar(40)', None],
                                                         ['height', 'bigint', None],
                                                         ['version', 'varchar(16)', None],
                                                         ['location', 'varchar(128)', None],
                                                         ['time_get_basic_info', 'real', 'not null'],
                                                         ['time_get_details', 'real', 'not null']]

    def initialize_db(self, hostname, user_name, password, clear_old_db=False):
        db_name = self.graph_name

        self.graph_db = GraphDB(hostname, db_name, user_name, password)
        self.graph_db.start_db()

        try:
            if clear_old_db:
                self.graph_db.drop_db(db_name)

            if not self.graph_db.check_db():
                self.graph_db.create_db()

            if not self.graph_db.check_table(self.db_table_time):
                self.graph_db.create_table(self.db_table_time, self.db_table_time_headers_complete)

            if not self.graph_db.check_table(self.db_table_nodes_id):
                self.graph_db.create_table(self.db_table_nodes_id, self.db_table_nodes_id_headers_complete)

            self.graph_db.add_extension_timescale()
            if not self.graph_db.check_table(self.db_hypertable_nodes_all):
                self.graph_db.create_hypertable(self.db_hypertable_nodes_all,
                                                self.db_hypertable_nodes_all_headers_complete)
        except InvalidInputException:
            msg = "Some errors in initialization of database!"
            throw_error(msg, InvalidInputException)
        finally:
            self.graph_db.close_db()

    def get_current_timestamp(self):
        self.current_timestamp = strftime("%Y-%m-%d %H:%M", gmtime()) + ':00'

    def get_next_time_id_in_db(self):
        get_number_of_timestamps_command = "SELECT COUNT(*) FROM " + self.db_table_time + ";"
        number = self.graph_db.query_items_with_command(get_number_of_timestamps_command)
        return number[0][0]

    def add_timestamp_to_table_time(self, timestamp, time_id):
        db_table_time_headers = "(" + self.db_table_time_headers_1 + ", " + self.db_table_time_headers_2 + ")"
        item = "('" + timestamp + "', " + str(time_id) + ")"
        self.graph_db.insert_item(db_table_time_headers, item, self.db_table_time)

    def traversal_graph_dfs(self, ip_address):
        if not self.graph_db:
            msg = "Traversal graph by dfs requires initialization of a graph database!"
            throw_error(msg, TraversalGraphException)

        nodes_all = {}
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

            nodes_all.update({ip_address: root})
            node_stack = Stack()
            node_stack.push(ip_address)

            start_time = timeit.default_timer()

            while not node_stack.is_empty():
                _vertex_ip = node_stack.pop()
                _vertex = nodes_all[_vertex_ip]

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
                                    if _peer_ip not in nodes_all:
                                        nodes_all.update({_peer_ip: new_node})
                                else:
                                    if _peer_ip not in nodes_all:
                                        new_node = Node(_peer_id, _peer_ip, default_ports + [port], peer_name, peer_nonce)
                                        nodes_all.update({_peer_ip: new_node})
                            else:
                                if _peer_ip not in nodes_all:
                                    new_node = Node(_peer_id, _peer_ip, default_ports + [port], peer_name, peer_nonce)
                                    nodes_all.update({_peer_ip: new_node})

                            _peer = nodes_all[_peer_ip]

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
        print("total number of vertex in the current graph: ", len(nodes_all))

        return nodes_all

    def get_next_vertex_id(self):
        get_number_of_nodes_command = "SELECT COUNT(*) FROM " + self.db_table_nodes_id + ";"
        number = self.graph_db.query_items_with_command(get_number_of_nodes_command)
        return number[0][0]

    def get_vertex_id_with_ip(self, ip_address):
        check_vertex_command = "SELECT * FROM " + self.db_table_nodes_id + " WHERE " + \
                               self.db_table_nodes_id_headers_1 + "='" + ip_address + "';"
        vertex_info = self.graph_db.query_items_with_command(check_vertex_command)
        if vertex_info:
            return vertex_info[0][1]
        else:
            return None

    def add_vertex_to_table_nodes_id(self, node_basic_info):
        ip_address = node_basic_info[0]
        vertex_id = node_basic_info[1]
        port = node_basic_info[2]
        if port:
            item = "('" + ip_address + "', " + str(vertex_id) + ", " + str(port) + ");"
            db_table_nodes_id_headers = '(' + self.db_table_nodes_id_headers_1 + ',' + \
                                        self.db_table_nodes_id_headers_2 + ',' + \
                                        self.db_table_nodes_id_headers_3 + ')'
        else:
            item = "('" + ip_address + "', " + str(vertex_id) + ");"
            db_table_nodes_id_headers = '(' + self.db_table_nodes_id_headers_1 + ',' + \
                                        self.db_table_nodes_id_headers_2 + ')'
        self.graph_db.insert_item(db_table_nodes_id_headers, item, self.db_table_nodes_id)

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



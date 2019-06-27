from lib.graph_operations import *
from utils.errors import *
from vsys_graph import Graph
import utils.db_meta as db_meta
import timeit
import time
import setting as node_analysis_setting
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


class NodeAnalysis:
    def __init__(self, name, application_name, ip, ports):
        self.ip = ip
        self.ports = ports
        self.new_graph = Graph(name, application_name)
        self.wait_time = 0
        node_analysis_setting.check_directory_storing_results(node_analysis_setting.clear_old_results)

    def wait_time_after_analysis_finished(self):
        if self.wait_time < 0:
            msg = "Need to increase time gap for successive analysis!"
            throw_error(msg, InvalidInputException)
        else:
            time.sleep(self.wait_time)

    def successive_node_analysis(self, rounds=1, time_gap=800, non_stop=False):
        self.new_graph.initialize_db(hostname, user_name, password, clear_old_db=node_analysis_setting.clear_old_database)
        try:
            while rounds:
                vsys_node_analysis.new_graph.graph_db.start_db()

                current_round_start_time = timeit.default_timer()

                vsys_node_analysis.new_graph.get_current_timestamp()
                current_timestamp = vsys_node_analysis.new_graph.current_timestamp
                next_time_id = vsys_node_analysis.new_graph.get_next_time_id_in_db()

                vsys_node_analysis.new_graph.add_timestamp_to_table_time(current_timestamp, next_time_id)
                vsys_node_analysis.new_graph.traversal_graph_dfs(self.ip)
                nodes_and_matrix = vsys_node_analysis.new_graph.get_graph_symmetric_matrix()
                vsys_node_analysis.new_graph.get_nodes_detail(nodes_and_matrix, current_timestamp)

                timestamp = self.new_graph.get_the_last_timestamp()
                [symmetric_matrix, asymmetric_matrix, dim_to_vertex_id] = self.get_matrix_with_timestamp(timestamp)

                self.plot_node_matrix_save(symmetric_matrix, dim_to_vertex_id, timestamp)
                self.plot_snapshot_graph(asymmetric_matrix, dim_to_vertex_id, timestamp)

                self.output_node_details_to_csv_file()

                current_round_stop_time = timeit.default_timer()

                self.wait_time = time_gap - int(current_round_stop_time - current_round_start_time)

                self.wait_time_after_analysis_finished()

                rounds -= 1
                if non_stop:
                    rounds += 1
        except NodeAnalysisException:
            msg = "Some error in successive node analysis!"
            throw_error(msg, NodeAnalysisException)
        finally:
            vsys_node_analysis.new_graph.graph_db.close_db()

    def get_matrix_with_timestamp(self, timestamp):
        vsys_node_analysis.new_graph.graph_db.start_db()
        all_nodes_info = self.new_graph.get_nodes_info_with_timestamp(timestamp)

        vertex_id_map_dim = {}
        dim_map_vertex_id = {}
        matrix_row_dim = len(all_nodes_info)
        symmetric_matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)
        asymmetric_matrix = np.zeros([matrix_row_dim, matrix_row_dim], dtype=np.int8)

        dim_count = 0
        for node in all_nodes_info:
            id_header_name = db_meta.hypertable_nodes_all_header_vertex_id['name']
            id_dim = db_meta.hypertable_nodes_all.headers[id_header_name].header_id
            vertex_id = node[id_dim]
            dim_map_vertex_id.update({dim_count: vertex_id})
            vertex_id_map_dim.update({vertex_id: dim_map_vertex_id[dim_count]})
            dim_count += 1

        for node in all_nodes_info:
            id_header_name = db_meta.hypertable_nodes_all_header_vertex_id['name']
            id_dim = db_meta.hypertable_nodes_all.headers[id_header_name].header_id
            vertex_id = node[id_dim]
            vertex_id_by_dim = vertex_id_map_dim[vertex_id]

            status_header_name = db_meta.hypertable_nodes_all_header_status['name']
            status_dim = db_meta.hypertable_nodes_all.headers[status_header_name].header_id
            status = node[status_dim]

            peers_header_name = db_meta.hypertable_nodes_all_header_peers['name']
            peers_dim = db_meta.hypertable_nodes_all.headers[peers_header_name].header_id
            peers_id = node[peers_dim].strip('|').split(' ')
            peers_id_map_to_dim = [vertex_id_map_dim[int(i)] for i in peers_id]

            symmetric_matrix[vertex_id_by_dim, peers_id_map_to_dim] = 1

            if status == 'True':
                asymmetric_matrix[vertex_id_by_dim, peers_id_map_to_dim] = 1

        return [symmetric_matrix, asymmetric_matrix, dim_map_vertex_id]

    def plot_node_matrix_save(self, matrix, dim_to_vertex_id, timestamp):
        file_name = timestamp.replace(':', '-') + '_matrix_plot.png'
        target = node_analysis_setting.path_storing_results + "/" + file_name
        plt.figure()
        plt.matshow(matrix)
        plt.savefig(target, bbox_inches='tight')

    def output_node_details_to_csv_file(self, timestamp=None):
        vsys_node_analysis.new_graph.graph_db.start_db()
        if not timestamp:
            timestamp = self.new_graph.get_the_last_timestamp()

        if timestamp:
            all_nodes_info = self.new_graph.get_nodes_info_with_timestamp(timestamp)
        else:
            all_nodes_info = None

        if all_nodes_info:
            headers_and_nodes_info = formalize_item_detailed_info(all_nodes_info)
            headers = headers_and_nodes_info[0]
            nodes_info = headers_and_nodes_info[1]
            [active_nodes, inactive_nodes] = get_nodes_with_status(nodes_info)
            active_nodes_sort = sort_nodes_by_number_peers(active_nodes)
            inactive_nodes_sort = sort_nodes_by_number_peers(inactive_nodes)
            output_items_to_csv_file(headers[1:], active_nodes_sort + inactive_nodes_sort, timestamp)
        else:
            msg = "No node information to output csv!"
            throw_error(msg, InvalidInputException)

    def plot_snapshot_graph(self, asymmetric_matrix, dim_to_vertex_id, timestamp):
        dim = list(dim_to_vertex_id.keys())
        nodes = [dim_to_vertex_id[d] for d in dim]

        nodes_graph = nx.DiGraph()
        nodes_graph.add_nodes_from(nodes)
        color_list = ['blue'] * len(nodes)

        number_dim = asymmetric_matrix.shape[0]

        for d in range(number_dim):
            node_id = dim_to_vertex_id[d]
            peers_by_dim = np.nonzero(asymmetric_matrix[d, :])[0]
            if len(peers_by_dim) == 0:
                color_list[node_id] = 'red'
            peers = [dim_to_vertex_id[i] for i in peers_by_dim]
            for p in peers:
                nodes_graph.add_edge(node_id, p, length=len(peers))

        file_name = timestamp.replace(':', '-') + '_node_graph_plot.png'
        target = node_analysis_setting.path_storing_results + "/" + file_name
        pos = nx.circular_layout(nodes_graph, scale=1)
        plt.figure()
        nx.draw(nodes_graph, node_color=color_list, with_labels=True, pos=pos)
        plt.draw()
        plt.savefig(target, bbox_inches='tight')

    def plot_node_performance(self, ip):
        vsys_node_analysis.new_graph.graph_db.start_db()
        all_info = self.new_graph.get_nodes_info_with_ip(ip)

        status_header_name = db_meta.hypertable_nodes_all_header_status['name']
        status_dim = db_meta.hypertable_nodes_all.headers[status_header_name].header_id

        number_peers_header_name = db_meta.hypertable_nodes_all_header_number_peers['name']
        number_peers_dim = db_meta.hypertable_nodes_all.headers[number_peers_header_name].header_id

        x = []
        status = []
        number_peers = []
        for item in all_info:
            time_id = self.new_graph.get_timestamp_id(item[0])
            if time_id:
                x.append(time_id)
                if item[status_dim]:
                    status.append(node_analysis_setting.status_labels[item[status_dim]])
                else:
                    status.append(node_analysis_setting.status_labels['None'])

                number_peers.append(int(item[number_peers_dim]))

        file_name_status = ip + '_node_performance_status.png'
        target_status = node_analysis_setting.path_storing_results + "/" + file_name_status
        plt.figure()
        plt.plot(x, status)
        plt.yticks(list(node_analysis_setting.status_labels.values()), list(node_analysis_setting.status_labels.keys()))
        plt.savefig(target_status, bbox_inches='tight')

        file_name_number_peers = ip + '_node_performance_number_peers.png'
        target_number_peers = node_analysis_setting.path_storing_results + "/" + file_name_number_peers
        plt.figure()
        plt.plot(x, number_peers)
        plt.savefig(target_number_peers, bbox_inches='tight')


if __name__ == "__main__":
    graph_name = node_analysis_setting.graph_name
    system_application_name = node_analysis_setting.system_application_name

    hostname = node_analysis_setting.hostname
    user_name = node_analysis_setting.user_name
    password = node_analysis_setting.password

    ip_address = node_analysis_setting.ip_address
    default_ports = node_analysis_setting.default_ports

    vsys_node_analysis = NodeAnalysis(graph_name, system_application_name, ip_address, default_ports)
    vsys_node_analysis.successive_node_analysis()
    vsys_node_analysis.plot_node_performance(vsys_node_analysis.ip)


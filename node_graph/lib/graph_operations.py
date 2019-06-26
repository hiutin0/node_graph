import csv
import utils.setting as node_analysis_setting
import utils.db_meta as db_meta


def sort_nodes_by_number_peers(all_nodes):
    sort_item_name = db_meta.hypertable_nodes_all_header_number_peers['name']
    result = sorted(all_nodes.items(), key=lambda x: x[1][sort_item_name], reverse=True)
    return result


def formalize_item_detailed_info(all_info):
    nodes_info = {}
    headers_list = db_meta.hypertable_nodes_all.get_all_headers().split(', ')
    for item in all_info:
        node = {}
        for position in range(1, len(item)):
            node.update({headers_list[position]: item[position]})
        ip_address = db_meta.hypertable_nodes_all_header_ip_address['name']
        header_id = db_meta.hypertable_nodes_all.headers[ip_address].header_id
        nodes_info.update({item[header_id]: node})

    return [headers_list, nodes_info]


def get_nodes_with_status(nodes_info):
    active_nodes = {}
    inactive_nodes = {}
    for item in nodes_info:
        node = nodes_info[item]
        if node[db_meta.hypertable_nodes_all_header_status['name']] == 'True':
            active_nodes.update({item: node})
        else:
            inactive_nodes.update({item: node})
    return [active_nodes, inactive_nodes]


def output_items_to_csv_file(headers, items, timestamp):
    field_names = headers
    file_name = timestamp.replace(':', '-') + '_nodes_details.csv'
    target = node_analysis_setting.path_storing_results + "/" + file_name

    with open(target, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        for item in items:
            writer.writerow(item[1])
    csv_file.close()

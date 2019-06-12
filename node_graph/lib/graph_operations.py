import csv


def sort_nodes_by_number_peers(all_nodes):
    result = sorted(all_nodes.items(), key=lambda x: x[1]['number_peers'], reverse=True)
    return result


def get_nodes_with_status(nodes_info):
    active_nodes = {}
    inactive_nodes = {}
    for key in nodes_info:
        if nodes_info[key]['status']:
            active_nodes.update({key: nodes_info[key]})
        else:
            inactive_nodes.update({key: nodes_info[key]})
    return [active_nodes, inactive_nodes]


def output_items_to_csv_file(headers, items):
    field_names = headers
    file_name = 'nodes_details.csv'
    with open(file_name, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()

        for item in items:
            vertex_id = item[0].split('-')[1]
            row_info = dict()
            row_info.update({field_names[0]: vertex_id})
            row_info.update(item[1])
            writer.writerow(row_info)

    csv_file.close()

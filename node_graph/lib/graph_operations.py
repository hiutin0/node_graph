
import csv


def sort_nodes_by_number_peers(all_nodes):
    result = sorted(all_nodes.items, key=lambda x: x[1]['number_peers'], reverse=True)
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
    fieldnames = headers
    with open('nodes_details.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            vertex_id = item[0].split('-')[1]
            row_info = dict()
            row_info.update({fieldnames[0]: vertex_id})
            row_info.update(item[1])
            writer.writerow(row_info)

    csvfile.close()

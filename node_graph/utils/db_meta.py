from utils.errors import *

data_type_without_quote = ['bigint', 'int', 'real']
header_name = 'name'
header_data_type = 'dtype'
header_attribute = 'attribute'


class TableHeader:
    def __init__(self, name, data_type, attribute):
        self.name = name
        self.data_type = data_type
        self.attribute = attribute
        self.header_id = 0
        self.value = None

    def set_header_id(self, number):
        self.header_id = number

    def set_value_to_db_format(self, data):
        if self.data_type in data_type_without_quote:
            if data:
                self.value = str(data)
            else:
                self.value = "'" + str(data) + "'"
        else:
            self.value = "'" + str(data) + "'"


class Table:
    def __init__(self, name):
        self.name = name
        self.headers = {}
        self.headers_list = []

    def add_table_header(self, header):
        new_header = TableHeader(header[header_name], header[header_data_type], header[header_attribute])
        new_header.set_header_id(len(self.headers_list))
        self.headers.update({new_header.name: new_header})
        self.headers_list.append(new_header.name)

    def get_headers_with_desc(self):
        headers = []
        for head in self.headers_list:
            headers.append([self.headers[head].name, self.headers[head].data_type, self.headers[head].attribute])
        return headers

    def set_value_in_a_row(self, value, column_id=None, column_name=None):
        if column_id and (not column_name):
            self.headers[self.headers_list[column_id]].set_value_to_db_format(value)

        if (not column_id) and (not column_name):
            if column_id != 0:
                msg = "Invalid input to set value in a row of a table!"
                throw_error(msg, InvalidInputException)
            else:
                self.headers[self.headers_list[column_id]].set_value_to_db_format(value)

        if (not column_id) and column_name:
            if column_id != 0:
                self.headers[column_name].set_value_to_db_format(value)
            else:
                if self.headers_list[column_id] == column_name:
                    self.headers[column_name].set_value_to_db_format(value)

        if column_id and column_name:
            if self.headers_list[column_id] != column_name:
                msg = "Invalid input to set value in a row of a table!"
                throw_error(msg, InvalidInputException)
            else:
                self.headers[column_name].set_value_to_db_format(value)

    def get_all_headers(self):
        return ', '.join(self.headers_list)

    def get_all_values(self):
        return ', '.join([self.headers[head].value for head in self.headers])


table_time = Table('timestamp_to_time_id')
table_time_header_time = {header_name: 'time', header_data_type: 'timestamp', header_attribute: 'not null'}
table_time_header_time_id = {header_name: 'time_id', header_data_type: 'bigint', header_attribute: 'not null'}
table_time.add_table_header(table_time_header_time)
table_time.add_table_header(table_time_header_time_id)

table_nodes_id = Table('nodes_id')
table_nodes_id_header_node_ip = {header_name: 'node_ip', header_data_type: 'inet', header_attribute: 'primary key'}
table_nodes_id_header_vertex_id = {header_name: 'vertex_id', header_data_type: 'bigint', header_attribute: 'not null'}
table_nodes_id_header_port = {header_name: 'port', header_data_type: 'varchar(10)', header_attribute: None}
table_nodes_id.add_table_header(table_nodes_id_header_node_ip)
table_nodes_id.add_table_header(table_nodes_id_header_vertex_id)
table_nodes_id.add_table_header(table_nodes_id_header_port)

hypertable_nodes_all = Table('nodes_all')
hypertable_nodes_all_header_time = {header_name: 'time', header_data_type: 'timestamp', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_time)
hypertable_nodes_all_header_vertex_id = {header_name: 'vertex_id', header_data_type: 'bigint', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_vertex_id)
hypertable_nodes_all_header_ip_address = {header_name: 'ip_address', header_data_type: 'inet', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_ip_address)
hypertable_nodes_all_header_port = {header_name: 'port', header_data_type: 'varchar(10)', header_attribute: None}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_port)
hypertable_nodes_all_header_status = {header_name: 'status', header_data_type: 'varchar(5)', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_status)
hypertable_nodes_all_header_node_name = {header_name: 'node_name', header_data_type: 'varchar', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_node_name)
hypertable_nodes_all_header_node_nonce = {header_name: 'node_nonce', header_data_type: 'int', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_node_nonce)
hypertable_nodes_all_header_number_peers = {header_name: 'number_peers', header_data_type: 'int', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_number_peers)
hypertable_nodes_all_header_peers = {header_name: 'peers', header_data_type: 'varchar', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_peers)
hypertable_nodes_all_header_wallet_address = {header_name: 'wallet_address', header_data_type: 'varchar(40)', header_attribute: None}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_wallet_address)
hypertable_nodes_all_header_height = {header_name: 'height', header_data_type: 'varchar(256)', header_attribute: None}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_height)
hypertable_nodes_all_header_version = {header_name: 'version', header_data_type: 'varchar(16)', header_attribute: None}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_version)
hypertable_nodes_all_header_location = {header_name: 'location', header_data_type: 'varchar', header_attribute: None}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_location)
hypertable_nodes_all_header_time_basic = {header_name: 'time_basic', header_data_type: 'real', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_time_basic)
hypertable_nodes_all_header_time_details = {header_name: 'time_details', header_data_type: 'real', header_attribute: 'not null'}
hypertable_nodes_all.add_table_header(hypertable_nodes_all_header_time_details)

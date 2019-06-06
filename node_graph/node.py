from utils.ip_operation import *
from utils.errors import *
import requests


valid_status_code = 200
time_wait_for_response = 0.5


def get_node_link(ip, port):
    return "http://" + ip + ":" + port


def get_node_valid_port(url='', ip='', ports=None):
    if (not url) and (not ip) and (not ports):
        msg = "No input for api request!"
        throw_error(msg, InvalidInputException)

    valid_ports = []
    if ip and ports:
        for port in ports:
            link = get_node_link(ip, port)
            try:
                response = requests.get(link, timeout=time_wait_for_response).status_code
            except:
                response = 400
            if response == valid_status_code:
                valid_ports.append(port)
    if url:
        try:
            response = requests.get(url, timeout=time_wait_for_response).status_code
        except:
            response = 400
        if response == valid_status_code:
            valid_ports.append(url.split(':')[-1])

    if not valid_ports:
        return None
    else:
        return valid_ports[-1]


class Node:
    def __init__(self, vertex_id, ip, default_ports, name='', nonce=''):
        self.id = vertex_id
        self.ip_address = ip
        self.node_name = name
        self.node_nonce = nonce
        self.peers = list()

        self.port = get_node_valid_port(ip=self.ip_address, ports=default_ports)
        if self.port:
            self.status = True
            self.link = get_node_link(self.ip_address, self.port)
        else:
            self.status = False
            self.link = None
        self.visited = False

    def ip_hex(self):
        return ip_to_hex_string(self.ip_address)



import logging
import requests
from requests.exceptions import RequestException
from .errors import NetworkException


def api_default_port():
    return ['9922']


def get_node_link(node, port):
    return "http://" + node + ":" + port


def get_peer_nodes(node, ports):
    for port in ports:
        print(get_node_link(node, port))
        print(get_height(get_node_link(node, port)))


def get_height(link):
    url = link + '/blocks/height'
    return request(url)['height']


def request(url, post_data='', api_key=''):
    headers = {}
    if api_key:
        headers['api_key'] = api_key
    header_str = ' '.join(['--header \'{}: {}\''.format(k, v) for k, v in headers.items()])
    try:
        if post_data:
            headers['Content-Type'] = 'application/json'
            data_str = '-d {}'.format(post_data)
            logging.info("curl -X POST %s %s %s" % (header_str, data_str, url))
            return requests.post(url, data=post_data, headers=headers).json()
        else:
            logging.info("curl -X GET %s %s" % (header_str, url))
            return requests.get(url, headers=headers).json()
    except RequestException as ex:
        msg = 'Failed to get response: {}'.format(ex)
        raise NetworkException(msg)

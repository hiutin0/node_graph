import logging
import requests
from requests.exceptions import RequestException
from .errors import NetworkException
from bs4 import BeautifulSoup

valid_status_code = 200
time_wait_for_response = 1


def set_api_default_port():
    return ['9922']


def check_node_request(url):
    return requests.get(url).status_code


def get_node_version(link):
    url = link + '/node/version'
    return request(url)['version']


def get_location_ip(ip):
    website = 'https://tools.keycdn.com/geo'
    url = website + '?host=' + ip
    try:
        response = requests.get(url, timeout=time_wait_for_response)
        status = response.status_code
    except:
        status = 400

    if status == valid_status_code:
        html = requests.get(url).content
        soup = BeautifulSoup(html, 'html.parser')

        content = soup.find('div', {"id": "geoResult"})
        if content:
            content_details = content.find_all('dd')
            if len(content_details) >= 4:
                country = content_details[3].text
            else:
                country = None

            if len(content_details) >= 2:
                region = content_details[1].text
            else:
                region = None

            location = region + ' | ' + country
        else:
            location = None
        return location
    else:
        return None


def parse_ip_port_name_nonce(item):
    [ip, port] = parse_ip_port(item)
    peer_name = item['peerName']
    peer_nonce = item['peerNonce']

    return [ip, port, peer_name, peer_nonce]


def parse_ip_port(item):
    ip_port = item['address'].split('/')[-1]
    [ip, port] = ip_port.split(':')
    return [ip, port]


def get_node_wallet_address(link):
    url = link + '/addresses'
    return request(url)[0]


def get_peer_nodes(link):
    url = link + '/peers/connected'
    return request(url)['peers']


def get_node_height(link):
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

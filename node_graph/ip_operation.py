from utils.errors import *


def ip_to_hex_string(s):
    number = s.split('.')
    hex_string = ''.join([hex(int(i))[2:] for i in number])
    return hex_string


def hex_string_to_ip(s):
    if not s:
        msg = "Input cannot be empty!"
        throw_error(msg, InvalidInputException)

    if len(s) % 2 != 0:
        msg = "Input is invalid!"
        throw_error(msg, InvalidInputException)

    ip = ''
    for i in range(int(len(s)/2)):
        position_first = int(2*i)
        position_second = int(2*(i + 1))
        hex_string = int(s[position_first:position_second], 16)
        ip = ip + str(hex_string)
        if i < (int(len(s)/2) - 1):
            ip = ip + '.'
    return ip

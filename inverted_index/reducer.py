# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 18:31:42 2019

@author: amits
"""

import socket
import pickle
import select
import sys


def recv_msg(sock):
    """
    Receives message sent by server. The message is recieved in chunks with 
    the maximum size of chunk equal to 4096 bytes.
    """
    try:
        # Wait till the socket is ready to receive data
        read, _, _ = select.select([sock], [], [])
        chunks = []
        block = None
        while True:
            block = sock.recv(4096)
            if not block:
                break
            chunks.append(block)
    except socket.timeout:
        pass
    msg = pickle.loads(b''.join(chunks))
    return msg


def send_msg(sock, msg):
    """
    Sends the message to server. If msg is larger than buffersize then the message
    is split into chunks and the chunks are sent seperately.
    """
    # Wait until socket is ready for sending data
    _, write, _ = select.select([], [sock], [])
    for i in range(0, len(msg), 4096):
        sent = sock.send(msg[i:i+4096])
        # If no data is being sent, we can assume that the connection has died.
        if sent == 0:
            raise Exception("Socket Connection Broken")

def get_key(sock, key):
    """
    Sends the get command to server
    """
    msg = pickle.dumps(('get', key))
    send_msg(sock, msg)
    msg = recv_msg(sock)
    if msg == False:
        raise(Exception("Key not found.."))
    if msg[0] != len(msg[1]):
        raise(Exception("Transmission Error!!"))
    return msg[1]

def set_key(sock, key, value):
    """
    Sends the set command to server
    """
    msg = pickle.dumps(('set', key, len(value), value))
    send_msg(sock, msg)
    msg = recv_msg(sock)
    if msg == False:
        raise(Exception("Couldn't set value.."))

def connect_store(data_store):
    try:
        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cs.connect((data_store[0], data_store[1]))
        cs.setblocking(False)
        cs.settimeout(0.5)
        return cs
    except:
        if cs:
            cs.close()
            del cs
        return False

if __name__ == "__main__":
    try:
        data_store = (sys.argv[1], int(sys.argv[2]))
        input_key = sys.argv[3]
        output_key = sys.argv[4]
        cs = connect_store(data_store)
        if not cs:
            raise Exception("Can't connect to data store..")
        input_data = get_key(cs, input_key)
        di = {}
        for tup in input_data:
            if tup[0] in di:
                di[tup[0]].append(tup[1])
            else:
                di[tup[0]] = [tup[1]]
        set_key(cs, output_key, di)
    except Exception as e:
        sys.stderr.buffer.write(e)
    finally:
        cs.close()
        del cs
        del input_data
        del di

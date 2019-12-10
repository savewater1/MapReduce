# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 22:08:43 2019

@author: amits
"""


import time
import sys
import json
import select
import socket
import pickle
import subprocess
import logging
import xmlrpc.client
import pdb



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



## The main program is called with the path of configuration file which
##  MUST be stored at the directory where main program is executing
if __name__ == "__main__":
    try:
#        logging.basicConfig(filename="mapreduce.log", level = logging.DEBUG)
        logger = logging.getLogger('main')
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler("mapreduce.log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatterF = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
        formatterC = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatterF)
        ch.setFormatter(formatterC)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.debug("Beginning..")
        config_file = sys.argv[1]
        logger.debug("Reading configuration file..")
        with open(config_file, "rb") as file:
            config = json.load(file)
        
        # Driver program configurations
        main = config["main"]
        # Data Store
        store = config["store"]
        # Data Store Address
        if store["task_address"] == main["task_address"]:
            data_store = (store["task_address"], store["task_port"])
        else:
            data_store = (store["external_ip"], store["task_port"])
        
        # Estabilishing connection to data store
        logger.debug("Estabilihing connection to data store..")
        cs = connect_store(data_store)
        if not cs:
            raise Exception("Can't connect to data store..")
        
        set_key(cs, "config", config)
        logger.debug("Configuration file stored on key-value store")

        with open(config["input_data"]["task_name"], 'rb') as file:
            input_data = pickle.load(file)
        logger.debug("Input data read from file..")
        
        # Main program address
        main = config["main"]
        # Key for input data
        input_key = config["input_data"]["task_address"]
        logger.debug("Store input data into data store using input key..")
        set_key(cs, input_key, input_data)
        
        mapper = config["mapper"]
        reducer = config["reducer"]
        
        # Configuration of master
        master = config["master"]
        # Send master script to node executing master if the
        # node is different than the one executing main program
        if main["task_address"]!=master["task_address"]:
            cmd = ["gcloud", "compute", "ssh", master['task_address'], "--command", "python3 "+master['task_name']+" config "+store["task_address"]+" "+str(store["task_port"]), "--", "-L"+str(master["task_port"])+":localhost:"+str(master["task_port"])]
        else:
            cmd = ["python3", master["task_name"], "config", store["task_address"], str(store["task_port"])]
        
#        pdb.set_trace()
        proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Waiting for master to get started
        time.sleep(10)
        with xmlrpc.client.ServerProxy("http://localhost"+":"+master["task_port"]+"/") as proxy:
            op = proxy.init_cluster()
            logger.debug(op)
            op = proxy.runmapred()
            logger.debug(op)
            op = proxy.destroy()
            logger.debug(op)
        
        proc.wait()
        result, error = proc.communicate()
        if result == [] or proc.returncode != 0:
            logger.error("Master Did Not FINISH!!")
            logger.error(str(error))
        else:
            output = get_key(cs, config["output_data"]["task_name"])
            logger.debug("Master finished successfully!!")
            logger.debug(error.decode())
            logger.debug(result.decode())
            logger.debug("Output: "+str(output))
            logger.info("Output: "+str(output))
    except Exception as e:
        logger.exception(e)
    finally:
        if proc.returncode == None:
            logger.debug("Killing master..")
            try:
                outs, errs = proc.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
                outs, errs = proc.communicate()
            logger.error("Master Did Not FINISH!!")
            logger.error(str(errs))
            logger.debug(str(outs))
        logger.debug("Exiting main!!")
        

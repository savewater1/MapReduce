# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 21:30:00 2019

@author: amits
"""

import socket
import sys
import select
import pickle
import subprocess
import logging
import uuid
import time
import threading
from xmlrpc.server import SimpleXMLRPCServer
import googleapiclient.discovery



class Error:
    def __init__(self):
        self._error = []
        self._flag = 0
        self._lock = threading.Lock()
    def set_error(self, msg):
        try:
            self._lock.acquire()
            self._error.append(msg)
            self._flag = 1
        finally:
            self._lock.release()
    def get_error(self):
        return self._error
    def get_flag(self):
        return self._flag


class Master:
    def __init__(self, data_store, config_file_name):
        self.data_store = data_store
        self.cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cs.connect(data_store)
        self.cs.setblocking(False)
        self.cs.settimeout(0.5)
        logging.debug("Getting the configuration file from key-value store")
        self.config = get_key(self.cs, config_file_name)
    
    def init_cluster(self):
        try:
            logging.debug("downloading input data from key-val store..")
            input_key = self.config['input_data']['task_address']
            input_data = get_key(self.cs, input_key)
            
            logging.info("partitioning data into chunks..")
            numWorkers = len(self.config['mapper']['workers'])
            ind = [int(i*len(input_data)/numWorkers) for i in range(numWorkers)] + [len(input_data)]
            if type(input_data) in (str, list, tuple):
                chunks = [input_data[ind[i]:ind[i+1]] for i in range(numWorkers)]
            elif type(input_data) == dict:
                input_data = list(input_data.items())
                chunks = [input_data[ind[i]:ind[i+1]] for i in range(numWorkers)]
            else:
                raise Exception("Input type not recognized..")
            
            logging.debug("storing chunks in key-value store..")
            self.chunk_keys = ["input"+str(i) for i in range(numWorkers)]
            for chunk_key, chunk in zip(self.chunk_keys, chunks):
                set_key(self.cs, chunk_key, chunk)
            
            # Clearing any existing data for intermediate key as mapper calls append method to
            # add data to intermediate_key
            logging.debug("Clearing existing data for key for intermediate value..")
            intermediate_data = self.config["intermediate_data"]
            set_key(self.cs, intermediate_data["task_address"], [])
            
            # Provisioning gcloud compute instances for worker nodes
            self.oslogin = googleapiclient.discovery.build("oslogin", "v1")
            # ADD service account details in config
            self.account = self.config["gcloud"]["service_account"]
            if not self.account.startswith('users/'):
                self.account = 'users/' + self.account
            self.compute = googleapiclient.discovery.build('compute', 'v1')
            # Add project, zone and network info in config
            self.gcloud = self.config["gcloud"]
            
            workers = self.config['mapper']['workers']
            master = self.config["master"]
            names = [worker["task_address"] for worker in workers if master["task_address"] != worker["task_address"]]
            if names:
                res = create_workers(names, self.gcloud["project_id"], self.gcloud["zone"], self.gcloud["network"])
                logging.debug(res)
            return "Server Initialized Successfully"
        except Exception as e:
            logging.exception(e)
            raise Exception("Error in init..")
            self.destroy()
            
    
    def runmapred(self): 
        try:
            error = Error()
            # Master configuration
            master = self.config["master"]
            # Mapper configuration
            mapper = self.config["mapper"] 
            # Intermediate Data configuration
            intermediate_data = self.config["intermediate_data"]
            logging.debug("Starting mappers..")
            for worker, ip in zip(mapper["workers"], self.chunk_keys):
                t = threading.Thread(target = start_worker, args = (error, self.oslogin, self.gcloud["service_account"], master, mapper, worker, self.data_store, intermediate_data["task_address"], ip))
                t.start()
            logging.debug('Waiting for all mappers to finish before calling reduce..')
            main_thread = threading.currentThread()
            for t in threading.enumerate():
                if t is not main_thread:
                    t.join()
            if error.get_flag() == 1:
                logging.error("\n".join(error.get_error()))
                raise(Exception("Worker Failure.."))
            logging.info("Workers finished execution of map tasks..")
            
            # Reducer configuration
            reducer = self.config["reducer"]
            # Output data configuration
            output_data = self.config["output_data"]
            rwa = set([rw["task_address"] for rw in reducer["workers"]])
            mwa = set([mw["task_address"] for mw in mapper["workers"]])
            new_workers = [a for a in rwa if (a not in mwa)&(a!="localhost")]
            del_workers = [a for a in mwa if (a not in rwa)&(a!="localhost")]
            if del_workers:
                logging.debug("Deleting map workers which won't be reused..")
                res = delete_workers(del_workers, self.project)
                logging.debug(res)
            if new_workers:
                logging.debug("Creating new workers for reduce tasks..")
                res = create_workers(new_workers, self.project, self.zone, self.network)
            logging.debug("Starting reducers..")
            for worker, ip in zip(reducer["workers"], self.chunk_keys):
                t = threading.Thread(target = start_worker, args = (error, master, mapper, worker, self.data_store, output_data["task_address"], intermediate_data["task_name"]))
                t.start()
            logging.debug('Waiting for all reducers to finish..')
            main_thread = threading.currentThread()
            for t in threading.enumerate():
                if t is not main_thread:
                    t.join()
            if error.get_flag() == 1:
                logging.error("\n".join(error.get_error()))
                raise(Exception("Worker Failure.."))
            logging.info("Workers finished execution of reduce tasks..")
# =============================================================================
#             t = threading.Thread(target = start_worker, args = (error, master, reducer, reducer["workers"], self.data_store, output_data["task_address"], intermediate_data["task_name"]))
#             t.start()
#             t.join()
#             if error.get_flag() == 1:
#                 logging.error("\n".join(error.get_error()))
#                 raise(Exception("Worker Failure.."))
# =============================================================================
            return "Successfully finished map and reduce tasks.."
        except Exception as e:
            logging.exception(e)
            raise Exception("Could not finish mapred..")
            self.destroy()
            
    
    def destroy(self):
        logging.debug("Existing master...")
        if self.cs:
            self.cs.close()
#            main = self.config["main"]
            master = self.config["master"]
#            if main["task_address"] != master["task_address"]:
#                send_log_file(main, master)
        # Deleting gcloud compute instances for worker nodes
        numWorkers = len(self.config['workers'])
        workers = self.config['workers']
        master = self.config["master"]
        for i in range(numWorkers):
            if master["task_address"] != workers[i]["task_address"]:
                pass
        t = threading.Thread(target = shutdown_thread)
        t.start()
        return "Destroyed master"



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


def create_workers(names, project, zone, network):
    """
    Create gcloud compute instance. These instances are used as worker nodes to execute
    map and reduce tasks. It accepts the following arguments:
        names: a list of names, one each for a worker node
        project: name of the project that compute instances are attached to
        zone: zone for instances
        network: virtual network used by the map-reduce system
    """
    cmd = ["gcloud", "compute", "instances", "create", " ".join(names), "--project "+project, "--zone "+zone, "--no-service-account", "--no-scopes", "--network "+network, "--metadata=enable-oslogin=TRUE", "--quiet"]
    proc = subprocess.Popen(cmd, shell = False, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    proc.wait()
    out, err = proc.communicate()
    logging.debug(out.decode())
    if err or proc.returncode!=0:
        raise Exception("Instance creation failure!!")
    return "Instances created successfully!!"


def delete_workers(names, project):
    """
    Delete gcloud compute instances.
    """
    cmd = ["gcloud", "compute", "instances", "delete", " ".join(names), "--project "+project, "--quiet"]
    proc = subprocess.Popen(cmd, shell = False, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    proc.wait()
    out, err = proc.communicate()
    logging.debug(out.decode())
    if err or proc.returncode!=0:
        raise Exception("Instance deletion failure!!")
    return "Instances terminated and deleted successfully!!"


def create_ssh_key(oslogin, account, private_key_file=None, expire_time=300):
    """Generate an SSH key pair and apply it to the specified account."""
    private_key_file = private_key_file or '/tmp/key-' + str(uuid.uuid4())
    cmd = ['ssh-keygen', '-t', 'rsa', '-N', '', '-f', private_key_file]
    keygen = subprocess.Popen(cmd, shell = False, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    keygen.wait()
    output = keygen.communicate()[0]
    returncode = keygen.returncode
    if returncode:
        raise subprocess.CalledProcessError(returncode, cmd)
    if output:
        logging.info(output)

    with open(private_key_file + '.pub', 'r') as original:
        public_key = original.read().strip()

    # Expiration time is in microseconds.
    expiration = int((time.time() + expire_time) * 1000000)

    body = {
        'key': public_key,
        'expirationTimeUsec': expiration,
    }
    oslogin.users().importSshPublicKey(parent=account, body=body).execute()
    return private_key_file


def start_worker(error, oslogin, account, master, task, worker, data_store, op_key, ip_key):
    """
    Helper function that executes map/reduce script on remote/local machine.
    Takes the following arguments:
        error: Common data structure shared between workers for error reporting
        master: Master's configurations
        task: Task configurations
        worker: worker's configurations
        data_store: key-val store's configurations
        op_key: Key to be used to store the output
        ip_key: Key to used to fetch the input
        oslogin: used to login into gcloud compute instance, if the worker is running on remote VM
        account: service account
    """
    if master["task_address"] != worker["task_address"]:
        private_key_file = create_ssh_key(oslogin, account)
        profile = oslogin.users().getLoginProfile(name=account).execute()
        username = profile.get('posixAccounts')[0].get('username')
        dest = worker["task_address"]+":"+"/home/"+username+task["task_name"]
        cmd = ["gcloud", "compute", "scp", task["task_name"], dest]
        logging.info("Send the map/reduce script to worker node..")
        scp = subprocess.Popen(cmd, shell = False, stderr = subprocess.PIPE)
        scp.wait()
        if scp.returncode != 0:
            _, errs = scp.communicate()
            if type(errs) == "bytes":
                errs = errs.decode()
            msg = "worker-" + str(worker["task_address"]) + " : " + errs
            error.set_error(msg)
        cmd = ["ssh", "-i", private_key_file, '-o', 'StrictHostKeyChecking=no', '{username}@{hostname}'.format(username=username, hostname=worker["task_address"]), "python3", task["task_name"], data_store[0], str(data_store[1]), ip_key, op_key]
    else:
        cmd = ["python3", task["task_name"], data_store[0], str(data_store[1]), ip_key, op_key]
    proc = subprocess.Popen(cmd, shell = False, stderr = subprocess.PIPE)
    proc.wait()
    errs = proc.stderr.readlines()
    if len(errs) != 0:
        msg = "worker-" + str(worker["task_address"]) + " : " + str(errs)
        error.set_error(msg)


def shutdown_thread():
    server.shutdown()


if __name__ == "__main__":
    try:
        logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
        logging.debug("Master started..")
        config_file_name = sys.argv[1]
        data_store = (sys.argv[2], int(sys.argv[3]))
        logging.debug("Data Store - "+data_store[0] + ":" +str(data_store[1]))
        
        m = Master(data_store, config_file_name)
        
        master = m.config["master"]
        with SimpleXMLRPCServer((master["task_address"], int(master["task_port"]))) as server:
            server.register_introspection_functions()
            server.register_instance(m)
            server.serve_forever()
        sys.stdout.buffer.write(b"Successfully exiting master..")
        sys.stdout.flush()
    except Exception as e:
        logging.exception(e)
    finally:
        if m:
            m.destroy()

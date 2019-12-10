# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 20:08:06 2019

@author: amits
"""

import socket
import threading
import pickle
import sys
import select



class ReadWriteLock:
    """ From :  https://www.oreilly.com/library/view/python-cookbook/0596001673/ch06s04.html   
    A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release()


class serverSocket:
    """
    Creates a server socket and implements some method to allow interaction with 
    the socket.
    """
    def __init__(self, s = None, mcr = 100, host = socket.gethostbyname(socket.gethostname()), port = 9889):
        if s == None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._sock = s
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        self._host = host
        self._port = port
        self._MAX_CONCURRENT_REQUESTS = mcr
        self._BUFFSIZE = 4096
        self._lock = ReadWriteLock()
    
    def bind(self):
        self._sock.bind((self._host, self._port))
    
    def listen(self):
        self._sock.listen(self._MAX_CONCURRENT_REQUESTS)
    
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    @property
    def BUFFSIZE(self):
        return self._BUFFSIZE
    
    @property
    def MAX_CONCURRENT_REQUESTS(self):
        return self._MAX_CONCURRENT_REQUESTS
            
    def accept_conn(self):
        conn, addr = self._sock.accept()
        return (conn, addr)
    
    def __del__(self):
        self._sock.close()
        


def client_thread(conn, addr, lock, filename, buff):
    """
    This functions implements all the code to process client requests.
    When a client is first connected to the server, a seperate thread, running this
    function, is called for each client.
    """
    try: 
        while True:
            try:
                read, _, _ = select.select([conn], [], [])
                chunks = []
                block = None
                while True:
                    block = conn.recv(buff)
                    if not block:
                        break
                    chunks.append(block)
            except socket.timeout:
                pass
            try:
                in_msg = pickle.loads(b''.join(chunks))
            except pickle.UnpicklingError:
                in_msg = [False]
            if in_msg:
                # GET command
                if in_msg[0] in ['get', 'GET']:
                    try:
                        lock.acquire_read()
                        try:
                            with open(filename, 'rb') as file:
                                di = pickle.load(file)
                        except (FileNotFoundError, EOFError):
                            di = {}
                        except Exception as e:
                            raise(e)
                        if in_msg[1] in di:
                            out_msg = pickle.dumps(di[in_msg[1]])
                        else:
                            out_msg = pickle.dumps(False)
                    finally:
                        lock.release_read()
                # SET command
                elif in_msg[0] in ['set', 'SET']:
                    try:
                        lock.acquire_write()
                        try:
                            with open(filename, 'rb') as file:
                                di = pickle.load(file)
                        except (FileNotFoundError, EOFError):
                            di = {}
                        except Exception as e:
                            raise(e)
                        try:
                            if in_msg[2] != len(in_msg[3]):
                                raise Exception
                            di[in_msg[1]] = (in_msg[2], in_msg[3])
                            with open(filename, 'wb') as file:
                                pickle.dump(di, file)
                            out_msg = pickle.dumps(True)
                        except:
                            out_msg = pickle.dumps(False)
                    finally:
                        lock.release_write()  
                elif in_msg[0] in ['apd', 'APD']:
                    try:
                        lock.acquire_write()
                        try:
                            with open(filename, 'rb') as file:
                                di = pickle.load(file)
                        except (FileNotFoundError, EOFError):
                            di = {}
                        except Exception as e:
                            raise(e)
                        try:
                            if in_msg[2] != len(in_msg[3]):
                                raise Exception
                            existing = di[in_msg[1]]
                            new_len = existing[0] + in_msg[2]
                            new_data = existing[1]+in_msg[3]
                            di[in_msg[1]] = (new_len, new_data)
                            with open(filename, 'wb') as file:
                                pickle.dump(di, file)
                            out_msg = pickle.dumps(True)
                        except:
                            out_msg = pickle.dumps(False)
                    finally:
                        lock.release_write()
                else:
                    out_msg = pickle.dumps(False)
                _, write, _ = select.select([], [conn], [])
                for i in range(0, len(out_msg), buff):
                    sent = conn.send(out_msg[i:i+buff])
                    if sent == 0:
                        raise Exception("Socket Connection Broken")
            else: 
                raise Exception('Connection lost to '+addr[0])
    except Exception: 
        sys.exit(1)
    finally:
        conn.close()




if __name__ == '__main__':
    try:
        filename = 'data.pickle'
        ss = serverSocket()
        ss.bind()
        ss.listen()
        lock = ReadWriteLock()
        counter = 0
        while True:
            conn, addr = ss.accept_conn()
            counter += 1
            conn.setblocking(False)
            conn.settimeout(0.5)
            # Separate thread for each client
            t = threading.Thread(target = client_thread, name = addr[0], args = (conn, addr, lock, filename, ss.BUFFSIZE))
            t.start()
        conn.close()
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()
    except Exception:
        sys.exit(1)
    finally:
        del di, ss
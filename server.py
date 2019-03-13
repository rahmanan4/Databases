import socket
import sys
import threading
import time
from random import randint


class Server:
    # local address
    my_address = "127.0.0.1"
    # communicate with server on port --
    my_port = ""
    # number of data items in a particular domain
    my_mem_thresh = 100
    # store of memory addresses
    mem_info = {}
    # tcp for hearing for connections
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # tcp for initiating new connections
    csock = []
    # list of connections accepted
    connections = []
    # temporary store of data
    pseudo_database = {}
    # stores address and ports of nodes to be connected to
    address_list = []
    port_list = []
    config_flag = 0

    def __init__(self, my_port, isConnect, args=None):
        try:
            # parse through sys.argv[2] and extract
            # (address, port) of the nodes to connect this node to
            if isConnect:
                self.address_list = [args[i] for i in range(0, len(args), 2)]
                self.port_list = [int(args[i]) for i in range(1, len(args), 2)]
            # set up this node at  my_address='0.0.0.0'
            # set up this node to LISTEN at my_port
            self.my_port = int(my_port)
            # if stand-alone node then make mem_thresh = 100
            # else set to -1 for now. Will be updated when connected to other nodes
            if isConnect:
                self.mem_info[(self.my_address, self.my_port)] = -1
            else:
                self.mem_info[(self.my_address, self.my_port)] = 100
            self.sock.bind((self.my_address, self.my_port))
            print("Bound!")
            self.sock.listen(5)
        except socket.error as err:
            print(str(err))

    def get_connection(self, addr):
        for conn in self.connections:
            if conn.getpeername() is addr:
                return conn

    def set_my_mem_thresh(self, network_max):
        self.my_mem_thresh = network_max + 100
        self.mem_info[(self.my_address, self.my_port)] = self.my_mem_thresh

    def get_mem_info_max(self):
        max_thresh = 0
        for key in self.mem_info:
            if self.mem_info[key] > max_thresh:
                max_thresh = self.mem_info[key]
        return max_thresh

    # write value at key of this server
    def write(self, key, value):
        # check key is in this node
            if key <= self.my_mem_thresh and key > self.my_mem_thresh - 100:
                self.pseudo_database[key] = value
                return True
            else:
            # the key is not in this node. Look for appropriate node and send it there
                for addr in self.mem_info:
                    if self.mem_info[addr] > key and key > self.mem_info[addr] - 100:
                    # found the node where key is located
                        for connection in self.connections:
                        # look for the socket to that node
                            if connection.getpeername() == addr:
                            # send the op
                                connection.send(str.encode('w, ' + str(key) + ', ' + str(value)))
                            # return None when operation is sent to other node.
                                return None
                            if connection.getsockname() == addr:
                            # send the op
                                connection.send(str.encode('w, ' + str(key) + ', ' + str(value)))
                            # return None when operation is sent to other node
                                return None
                return False

    # validate and read value at key in this server
    # if key is valid return value at key else
    # return False
    def read(self, key, sender_mem):
        if key <= self.my_mem_thresh and key > self.my_mem_thresh - 100:
            value = str(self.pseudo_database[key])
            for addr in self.mem_info:
                if self.mem_info[addr] == sender_mem:
                    for connection in self.connections:
                        if connection.getpeername() == addr:
                            connection.send(str.encode(value))
                            return True
            return value

        else:
            # the key is not in this node. Look for appropriate node and send it there
            for addr in self.mem_info:
                if self.mem_info[addr] > key and key > self.mem_info[addr] - 100:
                    # found the node where key is located
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            connection.send(str.encode('r, ' + str(key) + ', ' + str(sender_mem)))
                            # return None when operation is sent to other node.
                            return None
                        if connection.getsockname() == addr:
                            # send the op
                            connection.send(str.encode('r, ' + str(key) + ', ' + str(sender_mem)))
                            # return None when operation is sent to other node
                            return None
            return False

    # check if key is valid -
    # if some data exists at key return True
    # if database empty or key not in database, return False
    def read_validation(self, key):
        if len(self.pseudo_database) == 0:
            print("Database is empty, please write first!")
            return False
        elif self.pseudo_database.get(key) is None:
            print("Key:", key, " is not in database!")
            return False
        return True

    # locking mechanism component that acquires and releases locks before performing operations
    def read_locker(self, key, sender_mem, lock):
        not_done = True
        while not_done:
            lock.acquire()
            try:
                self.read(int(key), sender_mem)
            finally:
                lock.release()
                not_done = False
        return
    def write_locker(self, key, value, lock):
        not_done = True
        while not_done:
            lock.acquire()
            try:
                self.write(int(key), value)
            finally:
                lock.release()
                not_done = False
        return

    # initiate number of locks that corresponds to the number of operations that will be performed
    def initiate_locks(self, num_reads):
        key_lock_list = []
        for i in range(num_reads):
            key_lock = threading.Lock()
            key_lock_list.append(key_lock)
        return key_lock_list

    # throughput tests for no locks, serial execution, and concurrent execution
    def rtsthroughput(self, sender_mem, seconds):
        ops = 0
        elapsed = 0
        lock_test = threading.Lock()
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.read_locker, args=(key, sender_mem, lock_test))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    def rtcthroughput(self, sender_mem, seconds):
        ops = 0
        elapsed = 0
        locks = self.initiate_locks(1000)
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            lock = locks[key]
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.read_locker, args=(key, sender_mem, lock))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    def rnlthroughput(self, sender_mem, seconds):
        ops = 0
        elapsed = 0
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.read, args=(key, sender_mem))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    def wtsthroughput(self, seconds):
        ops = 0
        elapsed = 0
        val = 10
        lock_test = threading.Lock()
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.write_locker, args=(key, val, lock_test))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    def wtcthroughput(self, seconds):
        ops = 0
        elapsed = 0
        val = 10
        locks = self.initiate_locks(1000)
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            lock = locks[key]
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.write_locker, args=(key, val, lock))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    def wnlthroughput(self, seconds):
        ops = 0
        elapsed = 0
        val = 10
        start = time.time()
        time.clock()
        while elapsed < seconds:
            key = randint(0, 399)
            elapsed = time.time() - start
            readtestThread = threading.Thread(target=self.write, args=(key, val))
            readtestThread.daemon = True
            readtestThread.start()
            ops += 1
        return ops

    # latency tests for no locks, serial execution, and concurrent execution
    def readtestserial(self, sender_mem):
        read_key_list = []
        num_reads = 10000
        for i in range(num_reads):
            value = randint(0, 399)
            read_key_list.append(value)
        start = time.time()
        lock_test = threading.Lock()
        for key in read_key_list:
            readtestThread = threading.Thread(target=self.read_locker, args=(int(key), sender_mem, lock_test))
            readtestThread.daemon = True
            readtestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def readtestconcurrent(self, sender_mem):
        read_key_list = []
        num_reads = 10000
        for i in range(num_reads):
            value = randint(0, 399)
            read_key_list.append(value)
        locks = self.initiate_locks(num_reads)
        start = time.time()
        for key in read_key_list:
            lock = locks[key]
            readtestThread = threading.Thread(target=self.read_locker, args=(int(key), sender_mem, lock))
            readtestThread.daemon = True
            readtestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def readtestnolock(self, sender_mem):
        read_key_list = []
        num_reads = 10000
        for i in range(num_reads):
            value = randint(0, 399)
            read_key_list.append(value)
        start = time.time()
        for key in read_key_list:
            readtestThread = threading.Thread(target=self.read, args=(int(key), sender_mem))
            readtestThread.daemon = True
            readtestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def writetestserial(self):
        write_key_list = []
        num_writes = 10000
        for i in range(num_writes):
            val = randint(0, 399)
            write_key_list.append(val)
        start = time.time()
        lock_test = threading.Lock()
        for key in write_key_list:
            value = randint(0, 100)
            writetestThread = threading.Thread(target=self.write_locker, args=(int(key), value, lock_test))
            writetestThread.daemon = True
            writetestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def writetestconcurrent(self):
        write_key_list = []
        num_writes = 10000
        for i in range(num_writes):
            value = randint(0, 399)
            write_key_list.append(value)
        locks = self.initiate_locks(num_writes)
        start = time.time()
        for key in write_key_list:
            lock = locks[key]
            value = randint(0, 100)
            writetestThread = threading.Thread(target=self.write_locker, args=(int(key), value, lock))
            writetestThread.daemon = True
            writetestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def writetestnolock(self):
        write_key_list = []
        num_writes = 10000
        for i in range(num_writes):
            value = randint(0, 399)
            write_key_list.append(value)
        start = time.time()
        for key in write_key_list:
            value = randint(0, 100)
            writetestThread = threading.Thread(target=self.write, args=(int(key), value))
            writetestThread.daemon = True
            writetestThread.start()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    def writeforread(self):
        lock_test = threading.Lock()
        write_key_list = []
        num_writes = 10000
        for i in range(num_writes):
            write_key_list.append(i)
        start = time.time()
        for key in write_key_list:
            try:
                acquired = lock_test.acquire()
                if acquired:
                    value = randint(0, 100)
                    writetestThread = threading.Thread(target=self.write, args=(int(key), value))
                    writetestThread.daemon = True
                    writetestThread.start()
            finally:
                if acquired:
                    lock_test.release()
        end = time.time()
        time_took = end - start
        time.sleep(0.5)
        return time_took

    # manages operations
    # dec_data is a list of all words in the command
    def operations(self, dec_data):
        # dec_data is an array of strings which was tokenized using ','
        # strip white-space
        op = dec_data[0].strip()

        # check operation and format: <w,key,value> or <r, key> or rt = readtest, wt = writetest, wfr = write
        # values for readtest
        if op == 'w' and len(dec_data) == 3:
            ret_val = self.write(int(dec_data[1].strip()), int(dec_data[2].strip()))
        elif op == 'r':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            if len(dec_data) == 3:
                if self.mem_info[(self.my_address, self.my_port)] != int(dec_data[2].strip()):
                    sender_mem = int(dec_data[2].strip())
            ret_val = self.read(int(dec_data[1].strip()), sender_mem)

        # read throughput test with no locks
        elif op == 'rnlthru':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.rnlthroughput(sender_mem, 1)
        # read throughput test with serial execution
        elif op == 'rtsthru':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.rtsthroughput(sender_mem, 1)
        # read throughput test with concurrent execution
        elif op == 'rtcthru':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.rtcthroughput(sender_mem, 1)

        # write throughput test with no locks
        elif op == 'wnlthru':
            ret_val = self.wnlthroughput(1)
        # write throughput test with serial execution
        elif op == 'wtsthru':
            ret_val = self.wtsthroughput(1)
        # write throughput test with concurrent execution
        elif op == 'wtcthru':
            ret_val = self.wtcthroughput(1)

        # read test with only one lock, serial execution of threads
        elif op == 'rts':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.readtestserial(sender_mem)
        # read test with lock per key, concurrent execution of threads
        elif op == 'rtc':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.readtestconcurrent(sender_mem)
        # read test with no locks implemented at all
        elif op == 'rnl':
            sender_mem = self.mem_info[(self.my_address, self.my_port)]
            ret_val = self.readtestnolock(sender_mem)

        # write test with only one lock, serial execution of threads
        elif op == 'wts':
            ret_val = self.writetestserial()
        # write test with lock per key, concurrent execution of threads
        elif op == 'wtc':
            ret_val = self.writetestconcurrent()
        # write test with no locks implemented at all
        elif op == 'wnl':
            ret_val = self.writetestnolock()
        # write test that fills in all dict values 0-299, used before readtests
        elif op == 'wfr':
            ret_val = self.writeforread()

        else:
            # return this if unrecognizable operation
            return False

        return ret_val

    # send Msg to other nodes and print its own msg
    # if Msg is a valid operation, perform it
    def sendMsg(self):
        while True:
            raw_msg = input("")
            print("Self:", raw_msg)
            dec_data = raw_msg.split(',')
            # strip white-space
            ret_val = self.operations(dec_data)
            print("Self: ret_val", ret_val)
            msg = raw_msg.encode('utf-8')

    # receive msg from other nodes
    # check if msg is an operation, if so perform it
    # if not just display it
    # conn != None means its a server socket
    # sock != None means its a client socket
    def recvMsg(self, conn=None, address=None, sock=None):
        # recv msg from other nodes.
        while True:
            if conn is None:
                data = sock.recv(1024)
            else:
                data = conn.recv(1024)
            if not data:
                break
            dec_data = data.decode('utf-8')
            print("DEC_DATA", dec_data)
            if dec_data.find("Welcome") != -1:
                dec_data = dec_data.split(':')
                self.mem_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[3].strip())

                # update my_mem_thresh only once
                if self.config_flag == 0:
                    self.config_flag = 1
                    self.set_my_mem_thresh(int(dec_data[4].strip()))

                print("AFTER:", self.mem_info, self.my_mem_thresh)

                if conn is None:
                    # send socket's name and not self.my_address
                    # when conn is None, it is a client socket
                    socket_info = sock.getsockname()
                    # print("SOCKET INFO", socket_info)
                    sock.send(str.encode("OK;" + socket_info[0] + ";" + \
                                         str(socket_info[1]) + ";" + str(self.my_mem_thresh)))
                else:
                    # conn is not None. So it is a server socket.
                    con_info = conn.getsockname()
                    conn.send(str.encode("OK;" + self.my_address + ";" + str(self.my_port) + \
                                         ";" + str(self.my_mem_thresh)))

            elif dec_data.find("OK") != -1:
                dec_data = dec_data.split(';')
                self.mem_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[3].strip())
                print("XXX", self.mem_info, self.my_mem_thresh)
            elif dec_data.find(',') == -1:
                # if no comma then it not an instruction
                print(str(data, 'utf-8').strip())
            else:
                # it has comma, so assume it is an instruction
                dec_data = dec_data.split(',')
                ret_val = self.operations(dec_data)
                print(ret_val)

    # run
    def run(self):
        if len(self.address_list) != 0:
            # this server has to be connected to (self.address, self.port)
            for address, port in zip(self.address_list, self.port_list):
                newsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                newsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.csock.append(newsock)
                newsock.connect((address, port))
                # add newsoc to connections
                self.connections.append(newsock)
                print("Connected to: ", address, ":", port)
                # Successfully connected. Now thread to receive data from (address, port)
                # This is a client socket so send only socket to appropriate informal parameter
                rThread = threading.Thread(target=self.recvMsg, args=(None, None, newsock,))
                rThread.daemon = True
                rThread.start()

        # thread to send data to every node in connections[]
        iThread = threading.Thread(target=self.sendMsg)
        iThread.daemon = True
        iThread.start()

        # listen for incoming connections to this node
        while True:
            conn, address = self.sock.accept()
            print(conn)
            print('Accepted connection from ', address, ' successfully!')
            conn.send(str.encode('Welcome:' + self.my_address + ":" + str(self.my_port) + ":" \
                                 + str(self.my_mem_thresh) + ":" + str(self.get_mem_info_max())))
            cThread = threading.Thread(target=self.handler, args=(conn, address))
            cThread.daemon = True
            cThread.start()
            self.connections.append(conn)

    # handler
    def handler(self, conn, address):
        reply = ""
        self.recvMsg(conn, address)
        # welcome = 'Connection successful. Welcome! \n <w, key, value> OR <r, key>'
        # conn.send(welcome.encode('utf-8'))


if len(sys.argv) == 2:
    server = Server(sys.argv[1], False)
    server.run()
else:
    server = Server(sys.argv[1], True, sys.argv[2:])
    server.run()

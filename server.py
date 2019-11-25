import socket
import sys
import threading
import time
import random
import tpc_c
from datetime import date


class Server:
    # local address
    my_address = "127.0.0.1"

    # communicate with server on port --
    my_port = ""

    # number of data items in a particular domain
    my_mem_thresh = 100
    my_warehouses_thresh = 1
    my_districts_thresh = 10
    my_customers_thresh = 3000

    # store of memory addresses
    mem_info = {}
    # store of what districts belong to this node
    warehouse_info = {}
    district_info = {}
    customer_info = {}

    # tcp for hearing for connections
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # tcp for initiating new connections
    csock = []

    # list of connections accepted
    connections = []

    # store of data
    numOfWarehouses = 1
    warehouseTable = tpc_c.createWarehouseTable(numOfWarehouses)
    districtTable = tpc_c.createDistrictTable(numOfWarehouses)
    customerTable = tpc_c.createCustomerTable(numOfWarehouses)
    historyTable = tpc_c.createHistoryTable()
    newOrderTable = tpc_c.createNewOrderTable()
    orderTable = tpc_c.createOrderTable()
    orderLineTable = tpc_c.createOrderLineTable()
    itemTable = tpc_c.createItemTable()
    stockTable = tpc_c.createStockTable(numOfWarehouses)

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
                self.warehouse_info[(self.my_address, self.my_port)] = -1
                self.district_info[(self.my_address, self.my_port)] = -1
                self.customer_info[(self.my_address, self.my_port)] = -1
            else:
                self.mem_info[(self.my_address, self.my_port)] = 100
                self.warehouse_info[(self.my_address, self.my_port)] = 1
                self.district_info[(self.my_address, self.my_port)] = 10
                self.customer_info[(self.my_address, self.my_port)] = 3000
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

    def set_my_warehouse_thresh(self, network_max):
        self.my_warehouses_thresh = network_max + 1
        self.warehouse_info[(self.my_address, self.my_port)] = self.my_warehouses_thresh

    def get_warehouse_info_max(self):
        max_warehouse = 0
        for key in self.warehouse_info:
            if self.warehouse_info[key] > max_warehouse:
                max_warehouse = self.warehouse_info[key]
        return max_warehouse

    def set_my_district_thresh(self, network_max):
        self.my_districts_thresh = network_max + 10
        self.district_info[(self.my_address, self.my_port)] = self.my_districts_thresh

    def get_district_info_max(self):
        max_district = 0
        for key in self.district_info:
            if self.district_info[key] > max_district:
                max_district = self.district_info[key]
        return max_district

    def set_my_customer_thresh(self, network_max):
        self.my_customers_thresh = network_max + 3000
        self.customer_info[(self.my_address, self.my_port)] = self.my_customers_thresh

    def get_customer_info_max(self):
        max_customer = 0
        for key in self.customer_info:
            if self.customer_info[key] > max_customer:
                max_customer = self.customer_info[key]
        return max_customer

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
            key = random.randint(0, 399)
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
            key = random.randint(0, 399)
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
            key = random.randint(0, 399)
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
            key = random.randint(0, 399)
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
            key = random.randint(0, 399)
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
            key = random.randint(0, 399)
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
            value = random.randint(0, 399)
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
            value = random.randint(0, 399)
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
            value = random.randint(0, 399)
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
            val = random.randint(0, 399)
            write_key_list.append(val)
        start = time.time()
        lock_test = threading.Lock()
        for key in write_key_list:
            value = random.randint(0, 100)
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
            value = random.randint(0, 399)
            write_key_list.append(value)
        locks = self.initiate_locks(num_writes)
        start = time.time()
        for key in write_key_list:
            lock = locks[key]
            value = random.randint(0, 100)
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
            value = random.randint(0, 399)
            write_key_list.append(value)
        start = time.time()
        for key in write_key_list:
            value = random.randint(0, 100)
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
                    value = random.randint(0, 100)
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

    # run
    def run(self):
        # If the address_list is empty, this means there is only one node, if there is more, then you have to connect
        # other servers
        if len(self.address_list) != 0:
            # this server has to be connected to (self.address, self.port)
            for address, port in zip(self.address_list, self.port_list):
                # for every address and port (parallel lists), create a new sock get the new node connected to the others
                newsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                newsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.csock.append(newsock)
                newsock.connect((address, port))
                # add newsoc to this nodes connections list
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
            # accepts connection between client (other server node)
            conn, address = self.sock.accept()
            print(conn)
            print('Accepted connection from ', address, ' successfully!')
            # send the new connection the following message
            conn.send(str.encode('Welcome:' + self.my_address + ":" + str(self.my_port) + ":" \
                                 + str(self.my_mem_thresh) + ":" + str(self.get_mem_info_max()) + ":" +
                                 str(self.get_district_info_max()) + ":" + str(self.my_districts_thresh) + ":" +
                                 str(self.get_warehouse_info_max()) + ":" + str(self.my_warehouses_thresh) + ":" +
                                 str(self.get_customer_info_max()) + ":" + str(self.my_customers_thresh)))

            # send to handler, which sends to recvMsg, if there is no data inside, then break from recvMsg which goes
            # back to handler which completes and goes back to here and continues doing the while loop over and over.
            # this is what allows the servers to constantly be looking and trying to see if the node itself is sending
            # an instruction for itself (sock) or if it is receiving an instruction from another server node (conn)
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
                # put self.my_mem_thresh into mem_info with key (address, port)
                self.mem_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[3].strip())
                self.warehouse_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[8].strip())
                self.district_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[6].strip())
                self.customer_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[10].strip())

                # update my_mem_thresh only once
                if self.config_flag == 0:
                    self.config_flag = 1
                    # set new connections mem thresh to the new max
                    self.set_my_mem_thresh(int(dec_data[4].strip()))
                    self.set_my_warehouse_thresh(int(dec_data[7].strip()))
                    self.set_my_district_thresh(int(dec_data[5].strip()))
                    self.set_my_customer_thresh(int(dec_data[9].strip()))

                print("AFTER:", self.mem_info, self.my_mem_thresh, self.warehouse_info, self.my_warehouses_thresh,
                      self.district_info, self.my_districts_thresh, self.customer_info, self.district_info)

                if conn is None:
                    # send socket's name and not self.my_address
                    # when conn is None, it is a client socket
                    socket_info = sock.getsockname()
                    # print("SOCKET INFO", socket_info)
                    sock.send(str.encode("OK;" + socket_info[0] + ";" + \
                                         str(socket_info[1]) + ";" + str(self.my_mem_thresh) + ";" +
                                         str(self.my_districts_thresh) + ";" + str(self.my_warehouses_thresh) + ";" +
                                         str(self.my_customers_thresh)))
                else:
                    # conn is not None. So it is a server socket.
                    con_info = conn.getsockname()
                    conn.send(str.encode("OK;" + self.my_address + ";" + str(self.my_port) + \
                                         ";" + str(self.my_mem_thresh) + ";" + str(self.my_districts_thresh) + ";" +
                                         str(self.my_warehouses_thresh) + ";" + str(self.my_customers_thresh)))

            elif dec_data.find("OK") != -1:
                dec_data = dec_data.split(';')
                self.mem_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[3].strip())
                self.warehouse_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[5].strip())
                self.district_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[4].strip())
                self.customer_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[6].strip())

                print("XXX", self.mem_info, self.my_mem_thresh, self.warehouse_info, self.my_warehouses_thresh,
                      self.district_info, self.my_districts_thresh, self.customer_info, self.my_customers_thresh)
            elif dec_data.find(',') == -1:
                # if no comma then it not an instruction for pseudo_database
                print(str(data, 'utf-8').strip())
            else:
                # it has comma, so assume it is an instruction, still works for tpc-c because no commas in it
                dec_data = dec_data.split(',')
                ret_val = self.operations(dec_data)
                print(ret_val)

    # send Msg to other nodes and print its own msg
    # if Msg is a valid operation, perform it
    def sendMsg(self):
        while True:
            raw_msg = input("")
            print("Self:", raw_msg)
            # this is only required if using pseudo_database dict, not required for TPC
            dec_data = raw_msg.split(',')
            # strip white-space
            ret_val = self.operations(dec_data)
            print("Self: ret_val", ret_val)
            msg = raw_msg.encode('utf-8')

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
        # TPC-C test that returns tpmC business throughput
        elif op == 'tpm':
            if len(self.connections) == 0:
                self.fix_firs_serv_num(30000)
                print('Fixed')
            runs = 1
            secs = 60
            total = 0
            i = 0
            trxns = ['neworder', 'payment', 'orderstatus', 'delivery', 'stocklevel']
            trxns_perf_list = []
            stop_threads = False
            for _ in range(10000):
                j = random.randint(0, (len(trxns)-1))
                trxns_perf_list.append(trxns[j])

            for i in range(runs):
                threads = []
                stop_threads = False
                end = datetime.datetime.now() + datetime.timedelta(seconds=secs)
                while True:
                    if datetime.datetime.now() >= end:
                        break
                    trxn = trxns_perf_list[i]
                    trxn_thread = threading.Thread(target=self.tpc_test, args=(trxn, lambda: stop_threads))
                    threads.append(trxn_thread)
                    trxn_thread.daemon = True
                    trxn_thread.start()
                    if i == (len(trxns_perf_list)-1):
                        i = 0
                    else:
                        i += 1
                print('run done @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                #stop_threads = True
                #for thread in threads:
                #    thread.join()
                trxn_count = self.trxn_count
                self.trxn_count = 0
                total += trxn_count
            print('tpm done @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            avg = total / runs
            time.sleep(5)
            return 'avg transactions completed in ' + str(secs) + ' secs: ' + str(avg)
        # tpc_c new order transaction profile, returns time taken to perform this
        elif op == 'neworder':
            W_ID = 1
            D_ID = random.randint(1, 10)
            C_ID = tpc_c.NURand(1023, 1, 3000)
            ol_cnt = random.randint(5, 15)
            rbk = random.randint(1, 100)
            I_IDS = []
            I_W_IDS = []
            I_QTYS = []
            for item in range(ol_cnt):
                if item == ol_cnt - 1 and rbk == 1:
                    OL_I_ID = 3000000
                else:
                    OL_I_ID = tpc_c.NURand(8191, 1, 100000)
                I_IDS.append(OL_I_ID)
                x = random.randint(1, 100)
                if x > 1:
                    OL_SUPPLY_W_ID = W_ID
                if x == 1:
                    list_W_IDS = []
                    for i in range(1, self.numOfWarehouses + 1):
                        list_W_IDS.append(i)
                    list_W_IDS.remove(W_ID)
                    OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS)-1))]
                I_W_IDS.append(OL_SUPPLY_W_ID)
                OL_QUANTITY = random.randint(1, 10)
                I_QTYS.append(OL_QUANTITY)
            O_ENTRY_D = date.today().strftime("%d/%m/%Y")
            start = time.process_time()
            tpc_c.newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                                      self.warehouseTable, self.districtTable, self.customerTable, self.itemTable,
                                      self.newOrderTable, self.orderLineTable, self.orderTable, self.stockTable)
            end = time.process_time()
            ret_val = end - start
            return ret_val
        # tpc_c payment transaction profile, returns time taken to perform this
        elif op == 'payment':
            W_ID = 1
            D_W_ID = W_ID
            D_ID = random.randint(1, 10)
            x = random.randint(1, 100)
            y = random.randint(1, 100)
            if x <= 85:
                C_D_ID = D_ID
                C_W_ID = W_ID
            else:
                C_D_ID = random.randint(1, 10)
                C_W_ID = random.randint(1, self.numOfWarehouses)
            if y <= 60:
                lastNameList = ['Robinson', 'Nguyen', 'Juarez']
                C_LAST = lastNameList[tpc_c.NURand(255, 0, 999) % len(lastNameList)]
                C_ID = None
            else:
                C_LAST = None
                C_ID = tpc_c.NURand(1023, 1, 3000)
            H_AMOUNT = round(random.uniform(1, 5000000), 2)
            H_DATE = date.today().strftime("%d/%m/%Y")
            start = time.process_time()
            tpc_c.paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE,
                                     self.warehouseTable, self.districtTable, self.customerTable, self.historyTable)
            end = time.process_time()
            ret_val = end - start
            return ret_val
        # tpc_c order status transaction profile, returns time taken to perform this
        elif op == 'orderstatus':
            W_ID = 1
            D_ID = random.randint(1, 10)
            y = random.randint(1, 100)
            if y <= 60:
                lastNameList = ['Robinson', 'Nguyen', 'Juarez']
                C_LAST = lastNameList[tpc_c.NURand(255, 0, 999) % len(lastNameList)]
                C_ID = None
            else:
                C_LAST = None
                C_ID = tpc_c.NURand(1023, 1, 3000)
            start = time.process_time()
            tpc_c.orderStatusTransaction(W_ID, D_ID, C_ID, C_LAST, self.customerTable, self.orderTable, self.orderLineTable)
            end = time.process_time()
            ret_val = end - start
            return ret_val
        # tpc_c delivery transaction profile, returns time taken to perform this
        elif op == 'delivery':
            W_ID = 1
            O_CARRIER_ID = random.randint(1, 10)
            OL_DELIVERY_D = date.today().strftime("%d/%m/%Y")
            start = time.process_time()
            result = tpc_c.deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, self.customerTable,
                                               self.newOrderTable, self.orderTable, self.orderLineTable)
            end = time.process_time()
            ret_val = end - start
            return ret_val
        # tpc_c stock level transaction profile, returns time taken to perform this
        elif op == 'stocklevel':
            W_ID = 1
            D_ID = 1
            threshold = random.randint(10, 20)
            start = time.process_time()
            result = tpc_c.stockLevelTransaction(W_ID, D_ID, threshold, self.districtTable, self.orderLineTable, self.stockTable)
            end = time.process_time()
            ret_val = end - start
            return ret_val
        elif op == 'mem_max':
            return self.get_mem_info_max()
        elif op == 'war_max':
            return self.get_warehouse_info_max()
        elif op == 'dist_max':
            return self.get_district_info_max()
        elif op == 'cust_max':
            return self.get_customer_info_max()
        else:
            # return this if unrecognizable operation
            return False


# initial server is establishing itself, giving the port it will use. It's address is default
if len(sys.argv) == 2:
    server = Server(sys.argv[1], False)
    server.run()
else:
    server = Server(sys.argv[1], True, sys.argv[2:])
    server.run()

import socket
import sys
import threading
import time
import random
import math
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
    my_customers_thresh = 30000

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

    # misc stores so that benchmark can get values it needs from other servers. initialize them to none so that
    # benchmark must wait until it receives the values
    misc = {'W_TAX': None, 'D_TAX': None, 'D_NEXT_O_ID': None, 'C_DISCOUNT': None, 'C_LAST': None, 'C_CREDIT': None,
            'S_QUANTITY': None, 'S_YTD': None, 'S_ORDER_CNT': None, 'S_REMOTE_CNT': None, 'S_DATA': None,
            'S_DIST_01': None, 'S_DIST_02': None, 'S_DIST_03': None, 'S_DIST_04': None, 'S_DIST_05': None,
            'S_DIST_06': None, 'S_DIST_07': None, 'S_DIST_08': None, 'S_DIST_09': None, 'S_DIST_10': None,
            'S_DIST_xx': None}

    # stores address and ports of nodes to be connected to
    address_list = []
    port_list = []
    config_flag = 0
    pad_len = 1024

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
                self.customer_info[(self.my_address, self.my_port)] = 30000
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
        self.warehouseTable[0]['W_ID'] = int(self.my_warehouses_thresh)
        self.warehouseTable[0]['W_NAME'] = ('Warehouse ' + str(self.my_warehouses_thresh))
        for i in range(0, 100000):
            self.stockTable[i]['S_W_ID'] = int(self.my_warehouses_thresh)

    def get_warehouse_info_max(self):
        max_warehouse = 0
        for key in self.warehouse_info:
            if self.warehouse_info[key] > max_warehouse:
                max_warehouse = self.warehouse_info[key]
        return max_warehouse

    def set_my_district_thresh(self, network_max):
        self.my_districts_thresh = network_max + 10
        self.district_info[(self.my_address, self.my_port)] = self.my_districts_thresh
        if network_max >= 10:
            for i in range(1, 11):
                self.districtTable[i-1]['D_ID'] = ((self.my_warehouses_thresh-1)*10) + i
                self.districtTable[i-1]['D_W_ID'] = int(self.my_warehouses_thresh)
                self.districtTable[i-1]['D_NAME'] = ('District ' + str(((self.my_warehouses_thresh-1)*10) + i))

    def get_district_info_max(self):
        max_district = 0
        for key in self.district_info:
            if self.district_info[key] > max_district:
                max_district = self.district_info[key]
        return max_district

    def set_my_customer_thresh(self, network_max):
        self.my_customers_thresh = network_max + 30000
        self.customer_info[(self.my_address, self.my_port)] = self.my_customers_thresh
        j = 1
        k = 1
        if network_max >= 30000:
            for i in range(1, 30001):
                self.customerTable[i-1]['C_ID'] = ((self.my_warehouses_thresh-1)*30000) + i
                self.customerTable[i-1]['C_W_ID'] = int(self.my_warehouses_thresh)
                self.customerTable[i-1]['C_D_ID'] = j + (self.my_districts_thresh-10)
                k += 1
                if k == 3001:
                    k = 1
                    j += 1

    def fix_firs_serv_num(self, network_max):
        j = 1
        k = 1
        if network_max >= 30000:
            for i in range(1, 30001):
                self.customerTable[i-1]['C_ID'] = ((self.my_warehouses_thresh-1)*30000) + i
                self.customerTable[i-1]['C_W_ID'] = int(self.my_warehouses_thresh)
                self.customerTable[i-1]['C_D_ID'] = j + (self.my_districts_thresh-10)
                k += 1
                if k == 3001:
                    k = 1
                    j += 1

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

    def get_W_info(self, warehouseTable, W_ID, sender_mem):
        for warehouse in warehouseTable:
            if W_ID == warehouse['W_ID']:
                W_TAX = warehouse['W_TAX']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg('storeinfo,' + 'W_TAX,' + str(W_TAX) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_D_info(self, districtTable, D_ID, D_W_ID, sender_mem):
        for district in districtTable:
            if D_ID == district['D_ID'] and D_W_ID == district['D_W_ID']:
                D_TAX = district['D_TAX']
                D_NEXT_O_ID = district['D_NEXT_O_ID']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg('storeinfo,' + 'D_TAX,' + str(D_TAX) + ', ' + 'D_NEXT_O_ID,' + str(D_NEXT_O_ID) + ',')
                        connection.send(str.encode(op))
                        return True

    def get_C_info(self, customerTable, C_ID, C_D_ID, C_W_ID, sender_mem):
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_DISCOUNT = customer['C_DISCOUNT']
                C_LAST = customer['C_LAST']
                C_CREDIT = customer['C_CREDIT']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg('storeinfo,' + 'C_DISCOUNT,' + str(C_DISCOUNT) + ', ' + 'C_LAST,' + str(C_LAST)
                                          + ', ' + 'C_CREDIT,' + str(C_CREDIT) + ',')
                        connection.send(str.encode(op))

                        return True

    def get_S_info(self, stockTable, OL_I_ID, OL_SUPPLY_W_ID, D_ID, sender_mem):
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                S_QUANTITY = stock['S_QUANTITY']
                S_YTD = stock['S_YTD']
                S_ORDER_CNT = stock['S_ORDER_CNT']
                S_REMOTE_CNT = stock['S_REMOTE_CNT']
                S_DATA = stock['S_DATA']
                if D_ID > 10:
                    xx = D_ID - ((self.my_warehouses_thresh * 10)-10)
                    S_DIST_xx = stock['S_DIST_' + '{0:0=2d}'.format(xx)]
                else:
                    S_DIST_xx = stock['S_DIST_' + '{0:0=2d}'.format(D_ID)]
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg('storeinfo,' + 'S_QUANTITY,' + str(S_QUANTITY) + ' , ' + 'S_YTD,' +
                                                   str(S_YTD) + ', ' + 'S_ORDER_CNT,' + str(S_ORDER_CNT) + ', ' +
                                                   'S_REMOTE_CNT,' + str(S_REMOTE_CNT) + ', ' + 'S_DATA,' + str(S_DATA)
                                                   + ', ' + 'S_DIST_xx,' + str(S_DIST_xx) + ', ')
                        connection.send(str.encode(op))

                        return True

    def update_S_info(self, stockTable, OL_I_ID, OL_SUPPLY_W_ID, S_QUANTITY, S_YTD, S_ORDER_CNT,
                      S_REMOTE_CNT):
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                stock['S_QUANTITY'] = S_QUANTITY
                stock['S_YTD'] = S_YTD
                stock['S_ORDER_CNT'] = S_ORDER_CNT
                stock['S_REMOTE_CNT'] = S_REMOTE_CNT
                break
        return 'S_info has been updated'

    def pad_msg(self, og_msg):
        leng = len(og_msg.encode("utf-8"))
        if leng != self.pad_len:
            pad_amt = self.pad_len - leng
            add_amt = pad_amt * '-'
            edit_msg = og_msg + add_amt
        return edit_msg

    def newOrderTransaction(self, W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                            warehouseTable, districtTable, customerTable, itemTable, newOrderTable, orderLineTable,
                            orderTable, stockTable, sender_mem):
        stockTableInitial = stockTable.copy()
        # ------------------------------------------------------------------------------------------------------------------
        # Getting information from wareHouseTable, districtTable, customerTable
        # ------------------------------------------------------------------------------------------------------------------
        for warehouse in warehouseTable:
            if W_ID == warehouse['W_ID']:
                W_TAX = warehouse['W_TAX']
                self.misc['W_TAX'] = W_TAX
                break
            # if more than one node, and the warehouse isn't the right node, must search for proper W_ID.
            # this is implementation specific, other programs running the distributed version must replace this logic.
            else:
                for addr in self.warehouse_info:
                    if self.warehouse_info[addr] >= W_ID > self.warehouse_info[addr] - 1:
                        # found the node where warehouse is
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('get_W_info,' + str(W_ID) + ' , ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('get_W_info,' + str(W_ID) + ' , ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                # until the other server actually has given this server the information it needs, to random task of setting
                # x = 5
                while self.misc['W_TAX'] is None:
                    x = 5
                W_TAX = self.misc['W_TAX']
                # make sure that W_TAX actually got changed to a value that it got
                assert W_TAX != None
        #print('W_info done')
        #print()

        for district in districtTable:
            found_in_own_district = False
            if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
                D_TAX = district['D_TAX']
                D_NEXT_O_ID = district['D_NEXT_O_ID']
                district['D_NEXT_O_ID'] += 1
                found_in_own_district = True
                break
        if found_in_own_district is False:
            for addr in self.warehouse_info:
                if self.district_info[addr] >= D_ID > self.district_info[addr] - 10 and \
                        self.warehouse_info[addr] >= D_W_ID > self.warehouse_info[addr] - 1:
                    # found the node where district is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg('get_D_info,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg('get_D_info,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
                # until the other server actually has given this server the information it needs, to random task of setting
                # x = 5
            while self.misc['D_TAX'] is None and self.misc['D_NEXT_O_ID'] is None:
                x = 5
            D_TAX = self.misc['D_TAX']
            D_NEXT_O_ID = self.misc['D_NEXT_O_ID']
            district['D_NEXT_O_ID'] += 1

            # make sure that D_TAX and D_NEXT_O_ID actually got changed to a value that it got
            assert D_TAX != None
            assert D_NEXT_O_ID != None
        #print('d_info done')
        #print()

        customerInfo = []
        for customer in customerTable:
            found_in_own_customer = False
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_DISCOUNT = customer['C_DISCOUNT']
                C_LAST = customer['C_LAST']
                C_CREDIT = customer['C_CREDIT']
                customerInfo.append([C_DISCOUNT, C_LAST, C_CREDIT])
                found_in_own_customer = True
                break
        if found_in_own_customer is False:
            for addr in self.warehouse_info:
                if self.customer_info[addr] >= C_ID > self.customer_info[addr] - 30000 and \
                        self.district_info[addr] >= C_D_ID > self.district_info[addr] - 10 and \
                        self.warehouse_info[addr] >= C_W_ID > self.warehouse_info[addr] - 1:
                    # found the node where district is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg('get_C_info,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                              + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg('get_C_info,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                              + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
            # until the other server actually has given this server the information it needs, to random task of setting
            # x = 5
            while self.misc['C_DISCOUNT'] is None and self.misc['C_LAST'] is None and self.misc['C_CREDIT'] is None:
                x = 5
            C_DISCOUNT = self.misc['C_DISCOUNT']
            C_LAST = self.misc['C_LAST']
            C_CREDIT = self.misc['C_CREDIT']
            customerInfo.append([C_DISCOUNT, C_LAST, C_CREDIT])
        assert C_DISCOUNT != None
        assert C_LAST != None
        assert C_CREDIT != None
        #print('c_info done')
        #print()

        # ------------------------------------------------------------------------------------------------------------------
        # Adding the order to the orderTable and newOrderTable
        # ------------------------------------------------------------------------------------------------------------------

        # determines whether all of the items in I_IDS are from the same warehouse where a list of warehouse IDs respective
        #   to an item is compared to the warehouse ID: I_W_IDS = W_ID

        # also adds all item information (item price, item name, item data) to a list of items

        # O_OL_CNT is ol_cnt which is the total number of items in the order
        O_ALL_LOCAL = True
        items = []
        for i in range(len(I_IDS)):
            O_ALL_LOCAL = O_ALL_LOCAL and I_W_IDS[i] == W_ID
            for item in itemTable:
                if I_IDS[i] == item['I_ID']:
                    items.append([item['I_PRICE'], item['I_NAME'], item['I_DATA']])
                    bad_item_num_flag = False
                    break
                else:
                    bad_item_num_flag = True
            if bad_item_num_flag == True:
                items.append([])

        O_CARRIER_ID = None
        O_OL_CNT = len(I_IDS)
        tpc_c.addNewOrder(newOrderTable, D_NEXT_O_ID, D_ID, W_ID)
        tpc_c.addOrder(orderTable, D_NEXT_O_ID, D_ID, W_ID, C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)

        # ------------------------------------------------------------------------------------------------------------------
        # Retrieve information from stockTable, update quantity and order and remote counts
        # ------------------------------------------------------------------------------------------------------------------
        itemInfo = []
        TOTAL_AMOUNT = 0
        for i in range(O_OL_CNT):
            # Fixed 1% of the New-Order Transaction are chosen at random to simulate data errors, determined based off of
            #   rbk input where if rbk is set to 1, then an invalid item value is placed into I_IDS
            if len(items[i]) == 0:
                # recover the old values for stockTable and exit the benchmark with item number is not valid
                for stock, stockI in zip(stockTable, stockTableInitial):
                    stock['S_QUANTITY'] = stockI['S_QUANTITY']
                    stock['S_YTD'] = stockI['S_YTD']
                    stock['S_ORDER_CNT'] = stockI['S_ORDER_CNT']
                    stock['S_REMOTE_CNT'] = stockI['S_REMOTE_CNT']
                return 'Item number is not valid'

            OL_NUMBER = i + 1
            OL_SUPPLY_W_ID = I_W_IDS[i]
            OL_I_ID = I_IDS[i]
            OL_QUANTITY = I_QTYS[i]
            itemInfo = items[i]
            I_PRICE = itemInfo[0]
            I_NAME = itemInfo[1]
            I_DATA = itemInfo[2]
            in_my_stock_table = False
            for stock in stockTable:
                if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                    in_my_stock_table = True
                    S_QUANTITY = stock['S_QUANTITY']
                    S_YTD = stock['S_YTD']
                    S_ORDER_CNT = stock['S_ORDER_CNT']
                    S_REMOTE_CNT = stock['S_REMOTE_CNT']
                    S_DATA = stock['S_DATA']
                    S_DIST_xx = ['S_DIST_' + '{0:0=2d}'.format(D_ID)]
                    break
            if in_my_stock_table is False:
                for addr in self.warehouse_info:
                    if self.warehouse_info[addr] >= OL_SUPPLY_W_ID > self.warehouse_info[addr] - 1:
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('get_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' + str(
                                        D_ID) + ', ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('get_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' + str(
                                        D_ID) + ', ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                while self.misc['S_QUANTITY'] is None and self.misc['S_YTD'] is None and self.misc['S_ORDER_CNT'] \
                        is None and self.misc['S_REMOTE_CNT'] is None and self.misc['S_DATA'] is None and \
                        self.misc['S_DIST_' + '{0:0=2d}'.format(D_ID - ((W_ID * 10)-10))] is None:
                    x = 5
                S_QUANTITY = self.misc['S_QUANTITY']
                S_YTD = self.misc['S_YTD']
                S_ORDER_CNT = self.misc['S_ORDER_CNT']
                S_REMOTE_CNT = self.misc['S_REMOTE_CNT']
                S_DATA = self.misc['S_DATA']
                S_DIST_xx = self.misc['S_DIST_xx']
                #S_DIST_xx = self.misc['S_DIST_' + '{0:0=2d}'.format(D_ID - ((W_ID * 10)-10))]

            assert S_QUANTITY != None
            assert S_YTD != None
            assert S_ORDER_CNT != None
            assert S_REMOTE_CNT != None
            assert S_DATA != None
            assert S_DIST_xx != None
            #print('s_info done')

            if S_QUANTITY >= OL_QUANTITY + 10:
                S_QUANTITY -= OL_QUANTITY
            else:
                S_QUANTITY = (S_QUANTITY - OL_QUANTITY) + 91
            S_YTD += OL_QUANTITY
            S_ORDER_CNT += 1
            if OL_SUPPLY_W_ID != W_ID:
                S_REMOTE_CNT += 1

            in_my_stock_table = False
            # Updating stock table
            for stock in stockTable:
                if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                    in_my_stock_table = True
                    stock['S_QUANTITY'] = S_QUANTITY
                    stock['S_YTD'] = S_YTD
                    stock['S_ORDER_CNT'] = S_ORDER_CNT
                    stock['S_REMOTE_CNT'] = S_REMOTE_CNT
                    break
            if in_my_stock_table is False:
                for addr in self.warehouse_info:
                    if self.warehouse_info[addr] >= OL_SUPPLY_W_ID > self.warehouse_info[addr] - 1:
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('update_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' +
                                    str(S_QUANTITY) + ', ' + str(S_YTD) + ', ' + str(S_ORDER_CNT) + ', ' +
                                    str(S_REMOTE_CNT) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('update_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' +
                                                  str(S_QUANTITY) + ', ' + str(S_YTD) + ', ' + str(S_ORDER_CNT) + ', ' +
                                                  str(S_REMOTE_CNT) + ', ')
                                connection.send(str.encode(op))
                #print('s_info updated')
            OL_AMOUNT = OL_QUANTITY * I_PRICE
            TOTAL_AMOUNT += OL_AMOUNT

            if I_DATA.find("ORIGINAL") != -1 and S_DATA.find("ORIGINAL") != -1:
                BRAND_GENERIC = 'B'
            else:
                BRAND_GENERIC = 'G'

            tpc_c.addOrderLine(orderLineTable, D_NEXT_O_ID, D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, O_ENTRY_D,
                         OL_QUANTITY, OL_AMOUNT, S_DIST_xx)

            itemInfo.append([I_NAME, S_QUANTITY, BRAND_GENERIC, I_PRICE, OL_AMOUNT])

        TOTAL_AMOUNT *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
        otherInfo = [(W_TAX, D_TAX, D_NEXT_O_ID, TOTAL_AMOUNT)]
        return [customerInfo, otherInfo, itemInfo]

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

                #read = newsock.makefile('r')
                #write = newsock.makefile('w')

                # add newsoc to this nodes connections list
                self.connections.append(newsock)
                print("Connected to: ", address, ":", port)
                # Successfully connected. Now thread to receive data from (address, port)
                # This is a client socket so send only socket to appropriate informal parameter
                rThread = threading.Thread(target=self.recvMsg, args=(None, None, newsock,))
                rThread.daemon = True
                rThread.start()

        if len(self.address_list) == 0:
            self.fix_firs_serv_num(self.my_customers_thresh)

        # thread to send data to every node in connections[]
        iThread = threading.Thread(target=self.sendMsg)
        iThread.daemon = True
        iThread.start()

        # listen for incoming connections to this node
        while True:
            # accepts connection between client (other server node)
            conn, address = self.sock.accept()
            #read = conn.makefile('r')
            #write = conn.makefile('w')
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
        #print(read)
        #print(write)
        # recv msg from other nodes.
        while True:
            if conn is None:
                data = sock.recv(self.pad_len)
                #data = sock.recv(1024)
                #sock.close()
                #data = read.readline()
                #read.close()
            else:
                data = conn.recv(self.pad_len)
                #data = conn.recv(1024)
                #conn.close()
                #data = read.readline()
                #read.close()
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
                      self.district_info, self.my_districts_thresh, self.customer_info, self.my_customers_thresh)

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
        print('Operation: ' + str(op))

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
        # tpc_c new order transaction profile, returns time taken to perform this
        elif op == 'neworder':
            sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
            # chooses constant W_ID based off of total number of server nodes
            W_ID = random.randint(1, len(self.warehouse_info))
            #print('beginning W_ID' + str(W_ID))

            # chooses specific D_ID that corresponds to W_ID, e.g. if W_ID = 2, then starting_district and
            # ending_district will correspond to this ID and be in range (11, 20)
            starting_district = ((W_ID*10)-9)
            ending_district = (W_ID*10)
            D_ID = random.randint(starting_district, ending_district)
            #print('beginning D_ID' + str(D_ID))

            # chooses specific C_ID from the NURand function defined in TPC-C spec. C_ID_offset is implementation
            # specific, offsets value produced by NURand it so it corrects C_ID to be respective of node with W_ID
            C_ID_offset = (D_ID-1) * 3000

            C_ID = tpc_c.NURand(1023, 1, 3000) + C_ID_offset
            #print('beginning C_ID' + str(C_ID))

            ol_cnt = random.randint(5, 15)
            rbk = random.randint(1, 100)
            I_IDS = []
            I_W_IDS = []
            I_QTYS = []
            for item in range(ol_cnt):
                if item == (ol_cnt - 1) and rbk == 1:
                    OL_I_ID = 3000000
                else:
                    OL_I_ID = tpc_c.NURand(8191, 1, 100000)
                I_IDS.append(OL_I_ID)
                x = random.randint(1, 100)
                if x > 1:
                    OL_SUPPLY_W_ID = self.my_warehouses_thresh
                if x == 1:
                    list_W_IDS = []
                    for i in range(1, len(self.warehouse_info)+1):
                        list_W_IDS.append(i)
                    list_W_IDS.remove(self.my_warehouses_thresh)
                    OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS)-1))]
                I_W_IDS.append(OL_SUPPLY_W_ID)
                #print(I_W_IDS)
                OL_QUANTITY = random.randint(1, 10)
                I_QTYS.append(OL_QUANTITY)
            O_ENTRY_D = date.today().strftime("%d/%m/%Y")
            start = time.process_time()
            self.newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                                      self.warehouseTable, self.districtTable, self.customerTable, self.itemTable,
                                      self.newOrderTable, self.orderLineTable, self.orderTable, self.stockTable, sender_mem)
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

        elif op == 'storeinfo':
            if dec_data[1].strip() == 'W_TAX':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                return 'W_info has been set'
            if dec_data[1].strip() == 'D_TAX' and dec_data[3].strip() == 'D_NEXT_O_ID':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                self.misc[dec_data[3].strip()] = int(dec_data[4].strip())
                return 'D_info has been set'
            if dec_data[1].strip() == 'C_DISCOUNT' and dec_data[3].strip() == 'C_LAST' and dec_data[5].strip() == 'C_CREDIT':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                self.misc[dec_data[3].strip()] = dec_data[4].strip()
                self.misc[dec_data[5].strip()] = dec_data[6].strip()
                return 'C_info has been set'
            if dec_data[1].strip() == 'S_QUANTITY' and dec_data[3].strip() == 'S_YTD' and dec_data[5].strip() == \
                    'S_ORDER_CNT' and dec_data[7].strip() == 'S_REMOTE_CNT' and dec_data[9].strip() == 'S_DATA' and \
                    dec_data[11].strip() == 'S_DIST_xx':
                self.misc[dec_data[1].strip()] = int(dec_data[2].strip())
                self.misc[dec_data[3].strip()] = int(dec_data[4].strip())
                self.misc[dec_data[5].strip()] = int(dec_data[6].strip())
                self.misc[dec_data[7].strip()] = int(dec_data[8].strip())
                self.misc[dec_data[9].strip()] = dec_data[10].strip()
                self.misc[dec_data[11].strip()] = dec_data[12].strip()

                print('dec_data[11]: ')
                print(dec_data[11].strip())

                print('S_DIST_xx from dec_data[12]: ')
                print(dec_data[12].strip())

                print('S_DIST_xx from self: ')
                print(self.misc[dec_data[11].strip()])
                return 'S_info has been set'

        elif op == 'get_W_info':
            W_ID = int(dec_data[1].strip())
            sender_mem = int(dec_data[2].strip())
            ret_val = self.get_W_info(self.warehouseTable, W_ID, sender_mem)
            return ret_val

        elif op == 'get_D_info':
            D_ID = int(dec_data[1].strip())
            D_W_ID = int(dec_data[2].strip())
            sender_mem = int(dec_data[3].strip())
            ret_val = self.get_D_info(self.districtTable, D_ID, D_W_ID, sender_mem)
            return ret_val

        elif op == 'get_C_info':
            C_ID = int(dec_data[1].strip())
            C_D_ID = int(dec_data[2].strip())
            C_W_ID = int(dec_data[3].strip())
            sender_mem = int(dec_data[4].strip())
            ret_val = self.get_C_info(self.customerTable, C_ID, C_D_ID, C_W_ID, sender_mem)
            return ret_val

        elif op == 'get_S_info':
            OL_I_ID = int(dec_data[1].strip())
            OL_SUPPLY_W_ID = int(dec_data[2].strip())
            D_ID = int(dec_data[3].strip())
            sender_mem = int(dec_data[4].strip())
            ret_val = self.get_S_info(self.stockTable, OL_I_ID, OL_SUPPLY_W_ID, D_ID, sender_mem)
            return ret_val

        elif op == 'update_S_info':
            OL_I_ID = int(dec_data[1].strip())
            OL_SUPPLY_W_ID = int(dec_data[2].strip())
            S_QUANTITY = int(dec_data[3].strip())
            S_YTD = int(dec_data[4].strip())
            S_ORDER_CNT = int(dec_data[5].strip())
            S_REMOTE_CNT = int(dec_data[6].strip())
            ret_val = self.update_S_info(self.stockTable, OL_I_ID, OL_SUPPLY_W_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT)
            return ret_val

        elif op == 'neworder_test':
            start = time.process_time()
            for i in range(1000):
                sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
                # chooses constant W_ID based off of total number of server nodes
                W_ID = random.randint(1, len(self.warehouse_info))
                # print('beginning W_ID' + str(W_ID))

                # chooses specific D_ID that corresponds to W_ID, e.g. if W_ID = 2, then starting_district and
                # ending_district will correspond to this ID and be in range (11, 20)
                starting_district = ((W_ID * 10) - 9)
                ending_district = (W_ID * 10)
                D_ID = random.randint(starting_district, ending_district)
                # print('beginning D_ID' + str(D_ID))

                # chooses specific C_ID from the NURand function defined in TPC-C spec. C_ID_offset is implementation
                # specific, offsets value produced by NURand it so it corrects C_ID to be respective of node with W_ID
                C_ID_offset = (D_ID - 1) * 3000

                C_ID = tpc_c.NURand(1023, 1, 3000) + C_ID_offset
                # print('beginning C_ID' + str(C_ID))

                ol_cnt = random.randint(5, 15)
                rbk = random.randint(1, 100)
                I_IDS = []
                I_W_IDS = []
                I_QTYS = []
                for item in range(ol_cnt):
                    if item == (ol_cnt - 1) and rbk == 1:
                        OL_I_ID = 3000000
                    else:
                        OL_I_ID = tpc_c.NURand(8191, 1, 100000)
                    I_IDS.append(OL_I_ID)
                    x = random.randint(1, 100)
                    if x > 1:
                        OL_SUPPLY_W_ID = self.my_warehouses_thresh
                    if x == 1:
                        list_W_IDS = []
                        for i in range(1, len(self.warehouse_info) + 1):
                            list_W_IDS.append(i)
                        list_W_IDS.remove(self.my_warehouses_thresh)
                        OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS) - 1))]
                    I_W_IDS.append(OL_SUPPLY_W_ID)
                    # print(I_W_IDS)
                    OL_QUANTITY = random.randint(1, 10)
                    I_QTYS.append(OL_QUANTITY)
                O_ENTRY_D = date.today().strftime("%d/%m/%Y")
                start = time.process_time()
                self.newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                                         self.warehouseTable, self.districtTable, self.customerTable, self.itemTable,
                                         self.newOrderTable, self.orderLineTable, self.orderTable, self.stockTable,
                                         sender_mem)
            end = time.process_time()
            print('start: ' + str(start))
            print('end: ' + str(end))
            ret_val = end - start
            time.sleep(1)
            return ret_val

        else:
            # return this if op is none of the above
            return 'Unrecognizable operation'


# initial server is establishing itself, giving the port it will use. It's address is default
if len(sys.argv) == 2:
    server = Server(sys.argv[1], False)
    server.run()
else:
    server = Server(sys.argv[1], True, sys.argv[2:])
    server.run()

'''
cd /Users/adilrahman/PycharmProjects/MastersProj
python3.6 server.py 10000

cd /Users/adilrahman/PycharmProjects/MastersProj
python3.6 server.py 10005 127.0.0.1 10000
'''

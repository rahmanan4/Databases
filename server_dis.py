import socket
import sys
import threading
import time
import random
import tpc_c
import json
import datetime
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
    # store of what warehouses, districts, and customers belong to this node
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
    historyTable = tpc_c.createHistoryTable(numOfWarehouses)
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
            'S_DIST_xx': None, 'C_BALANCE': None, 'C_FIRST': None, 'C_MIDDLE': None, 'C_LAST_ord_cid': None,
            'rem_cust': [], 'D_NEXT_O_ID_stock': None, 'low_stock': None, 'W_NAME_payment': None,
            'W_STREET_1_payment': None, 'W_STREET_2_payment': None, 'W_CITY_payment': None, 'W_STATE_payment': None,
            'W_ZIP_payment': None, 'W_YTD_payment': None, 'D_NAME_payment': None, 'D_STREET_1_payment': None,
            'D_STREET_2_payment': None, 'D_CITY_payment': None, 'D_STATE_payment': None, 'D_ZIP_payment': None,
            'D_YTD_payment': None, 'C_FIRST_payment': None, 'C_MIDDLE_payment': None, 'C_LAST_payment': None,
            'C_STREET_1_payment': None, 'C_STREET_2_payment': None, 'C_CITY_payment': None, 'C_STATE_payment': None,
            'C_ZIP_payment': None, 'C_PHONE_payment': None, 'C_SINCE_payment': None, 'C_CREDIT_payment': None,
            'C_CREDIT_LIM_payment': None, 'C_DISCOUNT_payment': None, 'C_DATA_payment': None, 'C_BALANCE_payment': None,
            'C_YTD_PAYMENT_payment': None, 'C_PAYMENT_CNT_payment': None}

    # stores address and ports of nodes to be connected to
    address_list = []
    port_list = []
    config_flag = 0
    pad_len = 1024
    trxn_count = 0

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

    # retrieve information for a connection if interconnected nodes > 1
    def get_connection(self, addr):
        for conn in self.connections:
            if conn.getpeername() is addr:
                return conn

    # set max threshold for key value store pseudo_database
    def set_my_mem_thresh(self, network_max):
        self.my_mem_thresh = network_max + 100
        self.mem_info[(self.my_address, self.my_port)] = self.my_mem_thresh

    # retrieve max threshold for key value store pseudo_database
    def get_mem_info_max(self):
        max_thresh = 0
        for key in self.mem_info:
            if self.mem_info[key] > max_thresh:
                max_thresh = self.mem_info[key]
        return max_thresh

    # set max warehouse threshold for TPC-C
    def set_my_warehouse_thresh(self, network_max):
        self.my_warehouses_thresh = network_max + 1
        self.warehouse_info[(self.my_address, self.my_port)] = self.my_warehouses_thresh
        self.warehouseTable[0]['W_ID'] = int(self.my_warehouses_thresh)
        self.warehouseTable[0]['W_NAME'] = ('Warehouse ' + str(self.my_warehouses_thresh))
        for i in range(0, 100000):
            self.stockTable[i]['S_W_ID'] = int(self.my_warehouses_thresh)

    # retrieve max warehouse threshold for TPC-C where warehouse and node number are equivalent
    def get_warehouse_info_max(self):
        max_warehouse = 0
        for key in self.warehouse_info:
            if self.warehouse_info[key] > max_warehouse:
                max_warehouse = self.warehouse_info[key]
        return max_warehouse

    # set max customer threshold for TPC-C
    def set_my_district_thresh(self, network_max):
        self.my_districts_thresh = network_max + 10
        self.district_info[(self.my_address, self.my_port)] = self.my_districts_thresh
        if network_max >= 10:
            for i in range(1, 11):
                self.districtTable[i - 1]['D_ID'] = ((self.my_warehouses_thresh - 1) * 10) + i
                self.districtTable[i - 1]['D_W_ID'] = int(self.my_warehouses_thresh)
                self.districtTable[i - 1]['D_NAME'] = ('District ' + str(((self.my_warehouses_thresh - 1) * 10) + i))

    # retrieve max district threshold for TPC-C
    def get_district_info_max(self):
        max_district = 0
        for key in self.district_info:
            if self.district_info[key] > max_district:
                max_district = self.district_info[key]
        return max_district

    # set max customer threshold for TPC-C
    def set_my_customer_thresh(self, network_max):
        self.my_customers_thresh = network_max + 30000
        self.customer_info[(self.my_address, self.my_port)] = self.my_customers_thresh
        j = 1
        k = 1
        if network_max >= 30000:
            for i in range(1, 30001):
                self.customerTable[i - 1]['C_ID'] = ((self.my_warehouses_thresh - 1) * 30000) + i
                self.customerTable[i - 1]['C_W_ID'] = int(self.my_warehouses_thresh)
                self.customerTable[i - 1]['C_D_ID'] = j + (self.my_districts_thresh - 10)
                k += 1
                if k == 3001:
                    k = 1
                    j += 1

    # necessary to execute if there is only one node for TPC-C
    def fix_firs_serv_num(self, network_max):
        j = 1
        k = 1
        if network_max >= 30000:
            for i in range(1, 30001):
                self.customerTable[i - 1]['C_ID'] = ((self.my_warehouses_thresh - 1) * 30000) + i
                self.customerTable[i - 1]['C_W_ID'] = int(self.my_warehouses_thresh)
                self.customerTable[i - 1]['C_D_ID'] = j + (self.my_districts_thresh - 10)
                k += 1
                if k == 3001:
                    k = 1
                    j += 1

    # retrieve max customer threshold for TPC-C
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

    # helper functions to perform transaction profiles for TPC-C
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
                        op = self.pad_msg(
                            'storeinfo,' + 'D_TAX,' + str(D_TAX) + ', ' + 'D_NEXT_O_ID,' + str(D_NEXT_O_ID) + ',')
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
                        op = self.pad_msg(
                            'storeinfo,' + 'C_DISCOUNT,' + str(C_DISCOUNT) + ', ' + 'C_LAST,' + str(C_LAST)
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
                    #xx = D_ID - ((self.my_warehouses_thresh * 10) - 10)
                    #S_DIST_xx = stock['S_DIST_' + '{0:0=2d}'.format(xx)]
                    xx = 10
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

    def get_C_info_ordstat(self, customerTable, C_ID, C_D_ID, C_W_ID, sender_mem, C_LAST):
        if C_ID is not None:
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                    C_BALANCE = customer['C_BALANCE']
                    C_FIRST = customer['C_FIRST']
                    C_MIDDLE = customer['C_MIDDLE']
                    C_LAST = customer['C_LAST']
                    break
            for addr in self.warehouse_info:
                if self.warehouse_info[addr] == sender_mem:
                    for connection in self.connections:
                        if connection.getpeername() == addr:
                            op = self.pad_msg(
                                'storeinfo,' + 'C_BALANCE,' + str(C_BALANCE) + ', ' + 'C_FIRST,' + str(C_FIRST)
                                + ', ' + 'C_MIDDLE,' + str(C_MIDDLE) + ', ' + 'C_LAST_ord_cid,' + str(C_LAST) + ',')
                            connection.send(str.encode(op))
                            return True
        if C_LAST is not None:
            customer_with_last = []
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                    customer_with_last.append(customer)
            sorted(customer_with_last, key=lambda i: i['C_FIRST'])
            # gets n/2 positioned customer where C_LAST is all the same
            customer_count = len(customer_with_last)
            if customer_count == 0:
                return
            assert customer_count != 0
            index = int((customer_count - 1) / 2)
            picked_customer = customer_with_last[index]
            C_ID = picked_customer['C_ID']
            for customer in customer_with_last:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                    C_BALANCE = customer['C_BALANCE']
                    C_FIRST = customer['C_FIRST']
                    C_MIDDLE = customer['C_MIDDLE']
                    C_LAST = customer['C_LAST']
            for addr in self.warehouse_info:
                if self.warehouse_info[addr] == sender_mem:
                    for connection in self.connections:
                        if connection.getpeername() == addr:
                            op = self.pad_msg(
                                'storeinfo,' + 'C_BALANCE,' + str(C_BALANCE) + ', ' + 'C_FIRST,' + str(C_FIRST)
                                + ', ' + 'C_MIDDLE,' + str(C_MIDDLE) + ', ' + 'C_LAST_ord_cid,' + str(C_LAST) + ',')
                            connection.send(str.encode(op))
                            self.misc['C_BALANCE'] = None
                            self.misc['C_FIRST'] = None
                            self.misc['C_MIDDLE'] = None
                            self.misc['C_LAST_ord_cid'] = None
                            return True

    def get_D_info_stock(self, districtTable, D_ID, D_W_ID, sender_mem):
        for district in districtTable:
            if D_ID == district['D_ID'] and D_W_ID == district['D_W_ID']:
                D_NEXT_O_ID = district['D_NEXT_O_ID']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'D_NEXT_O_ID_stock,' + str(D_NEXT_O_ID) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_low_stock(self, orderLineTable, stockTable, OL_W_ID, OL_D_ID, sender_mem, D_NEXT_O_ID, threshold):
        low_stock = 0
        for orderLine in orderLineTable:
            if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and \
                    (orderLine['OL_O_ID'] < D_NEXT_O_ID or orderLine['OL_O_ID'] >= (D_NEXT_O_ID - 20)):
                S_I_ID = orderLine['OL_I_ID']
                S_W_ID = OL_W_ID
                for stock in stockTable:
                    if S_I_ID == stock['S_I_ID'] and S_W_ID == stock['S_W_ID'] and stock['S_QUANTITY'] < threshold:
                        low_stock += 1
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'low_stock,' + str(low_stock) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_W_info_payment(self, warehouseTable, W_ID, sender_mem, H_AMOUNT):
        for warehouse in warehouseTable:
            if W_ID == warehouse['W_ID']:
                W_NAME = warehouse['W_NAME']
                W_STREET_1 = warehouse['W_STREET_1']
                W_STREET_2 = warehouse['W_STREET_2']
                W_CITY = warehouse['W_CITY']
                W_STATE = warehouse['W_STATE']
                W_ZIP = warehouse['W_ZIP']
                warehouse['W_YTD'] = warehouse['W_YTD'] + H_AMOUNT
                W_YTD = warehouse['W_YTD']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'W_NAME_payment,' + str(W_NAME) + ', ' 'W_STREET_1_payment,' +
                            str(W_STREET_1) + ', ' 'W_STREET_2_payment,' + str(W_STREET_2) + ', ' 'W_CITY_payment,' +
                            str(W_CITY) + ', ' + 'W_STATE_payment,' + str(W_STATE) + ', ' + 'W_ZIP_payment,' + str(W_ZIP)
                            + ', ' + 'W_YTD_payment,' + str(W_YTD) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_D_info_payment(self, districtTable, D_ID, D_W_ID, sender_mem, H_AMOUNT):
        for district in districtTable:
            if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
                D_NAME = district['D_NAME']
                D_STREET_1 = district['D_STREET_1']
                D_STREET_2 = district['D_STREET_2']
                D_CITY = district['D_CITY']
                D_STATE = district['D_STATE']
                D_ZIP = district['D_ZIP']
                district['D_YTD'] = district['D_YTD'] + H_AMOUNT
                D_YTD = district['D_YTD']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'D_NAME_payment,' + str(D_NAME) + ', ' 'D_STREET_1_payment,' +
                            str(D_STREET_1) + ', ' 'D_STREET_2_payment,' + str(D_STREET_2) + ', ' 'D_CITY_payment,' +
                            str(D_CITY) + ', ' + 'D_STATE_payment,' + str(D_STATE) + ', ' + 'D_ZIP_payment,' + str(D_ZIP)
                            + ', ' + 'D_YTD_payment,' + str(D_YTD) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_C_info_payment(self, customerTable, C_ID, C_D_ID, C_W_ID, sender_mem, H_AMOUNT):
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                C_BALANCE = customer['C_BALANCE']
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                C_YTD_PAYMENT = customer['C_YTD_PAYMENT']
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                C_PAYMENT_CNT = customer['C_PAYMENT_CNT']
                break
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'C_FIRST_payment,' + str(C_FIRST) + ', ' 'C_MIDDLE_payment,' +
                            str(C_MIDDLE) + ', ' 'C_LAST_payment,' + str(C_LAST) + ', ' 'C_STREET_1_payment,' +
                            str(C_STREET_1) + ', ' + 'C_STREET_2_payment,' + str(C_STREET_2) + ', ' + 'C_CITY_payment,' + str(C_CITY)
                            + ', ' + 'C_STATE_payment,' + str(C_STATE) + ', ' 'C_ZIP_payment,' + str(C_ZIP) + ', '
                            + 'C_PHONE_payment,' + str(C_PHONE) + ', ' + 'C_SINCE_payment,' + str(C_SINCE) + ', ' +
                            'C_CREDIT_payment,' + str(C_CREDIT) + ', ' + 'C_CREDIT_LIM_payment,' + str(C_CREDIT_LIM) + ', '
                            'C_DISCOUNT_payment,' + str(C_DISCOUNT) + ', ' + 'C_DATA_payment,' + str(C_DATA) + ', ' +
                            'C_BALANCE_payment,' + str(C_BALANCE) + ', ' + 'C_YTD_PAYMENT_payment,' + str(C_YTD_PAYMENT)
                            + ', ' + 'C_PAYMENT_CNT_payment,' + str(C_PAYMENT_CNT) + ', ')
                        connection.send(str.encode(op))
                        return True

    def get_C_info_payment_last(self, customerTable, C_LAST, C_D_ID, C_W_ID, sender_mem, H_AMOUNT):
        customer_with_last = []
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                customer_with_last.append(customer)
        sorted(customer_with_last, key=lambda i: i['C_FIRST'])
        # gets n/2 positioned customer where C_LAST is all the same
        customer_count = len(customer_with_last)
        if customer_count == 0:
            return
        index = int((customer_count - 1) / 2)
        picked_customer = customer_with_last[index]
        C_ID = picked_customer['C_ID']
        for customer in customer_with_last:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_ID = customer['C_ID']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                C_BALANCE = customer['C_BALANCE']
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                C_YTD_PAYMENT = customer['C_YTD_PAYMENT']
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                C_PAYMENT_CNT = customer['C_PAYMENT_CNT']
        for addr in self.warehouse_info:
            if self.warehouse_info[addr] == sender_mem:
                for connection in self.connections:
                    if connection.getpeername() == addr:
                        op = self.pad_msg(
                            'storeinfo,' + 'C_FIRST_payment,' + str(C_FIRST) + ', ' 'C_MIDDLE_payment,' +
                            str(C_MIDDLE) + ', ' 'C_LAST_payment,' + str(C_LAST) + ', ' 'C_STREET_1_payment,' +
                            str(C_STREET_1) + ', ' + 'C_STREET_2_payment,' + str(
                                C_STREET_2) + ', ' + 'C_CITY_payment,' + str(C_CITY)
                            + ', ' + 'C_STATE_payment,' + str(C_STATE) + ', ' 'C_ZIP_payment,' + str(C_ZIP) + ', '
                            + 'C_PHONE_payment,' + str(C_PHONE) + ', ' + 'C_SINCE_payment,' + str(C_SINCE) + ', ' +
                            'C_CREDIT_payment,' + str(C_CREDIT) + ', ' + 'C_CREDIT_LIM_payment,' + str(
                                C_CREDIT_LIM) + ', '
                                                'C_DISCOUNT_payment,' + str(
                                C_DISCOUNT) + ', ' + 'C_DATA_payment,' + str(C_DATA) + ', ' +
                            'C_BALANCE_payment,' + str(C_BALANCE) + ', ' + 'C_YTD_PAYMENT_payment,' + str(C_YTD_PAYMENT)
                            + ', ' + 'C_PAYMENT_CNT_payment,' + str(C_PAYMENT_CNT) + ', ')
                        connection.send(str.encode(op))
                        return True

    # pads string encoded message sent between sockets until message length == pad_len
    def pad_msg(self, og_msg):
        leng = len(og_msg.encode("utf-8"))
        if leng != self.pad_len:
            pad_amt = self.pad_len - leng
            add_amt = pad_amt * '-'
            edit_msg = og_msg + add_amt
        return edit_msg

    # Delivery transaction profile for TPC-C
    def deliveryTransaction(self, W_ID, O_CARRIER_ID, OL_DELIVERY_D, customerTable, newOrderTable, orderTable,
                            orderLineTable):
        result_file = []
        for D_ID in range(1, 11):
            NO_W_ID = W_ID
            NO_D_ID = D_ID
            low_NO_O_ID = float('inf')
            for newOrder in newOrderTable:
                if NO_W_ID == newOrder['NO_W_ID'] and NO_D_ID == newOrder['NO_D_ID'] and newOrder[
                    'NO_O_ID'] < low_NO_O_ID:
                    low_NO_O_ID = newOrder['NO_O_ID']
            if low_NO_O_ID == float('inf'):
                # TODO: if transaction is skipped, and skipped amount of transactions is more than 1% of total transactions,
                # then must report this. The ending result file produced should show percentage of skipped deliveries and
                # skipped districts
                continue
            tpc_c.deleteNewOrder(newOrderTable, low_NO_O_ID, NO_D_ID, NO_W_ID)
            O_W_ID = W_ID
            O_D_ID = D_ID
            O_ID = low_NO_O_ID
            for order in orderTable:
                if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_ID == order['O_ID']:
                    O_C_ID = order['O_C_ID']
                    order['O_CARRIER_ID'] = O_CARRIER_ID
            OL_W_ID = O_W_ID
            OL_D_ID = O_D_ID
            OL_O_ID = O_ID
            sum_total = 0
            for orderLine in orderLineTable:
                if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and OL_O_ID == orderLine['OL_O_ID']:
                    orderLine['OL_DELIVERY_D'] = OL_DELIVERY_D
                    sum_total += orderLine['OL_AMOUNT']
            C_W_ID = W_ID
            C_D_ID = D_ID
            C_ID = O_C_ID
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                    customer['C_BALANCE'] += sum_total
                    customer['C_DELIVERY_CNT'] += 1
            result_file.append((D_ID, low_NO_O_ID))
        return result_file

    # New-Order transaction profile for TPC-C
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
                            op = self.pad_msg(
                                'get_D_info,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg(
                                'get_D_info,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
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
                                op = self.pad_msg(
                                    'get_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' + str(
                                        D_ID) + ', ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg(
                                    'get_S_info,' + str(OL_I_ID) + ', ' + str(OL_SUPPLY_W_ID) + ', ' + str(
                                        D_ID) + ', ' + str(sender_mem) + ', ')
                                connection.send(str.encode(op))
                while self.misc['S_QUANTITY'] is None and self.misc['S_YTD'] is None and self.misc['S_ORDER_CNT'] \
                        is None and self.misc['S_REMOTE_CNT'] is None and self.misc['S_DATA'] is None and \
                        self.misc['S_DIST_' + '{0:0=2d}'.format(D_ID - ((W_ID * 10) - 10))] is None:
                    x = 5
                S_QUANTITY = self.misc['S_QUANTITY']
                S_YTD = self.misc['S_YTD']
                S_ORDER_CNT = self.misc['S_ORDER_CNT']
                S_REMOTE_CNT = self.misc['S_REMOTE_CNT']
                S_DATA = self.misc['S_DATA']
                S_DIST_xx = self.misc['S_DIST_xx']

            assert S_QUANTITY != None
            assert S_YTD != None
            assert S_ORDER_CNT != None
            assert S_REMOTE_CNT != None
            assert S_DATA != None
            assert S_DIST_xx != None

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
                # print('s_info updated')
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

    # Payment transaction profile for TPC-C
    def paymentTransaction(self, W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE, warehouseTable,
                           districtTable, customerTable, historyTable, sender_mem):
        warehouse_info = []
        district_info = []
        customer_info = []
        in_own_warehouse = False
        for warehouse in warehouseTable:
            if W_ID == warehouse['W_ID']:
                in_own_warehouse = True
                W_NAME = warehouse['W_NAME']
                W_STREET_1 = warehouse['W_STREET_1']
                W_STREET_2 = warehouse['W_STREET_2']
                W_CITY = warehouse['W_CITY']
                W_STATE = warehouse['W_STATE']
                W_ZIP = warehouse['W_ZIP']
                warehouse['W_YTD'] = warehouse['W_YTD'] + H_AMOUNT
                warehouse_info.append([W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, warehouse['W_YTD']])
                break
        if in_own_warehouse is False:
            for addr in self.warehouse_info:
                if self.warehouse_info[addr] >= W_ID > self.warehouse_info[addr] - 1:
                    # found the node where warehouse is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg('get_W_info_payment,' + str(W_ID) + ' , ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg('get_W_info_payment,' + str(W_ID) + ' , ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                            connection.send(str.encode(op))
            while self.misc['W_NAME_payment'] is None and self.misc['W_STREET_1_payment'] is None and \
                self.misc['W_STREET_2_payment'] is None and self.misc['W_CITY_payment'] is None and \
                self.misc['W_STATE_payment'] is None and self.misc['W_ZIP_payment'] is None and \
                self.misc['W_YTD_payment'] is None:
                x = 5
            W_NAME = self.misc['W_NAME_payment']
            W_STREET_1 = self.misc['W_STREET_1_payment']
            W_STREET_2 = self.misc['W_STREET_2_payment']
            W_CITY = self.misc['W_CITY_payment']
            W_STATE = self.misc['W_STATE_payment']
            W_ZIP = self.misc['W_ZIP_payment']
            W_YTD = self.misc['W_YTD_payment']
            assert W_NAME is not None
            assert W_STREET_1 is not None
            assert W_STREET_2 is not None
            assert W_CITY is not None
            assert W_STATE is not None
            assert W_ZIP is not None
            assert W_YTD is not None
            warehouse_info.append([W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD])

        in_own_district = False
        for district in districtTable:
            if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
                in_own_district = True
                D_NAME = district['D_NAME']
                D_STREET_1 = district['D_STREET_1']
                D_STREET_2 = district['D_STREET_2']
                D_CITY = district['D_CITY']
                D_STATE = district['D_STATE']
                D_ZIP = district['D_ZIP']
                district['D_YTD'] = district['D_YTD'] + H_AMOUNT
                D_YTD = district['D_YTD']
                district_info.append([D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD])
                break
        if in_own_district is False:
            for addr in self.warehouse_info:
                if self.district_info[addr] >= D_ID > self.district_info[addr] - 10 and \
                        self.warehouse_info[addr] >= D_W_ID > self.warehouse_info[addr] - 1:
                    # found the node where warehouse is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg('get_D_info_payment,' + str(D_ID) + ', ' + str(D_W_ID) + ' , ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg('get_D_info_payment,' + str(D_ID) + ', ' + str(D_W_ID) + ' , ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                            connection.send(str.encode(op))
            while self.misc['D_NAME_payment'] is None and self.misc['D_STREET_1_payment'] is None and \
                self.misc['D_STREET_2_payment'] is None and self.misc['D_CITY_payment'] is None and \
                self.misc['D_STATE_payment'] is None and self.misc['D_ZIP_payment'] is None and \
                self.misc['D_YTD_payment'] is None:
                x = 5
            D_NAME = self.misc['D_NAME_payment']
            D_STREET_1 = self.misc['D_STREET_1_payment']
            D_STREET_2 =self.misc['D_STREET_2_payment']
            D_CITY = self.misc['D_CITY_payment']
            D_STATE = self.misc['D_STATE_payment']
            D_ZIP = self.misc['D_ZIP_payment']
            D_YTD = self.misc['D_YTD_payment']
            assert D_NAME is not None
            assert D_STREET_1 is not None
            assert D_STREET_2 is not None
            assert D_CITY is not None
            assert D_STATE is not None
            assert D_ZIP is not None
            assert D_YTD is not None
            district_info.append([D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD])

        in_own_customer = False
        if C_ID is not None:
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                    in_own_customer = True
                    C_FIRST = customer['C_FIRST']
                    C_MIDDLE = customer['C_MIDDLE']
                    C_LAST = customer['C_LAST']
                    C_STREET_1 = customer['C_STREET_1']
                    C_STREET_2 = customer['C_STREET_2']
                    C_CITY = customer['C_CITY']
                    C_STATE = customer['C_STATE']
                    C_ZIP = customer['C_ZIP']
                    C_PHONE = customer['C_PHONE']
                    C_SINCE = customer['C_SINCE']
                    C_CREDIT = customer['C_CREDIT']
                    C_CREDIT_LIM = customer['C_CREDIT_LIM']
                    C_DISCOUNT = customer['C_DISCOUNT']
                    C_DATA = customer['C_DATA']
                    customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                    C_BALANCE = customer['C_BALANCE']
                    customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                    C_YTD_PAYMENT = customer['C_YTD_PAYMENT']
                    customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                    C_PAYMENT_CNT = customer['C_PAYMENT_CNT']
                    customer_info.append(
                        [C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                         C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
                         C_YTD_PAYMENT, C_PAYMENT_CNT])
                    break
            if in_own_customer is False:
                for addr in self.warehouse_info:
                    if self.customer_info[addr] >= C_ID > self.customer_info[addr] - 30000 and self.district_info[addr] \
                            >= C_D_ID > self.district_info[addr] - 10 and self.warehouse_info[addr] >= C_W_ID > \
                            self.warehouse_info[addr] - 1:
                        # found the node where district is
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_payment,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                              + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_payment,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                              + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                                connection.send(str.encode(op))
            # until the other server actually has given this server the information it needs, to random task of setting
            # x = 5
                while self.misc['C_FIRST_payment'] is None and self.misc['C_MIDDLE_payment'] is None and \
                    self.misc['C_LAST_payment'] is None and self.misc['C_STREET_1_payment'] is None and \
                    self.misc['C_STREET_2_payment'] is None and self.misc['C_CITY_payment'] is None and \
                    self.misc['C_STATE_payment'] is None and self.misc['C_ZIP_payment'] is None and \
                    self.misc['C_PHONE_payment'] is None and self.misc['C_SINCE_payment'] is None and \
                    self.misc['C_CREDIT_payment'] is None and self.misc['C_CREDIT_LIM_payment'] is None and \
                    self.misc['C_DISCOUNT_payment'] is None and self.misc['C_DATA_payment'] is None and \
                    self.misc['C_BALANCE_payment'] is None and self.misc['C_YTD_PAYMENT_payment'] is None and \
                    self.misc['C_PAYMENT_CNT_payment'] is None:
                    x = 5

                C_FIRST = self.misc['C_FIRST_payment']
                C_MIDDLE = self.misc['C_MIDDLE_payment']
                C_LAST = self.misc['C_LAST_payment']
                C_STREET_1 = self.misc['C_STREET_1_payment']
                C_STREET_2 = self.misc['C_STREET_2_payment']
                C_CITY = self.misc['C_CITY_payment']
                C_STATE = self.misc['C_STATE_payment']
                C_ZIP = self.misc['C_ZIP_payment']
                C_PHONE = self.misc['C_PHONE_payment']
                C_SINCE = self.misc['C_SINCE_payment']
                C_CREDIT = self.misc['C_CREDIT_payment']
                C_CREDIT_LIM = self.misc['C_CREDIT_LIM_payment']
                C_DISCOUNT = self.misc['C_DISCOUNT_payment']
                C_DATA = self.misc['C_DATA_payment']
                C_BALANCE = self.misc['C_BALANCE_payment']
                C_YTD_PAYMENT = self.misc['C_YTD_PAYMENT_payment']
                C_PAYMENT_CNT = self.misc['C_PAYMENT_CNT_payment']
                customer_info.append(
                    [C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                    C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
                    C_YTD_PAYMENT, C_PAYMENT_CNT])
            assert C_FIRST is not None
            assert C_MIDDLE is not None
            assert C_LAST is not None
            assert C_STREET_1 is not None
            assert C_STREET_2 is not None
            assert C_CITY is not None
            assert C_STATE is not None
            assert C_ZIP is not None
            assert C_PHONE is not None
            assert C_SINCE is not None
            assert C_CREDIT is not None
            assert C_CREDIT_LIM is not None
            assert C_DISCOUNT is not None
            assert C_DATA is not None
            assert C_BALANCE is not None
            assert C_YTD_PAYMENT is not None
            assert C_PAYMENT_CNT is not None

        in_own_customer = False
        if C_LAST is not None:
            customer_with_lastname = []
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                    in_own_customer = True
                    customer_with_lastname.append(customer)
            if in_own_customer is True:
                # sorts the list of customers in ascending order based off of C_FIRST
                sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
                # gets n/2 positioned customer where C_LAST is all the same
                customer_count = len(customer_with_lastname)
                if customer_count == 0:
                    return 'Last name not present'
                index = int((customer_count - 1) / 2)
                picked_customer = customer_with_lastname[index]
                C_ID = picked_customer['C_ID']
                for customer in customerTable:
                    if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                        C_ID = customer['C_ID']
                        C_FIRST = customer['C_FIRST']
                        C_MIDDLE = customer['C_MIDDLE']
                        C_STREET_1 = customer['C_STREET_1']
                        C_STREET_2 = customer['C_STREET_2']
                        C_CITY = customer['C_CITY']
                        C_STATE = customer['C_STATE']
                        C_ZIP = customer['C_ZIP']
                        C_PHONE = customer['C_PHONE']
                        C_SINCE = customer['C_SINCE']
                        C_CREDIT = customer['C_CREDIT']
                        C_CREDIT_LIM = customer['C_CREDIT_LIM']
                        C_DISCOUNT = customer['C_DISCOUNT']
                        C_DATA = customer['C_DATA']
                        customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                        C_BALANCE = customer['C_BALANCE']
                        customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                        C_YTD_PAYMENT = customer['C_YTD_PAYMENT']
                        customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                        C_PAYMENT_CNT = customer['C_PAYMENT_CNT']
                        break
            if C_W_ID != self.my_warehouses_thresh:
                for addr in self.warehouse_info:
                    if self.district_info[addr] >= C_D_ID > self.district_info[addr] - 10 and \
                            self.warehouse_info[addr] >= C_W_ID > self.warehouse_info[addr] - 1:
                        # found the node where district is
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_payment_last,' + str(C_LAST) + ', ' + str(C_D_ID) + ', ' +
                                                  str(C_W_ID) + ', ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_payment_last,' + str(C_LAST) + ', ' + str(C_D_ID) + ', ' +
                                                  str(C_W_ID) + ', ' + str(sender_mem) + ', ' + str(H_AMOUNT) + ', ')
                                connection.send(str.encode(op))
                while self.misc['C_FIRST_payment'] is None and self.misc['C_MIDDLE_payment'] is None and \
                        self.misc['C_LAST_payment'] is None and self.misc['C_STREET_1_payment'] is None and \
                        self.misc['C_STREET_2_payment'] is None and self.misc['C_CITY_payment'] is None and \
                        self.misc['C_STATE_payment'] is None and self.misc['C_ZIP_payment'] is None and \
                        self.misc['C_PHONE_payment'] is None and self.misc['C_SINCE_payment'] is None and \
                        self.misc['C_CREDIT_payment'] is None and self.misc['C_CREDIT_LIM_payment'] is None and \
                        self.misc['C_DISCOUNT_payment'] is None and self.misc['C_DATA_payment'] is None and \
                        self.misc['C_BALANCE_payment'] is None and self.misc['C_YTD_PAYMENT_payment'] is None and \
                        self.misc['C_PAYMENT_CNT_payment'] is None:
                    x = 5

                C_FIRST = self.misc['C_FIRST_payment']
                C_MIDDLE = self.misc['C_MIDDLE_payment']
                C_LAST = self.misc['C_LAST_payment']
                C_STREET_1 = self.misc['C_STREET_1_payment']
                C_STREET_2 = self.misc['C_STREET_2_payment']
                C_CITY = self.misc['C_CITY_payment']
                C_STATE = self.misc['C_STATE_payment']
                C_ZIP = self.misc['C_ZIP_payment']
                C_PHONE = self.misc['C_PHONE_payment']
                C_SINCE = self.misc['C_SINCE_payment']
                C_CREDIT = self.misc['C_CREDIT_payment']
                C_CREDIT_LIM = self.misc['C_CREDIT_LIM_payment']
                C_DISCOUNT = self.misc['C_DISCOUNT_payment']
                C_DATA = self.misc['C_DATA_payment']
                C_BALANCE = self.misc['C_BALANCE_payment']
                C_YTD_PAYMENT = self.misc['C_YTD_PAYMENT_payment']
                C_PAYMENT_CNT = self.misc['C_PAYMENT_CNT_payment']
                customer_info.append(
                    [C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
                     C_YTD_PAYMENT, C_PAYMENT_CNT])
                assert C_FIRST is not None
                assert C_MIDDLE is not None
                assert C_LAST is not None
                assert C_STREET_1 is not None
                assert C_STREET_2 is not None
                assert C_CITY is not None
                assert C_STATE is not None
                assert C_ZIP is not None
                assert C_PHONE is not None
                assert C_SINCE is not None
                assert C_CREDIT is not None
                assert C_CREDIT_LIM is not None
                assert C_DISCOUNT is not None
                assert C_DATA is not None
                assert C_BALANCE is not None
                assert C_YTD_PAYMENT is not None
                assert C_PAYMENT_CNT is not None
        if len(customer_info) == 0:
            return 'Last name not found'

        if C_CREDIT == 'BC':
            # map will perform the string typecasting on all of the iterables in the list, then join the strings together
            new_c_data = " ".join(map(str, [C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT]))
            C_DATA = (new_c_data + C_DATA)
            if len(C_DATA) > 500:
                # list slicing, will ensure that all characters to the right of 500 chars are removed
                C_DATA = C_DATA[:500]
                customer_info.append(
                    [C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
                     C_YTD_PAYMENT, C_PAYMENT_CNT])
        customer_info.append(
            [C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
             C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
             C_YTD_PAYMENT, C_PAYMENT_CNT])
        H_DATA = W_NAME + '    ' + D_NAME
        tpc_c.addToHistoryTable(historyTable, C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA)
        return [warehouse_info, district_info, customer_info]

    # Order-Status transaction profile for TPC-C
    def orderStatusTransaction(self, C_W_ID, C_D_ID, C_ID, C_LAST, customerTable, orderTable, orderLineTable, sender_mem):
        customer_info = []
        found_in_own_customer = False
        if C_ID is not None:
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                    found_in_own_customer = True
                    C_BALANCE = customer['C_BALANCE']
                    C_FIRST = customer['C_FIRST']
                    C_MIDDLE = customer['C_MIDDLE']
                    C_LAST = customer['C_LAST']
                    customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
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
                                op = self.pad_msg(
                                    'get_C_info_ordstat,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                    + str(sender_mem) + ', ' + 'cid,')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg(
                                    'get_C_info_ordstat,' + str(C_ID) + ', ' + str(C_D_ID) + ', ' + str(C_W_ID) + ', '
                                    + str(sender_mem) + ', ' + 'cid,')
                                connection.send(str.encode(op))
                # until the other server actually has given this server the information it needs, to random task of setting
                # x = 5
                while self.misc['C_BALANCE'] is None and self.misc['C_FIRST'] is None and self.misc['C_MIDDLE'] is None \
                        and self.misc['C_LAST_ord_cid'] is None:
                    x = 5
                C_BALANCE = self.misc['C_BALANCE']
                C_FIRST = self.misc['C_FIRST']
                C_MIDDLE = self.misc['C_MIDDLE']
                C_LAST = self.misc['C_LAST_ord_cid']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
        if C_LAST is not None:
            customer_with_lastname = []
            for customer in customerTable:
                if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                    found_in_own_customer = True
                    customer_with_lastname.append(customer)
            if found_in_own_customer is True:
                sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
                # gets n/2 positioned customer where C_LAST is all the same
                customer_count = len(customer_with_lastname)
                if customer_count == 0:
                    return 'Last name not present'
                index = int((customer_count - 1) / 2)
                picked_customer = customer_with_lastname[index]
                C_ID = picked_customer['C_ID']
                for customer in customer_with_lastname:
                    if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                        C_BALANCE = customer['C_BALANCE']
                        C_FIRST = customer['C_FIRST']
                        C_MIDDLE = customer['C_MIDDLE']
                        C_LAST = customer['C_LAST']
            if C_W_ID != self.my_warehouses_thresh:
                for addr in self.warehouse_info:
                    if self.district_info[addr] >= C_D_ID > self.district_info[addr] - 10 and \
                            self.warehouse_info[addr] >= C_W_ID > self.warehouse_info[addr] - 1:
                        # found the node where district is
                        for connection in self.connections:
                            # look for the socket to that node
                            if connection.getpeername() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_ordstat,' + str(C_LAST) + ', ' + str(C_D_ID) + ', ' +
                                                  str(C_W_ID) + ', ' + str(sender_mem) + ', ' + 'last,')
                                connection.send(str.encode(op))
                            if connection.getsockname() == addr:
                                # send the op
                                op = self.pad_msg('get_C_info_ordstat,' + str(C_LAST) + ', ' + str(C_D_ID) + ', ' +
                                                  str(C_W_ID) + ', ' + str(sender_mem) + ', ' + 'last,')
                                connection.send(str.encode(op))
                # until the other server actually has given this server the information it needs, to random task of setting
                # x = 5
                while self.misc['C_BALANCE'] is None and self.misc['C_FIRST'] is None and self.misc['C_MIDDLE'] is None \
                        and self.misc['C_LAST_ord_cid'] is None:
                    x = 5
                C_BALANCE = self.misc['C_BALANCE']
                C_FIRST = self.misc['C_FIRST']
                C_MIDDLE = self.misc['C_MIDDLE']
                C_LAST = self.misc['C_LAST_ord_cid']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
        if len(customer_info) == 0:
            return 'Last name not found'
        assert C_BALANCE is not None
        assert C_FIRST is not None
        assert C_MIDDLE is not None
        assert C_LAST is not None

        ordertable_info = []
        O_W_ID = C_W_ID
        O_D_ID = C_D_ID
        O_C_ID = C_ID

        O_ID = None
        for order in orderTable:
            if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_C_ID == order['O_C_ID']:
                O_ID = order['O_ID']
                O_ENTRY_D = order['O_ENTRY_D']
                O_CARRIER_ID = order['O_CARRIER_ID']
                ordertable_info.append([O_ID, O_ENTRY_D, O_CARRIER_ID])
        orderlinetable_info = []
        OL_W_ID = O_W_ID
        OL_D_ID = O_D_ID
        OL_O_ID = O_ID
        for orderLine in orderLineTable:
            if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_W_ID'] and OL_O_ID == orderLine['OL_O_ID']:
                OL_I_ID = orderLine['OL_I_ID']
                OL_SUPPLY_W_ID = orderLine['OL_SUPPLY_W_ID']
                OL_QUANTITY = orderLine['OL_QUANTITY']
                OL_AMOUNT = orderLine['OL_AMOUNT']
                OL_DELIVERY_D = orderLine['OL_DELIVERY_D']
                orderlinetable_info.append([OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D])
        self.misc['C_BALANCE'] = None
        self.misc['C_FIRST'] = None
        self.misc['C_MIDDLE'] = None
        self.misc['C_LAST_ord_cid'] = None
        return [customer_info, ordertable_info, orderlinetable_info]

    # Stock-Level transaction profile for TPC-C
    def stockLevelTransaction(self, W_ID, D_ID, threshold, districtTable, orderLineTable, stockTable, sender_mem):
        low_stock = 0
        D_W_ID = W_ID
        found_in_own_district = False
        for district in districtTable:
            if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
                found_in_own_district = True
                D_NEXT_O_ID = district['D_NEXT_O_ID']
        if found_in_own_district is False:
            for addr in self.warehouse_info:
                if self.district_info[addr] >= D_ID > self.district_info[addr] - 10 and \
                        self.warehouse_info[addr] >= D_W_ID > self.warehouse_info[addr] - 1:
                    # found the node where district is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg(
                                'get_D_info_stock,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg(
                                'get_D_info_stock,' + str(D_ID) + ', ' + str(D_W_ID) + ', ' + str(sender_mem) + ', ')
                            connection.send(str.encode(op))
            while self.misc['D_NEXT_O_ID_stock'] is None:
                x = 5
            D_NEXT_O_ID = self.misc['D_NEXT_O_ID_stock']
        OL_W_ID = W_ID
        OL_D_ID = D_ID
        found_in_own_orderline = False
        for orderLine in orderLineTable:
            if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and \
                    (orderLine['OL_O_ID'] < D_NEXT_O_ID or orderLine['OL_O_ID'] >= (D_NEXT_O_ID - 20)):
                found_in_own_orderline = True
                S_I_ID = orderLine['OL_I_ID']
                S_W_ID = W_ID
                for stock in stockTable:
                    if S_I_ID == stock['S_I_ID'] and S_W_ID == stock['S_W_ID'] and stock['S_QUANTITY'] < threshold:
                        low_stock += 1
        if found_in_own_orderline is False:
            for addr in self.warehouse_info:
                if self.district_info[addr] >= D_ID > self.district_info[addr] - 10 and \
                            self.warehouse_info[addr] >= D_W_ID > self.warehouse_info[addr] - 1:
                    # found the node where district is
                    for connection in self.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            op = self.pad_msg(
                                'get_low_stock,' + str(OL_W_ID) + ', ' + str(OL_D_ID) + ', ' + str(
                                    sender_mem) + ', ' + str(D_NEXT_O_ID) + ', ' + str(threshold) + ', ')
                            connection.send(str.encode(op))
                        if connection.getsockname() == addr:
                            # send the op
                            op = self.pad_msg(
                                'get_low_stock,' + str(OL_W_ID) + ', ' + str(OL_D_ID) + ', ' + str(
                                    sender_mem) + ', ' + str(D_NEXT_O_ID) + ', ' + str(threshold) + ', ')
                            connection.send(str.encode(op))
            while self.misc['low_stock'] is None:
                x = 5
            low_stock = self.misc['low_stock']
        return low_stock

    # function is called to initiate server node
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

                # read = newsock.makefile('r')
                # write = newsock.makefile('w')

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
            # read = conn.makefile('r')
            # write = conn.makefile('w')
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
                data = sock.recv(self.pad_len)
            else:
                data = conn.recv(self.pad_len)

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
            elif dec_data.find("JSON") != -1:
                dec = dec_data.split('@')
                dec_data = dec[1]
                dec_data = json.loads(dec_data)
                self.misc['rem_cust'] = self.misc['rem_cust'].append(dec_data.get("a"))
            else:
                # it has comma, so assume it is an instruction
                dec_data = dec_data.split(',')
                ret_val = self.operations(dec_data)
                print(ret_val)

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

    def tpc_test(self, trxn, stop):
        dec_data = trxn.split(',')
        ret_val = self.operations(dec_data)

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

        # TPC-C New-Order transaction profile op
        elif op == 'neworder':
            sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
            # chooses constant W_ID based off of total number of server nodes
            W_ID = random.randint(1, len(self.warehouse_info))
            #W_ID = 1
            # chooses specific D_ID that corresponds to W_ID, e.g. if W_ID = 2, then starting_district and
            # ending_district will correspond to this ID and be in range (11, 20)
            starting_district = ((W_ID * 10) - 9)
            ending_district = (W_ID * 10)
            D_ID = random.randint(starting_district, ending_district)

            # chooses specific C_ID from the NURand function defined in TPC-C spec. C_ID_offset is implementation
            # specific, offsets value produced by NURand it so it corrects C_ID to be respective of node with W_ID
            C_ID_offset = (D_ID - 1) * 3000
            C_ID = tpc_c.NURand(1023, 1, 3000) + C_ID_offset
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
                    if len(self.warehouse_info) == 1:
                        OL_SUPPLY_W_ID = self.my_warehouses_thresh
                    else:
                        OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS) - 1))]
                I_W_IDS.append(OL_SUPPLY_W_ID)
                OL_QUANTITY = random.randint(1, 10)
                I_QTYS.append(OL_QUANTITY)
            O_ENTRY_D = date.today().strftime("%d/%m/%Y")
            ret_val = self.newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                                     self.warehouseTable, self.districtTable, self.customerTable, self.itemTable,
                                     self.newOrderTable, self.orderLineTable, self.orderTable, self.stockTable,
                                     sender_mem)
            self.trxn_count += 1
            print('neworder done')
            return ret_val

        # TPC-C Payment transaction profile op
        elif op == 'payment':
            sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
            W_ID = random.randint(1, len(self.warehouse_info))
            #W_ID = 1
            D_W_ID = W_ID
            starting_district = ((W_ID * 10) - 9)
            ending_district = (W_ID * 10)
            D_ID = random.randint(starting_district, ending_district)
            x = random.randint(1, 100)
            y = random.randint(1, 100)
            if x <= 85:
                C_W_ID = W_ID
                C_D_ID = D_ID
            else:
                list_W_IDS = []
                for i in range(1, len(self.warehouse_info) + 1):
                    list_W_IDS.append(i)
                list_W_IDS.remove(self.my_warehouses_thresh)
                if len(self.warehouse_info) == 1:
                    C_W_ID = self.my_warehouses_thresh
                else:
                    C_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS) - 1))]
                starting_district = ((C_W_ID * 10) - 9)
                ending_district = (C_W_ID * 10)
                C_D_ID = random.randint(starting_district, ending_district)
            if y <= 60:
                lastNameList = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
                C_LAST = tpc_c.create_last_name_run(lastNameList, tpc_c.NURand(255, 0, 999))
                C_ID = None
            else:
                C_LAST = None
                C_ID_offset = (C_D_ID - 1) * 3000
                NURand_val = tpc_c.NURand(1023, 1, 3000)
                C_ID = NURand_val + C_ID_offset
            H_AMOUNT = round(random.uniform(1, 5000000), 2)
            H_DATE = date.today().strftime("%d/%m/%Y")
            ret_val = self.paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE,
                                    self.warehouseTable, self.districtTable, self.customerTable, self.historyTable, sender_mem)
            self.trxn_count += 1
            print('payment done')
            return ret_val

        # TPC-C Order-Status transaction profile op
        elif op == 'orderstatus':
            sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
            W_ID = random.randint(1, len(self.warehouse_info))
            #W_ID = 1
            starting_district = ((W_ID * 10) - 9)
            ending_district = (W_ID * 10)
            D_ID = random.randint(starting_district, ending_district)
            y = random.randint(1, 100)
            if y <= 60:
                lastNameList = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
                C_LAST = tpc_c.create_last_name_run(lastNameList, tpc_c.NURand(255, 0, 999))
                C_ID = None
            else:
                C_LAST = None
                C_ID_offset = (D_ID - 1) * 3000
                C_ID = tpc_c.NURand(1023, 1, 3000) + C_ID_offset

            ret_val = self.orderStatusTransaction(W_ID, D_ID, C_ID, C_LAST, self.customerTable, self.orderTable,
                                         self.orderLineTable, sender_mem)
            self.trxn_count += 1
            print('orderstatus done')
            return ret_val

        # TPC-C Delivery transaction profile op
        elif op == 'delivery':
            W_ID = random.randint(1, len(self.warehouse_info))
            #W_ID = 1
            O_CARRIER_ID = random.randint(1, 10)
            OL_DELIVERY_D = date.today().strftime("%d/%m/%Y")
            ret_val = self.deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, self.customerTable,
                                               self.newOrderTable, self.orderTable, self.orderLineTable)
            self.trxn_count += 1
            print('delivery done')
            return ret_val

        # TPC-C Stock-Level transaction profile op
        elif op == 'stocklevel':
            sender_mem = self.warehouse_info[(self.my_address, self.my_port)]
            W_ID = random.randint(1, len(self.warehouse_info))
            #W_ID = 1
            starting_district = ((W_ID * 10) - 9)
            ending_district = (W_ID * 10)
            D_ID = random.randint(starting_district, ending_district)
            threshold = random.randint(10, 20)
            ret_val = self.stockLevelTransaction(W_ID, D_ID, threshold, self.districtTable, self.orderLineTable,
                                                 self.stockTable, sender_mem)
            self.trxn_count += 1
            print('stocklevel done')
            return ret_val

        # returns specific nodes maximum value that its threshold contains
        elif op == 'mem_max':
            return self.get_mem_info_max()
        elif op == 'war_max':
            return self.get_warehouse_info_max()
        elif op == 'dist_max':
            return self.get_district_info_max()
        elif op == 'cust_max':
            return self.get_customer_info_max()

        # stores information within misc when a node needs information from other nodes
        elif op == 'storeinfo':
            if dec_data[1].strip() == 'W_TAX':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                return 'W_info has been set'
            if dec_data[1].strip() == 'D_TAX' and dec_data[3].strip() == 'D_NEXT_O_ID':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                self.misc[dec_data[3].strip()] = int(dec_data[4].strip())
                return 'D_info has been set'
            if dec_data[1].strip() == 'C_DISCOUNT' and dec_data[3].strip() == 'C_LAST' and dec_data[
                5].strip() == 'C_CREDIT':
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
                return 'S_info has been set'
            if dec_data[1].strip() == 'C_BALANCE' and dec_data[3].strip() == 'C_FIRST' and dec_data[5].strip() == \
                    'C_MIDDLE' and dec_data[7].strip() == 'C_LAST_ord_cid':
                self.misc[dec_data[1].strip()] = float(dec_data[2].strip())
                self.misc[dec_data[3].strip()] = dec_data[4].strip()
                self.misc[dec_data[5].strip()] = dec_data[6].strip()
                self.misc[dec_data[7].strip()] = dec_data[8].strip()
                return 'C_info_ordstat has been set'
            if dec_data[1].strip() == 'D_NEXT_O_ID_stock':
                self.misc[dec_data[1].strip()] = int(dec_data[2].strip())
                return 'D_info_stock has been set'
            if dec_data[1].strip() == 'low_stock':
                self.misc[dec_data[1].strip()] = int(dec_data[2].strip())
                return 'low_stock has been set'
            if dec_data[1].strip() == 'W_NAME_payment' and dec_data[3].strip() == 'W_STREET_1_payment' and \
                    dec_data[5].strip() == 'W_STREET_2_payment' and dec_data[7].strip() == 'W_CITY_payment' and \
                    dec_data[9].strip() == 'W_STATE_payment' and dec_data[11].strip() == 'W_ZIP_payment' and \
                    dec_data[13].strip() == 'W_YTD_payment':
                self.misc[dec_data[1].strip()] = dec_data[2].strip()
                self.misc[dec_data[3].strip()] = dec_data[4].strip()
                self.misc[dec_data[5].strip()] = dec_data[6].strip()
                self.misc[dec_data[7].strip()] = dec_data[8].strip()
                self.misc[dec_data[9].strip()] = dec_data[10].strip()
                self.misc[dec_data[11].strip()] = int(dec_data[12].strip())
                self.misc[dec_data[13].strip()] = float(dec_data[14].strip())
                return 'W_info_payment has been set'
            if dec_data[1].strip() == 'D_NAME_payment' and dec_data[3].strip() == 'D_STREET_1_payment' and \
                    dec_data[5].strip() == 'D_STREET_2_payment' and dec_data[7].strip() == 'D_CITY_payment' and \
                    dec_data[9].strip() == 'D_STATE_payment' and dec_data[11].strip() == 'D_ZIP_payment' and \
                    dec_data[13].strip() == 'D_YTD_payment':
                self.misc[dec_data[1].strip()] = dec_data[2].strip()
                self.misc[dec_data[3].strip()] = dec_data[4].strip()
                self.misc[dec_data[5].strip()] = dec_data[6].strip()
                self.misc[dec_data[7].strip()] = dec_data[8].strip()
                self.misc[dec_data[9].strip()] = dec_data[10].strip()
                self.misc[dec_data[11].strip()] = int(dec_data[12].strip())
                self.misc[dec_data[13].strip()] = float(dec_data[14].strip())
                return 'D_info_payment has been set'
            if dec_data[29].strip() == 'C_BALANCE_payment' and dec_data[31].strip() == 'C_YTD_PAYMENT_payment' and \
                    dec_data[33].strip() == 'C_PAYMENT_CNT_payment':
                self.misc[dec_data[1].strip()] = dec_data[2].strip()
                self.misc[dec_data[3].strip()] = dec_data[4].strip()
                self.misc[dec_data[5].strip()] = dec_data[6].strip()
                self.misc[dec_data[7].strip()] = dec_data[8].strip()
                self.misc[dec_data[9].strip()] = dec_data[10].strip()
                self.misc[dec_data[11].strip()] = dec_data[12].strip()
                self.misc[dec_data[13].strip()] = dec_data[14].strip()
                self.misc[dec_data[15].strip()] = int(dec_data[16].strip())
                self.misc[dec_data[17].strip()] = dec_data[18].strip()
                self.misc[dec_data[19].strip()] = dec_data[20].strip()
                self.misc[dec_data[21].strip()] = dec_data[22].strip()
                self.misc[dec_data[23].strip()] = float(dec_data[24].strip())
                self.misc[dec_data[25].strip()] = float(dec_data[26].strip())
                self.misc[dec_data[27].strip()] = dec_data[28].strip()
                self.misc[dec_data[29].strip()] = float(dec_data[30].strip())
                self.misc[dec_data[31].strip()] = float(dec_data[32].strip())
                self.misc[dec_data[33].strip()] = int(dec_data[34].strip())
                return 'C_info_payment has been set'

        # retrieves information when a node needs information from other nodes
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
            ret_val = self.update_S_info(self.stockTable, OL_I_ID, OL_SUPPLY_W_ID, S_QUANTITY, S_YTD, S_ORDER_CNT,
                                         S_REMOTE_CNT)
            return ret_val

        elif op == 'get_C_info_ordstat':
            if dec_data[5].strip() == 'cid':
                C_ID = int(dec_data[1].strip())
                C_D_ID = int(dec_data[2].strip())
                C_W_ID = int(dec_data[3].strip())
                sender_mem = int(dec_data[4].strip())
                C_LAST = None
                ret_val = self.get_C_info_ordstat(self.customerTable, C_ID, C_D_ID, C_W_ID, sender_mem, C_LAST)
                return ret_val
            if dec_data[5].strip() == 'last':
                C_LAST = dec_data[1].strip()
                C_D_ID = int(dec_data[2].strip())
                C_W_ID = int(dec_data[3].strip())
                sender_mem = int(dec_data[4].strip())
                C_ID = None
                ret_val = self.get_C_info_ordstat(self.customerTable, C_ID, C_D_ID, C_W_ID, sender_mem, C_LAST)
                return ret_val

        elif op == 'get_D_info_stock':
            D_ID = int(dec_data[1].strip())
            D_W_ID = int(dec_data[2].strip())
            sender_mem = int(dec_data[3].strip())
            ret_val = self.get_D_info_stock(self.districtTable, D_ID, D_W_ID, sender_mem)
            return ret_val

        elif op == 'get_low_stock':
            OL_W_ID = int(dec_data[1].strip())
            OL_D_ID = int(dec_data[2].strip())
            sender_mem = int(dec_data[3].strip())
            D_NEXT_O_ID = int(dec_data[4].strip())
            threshold = int(dec_data[5].strip())
            ret_val = self.get_low_stock(self.orderLineTable, self.stockTable, OL_W_ID, OL_D_ID, sender_mem, D_NEXT_O_ID, threshold)
            return ret_val

        elif op == 'get_W_info_payment':
            W_ID = int(dec_data[1].strip())
            sender_mem = int(dec_data[2].strip())
            H_AMOUNT = float(dec_data[3].strip())
            ret_val = self.get_W_info_payment(self.warehouseTable, W_ID, sender_mem, H_AMOUNT)
            return ret_val

        elif op == 'get_D_info_payment':
            D_ID = int(dec_data[1].strip())
            D_W_ID = int(dec_data[2].strip())
            sender_mem = int(dec_data[3].strip())
            H_AMOUNT = float(dec_data[4].strip())
            ret_val = self.get_D_info_payment(self.districtTable, D_ID, D_W_ID, sender_mem, H_AMOUNT)
            return ret_val

        elif op == 'get_C_info_payment':
            C_ID = int(dec_data[1].strip())
            C_D_ID = int(dec_data[2].strip())
            C_W_ID = int(dec_data[3].strip())
            sender_mem = int(dec_data[4].strip())
            H_AMOUNT = float(dec_data[5].strip())
            ret_val = self.get_C_info_payment(self.customerTable, C_ID, C_D_ID, C_W_ID, sender_mem, H_AMOUNT)
            return ret_val

        elif op == 'get_C_info_payment_last':
            C_LAST = dec_data[1].strip()
            C_D_ID = int(dec_data[2].strip())
            C_W_ID = int(dec_data[3].strip())
            sender_mem = int(dec_data[4].strip())
            H_AMOUNT = float(dec_data[5].strip())
            ret_val = self.get_C_info_payment_last(self.customerTable, C_LAST, C_D_ID, C_W_ID, sender_mem, H_AMOUNT)
            return ret_val

        # operations manager does not know what to do with user input, return this
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

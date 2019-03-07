import socket
import sys
import threading


class Server:
    my_address = "127.0.0.1"
    # communicate with server on port --
    my_port = ""
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
    #
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
        '''
        for key in self.mem_info:
            #print(address)
            if self.mem_info[key] > self.my_mem_thresh:
                    self.my_mem_thresh = self.mem_info[key]
        '''
        self.my_mem_thresh = network_max + 100
        self.mem_info[(self.my_address, self.my_port)] = self.my_mem_thresh

    def get_mem_info_max(self):
        max_thresh = 0
        for key in self.mem_info:
            # print(address)
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
                # print(address)
                if self.mem_info[addr] > key and key > self.mem_info[addr] - 100:
                    # found the node where key is located
                    for connection in self.connections:
                        # look for the socket to that node
                        print(connection.getpeername(), connection.getsockname(), addr)
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
    def read(self, key, sender_port):
        if key <= self.my_mem_thresh and key > self.my_mem_thresh - 100:
            value = str(self.pseudo_database[key])
            send
            for connection in self.connections:
                print(connection)
                print(connection.getsockname())
                if connection.getsockname() == sender_port:
                    connection.send(str.encode(value))
            return value

        else:
            # the key is not in this node. Look for appropriate node and send it there
            for addr in self.mem_info:
                # print(address)
                if self.mem_info[addr] > key and key > self.mem_info[addr] - 100:
                    # found the node where key is located
                    for connection in self.connections:
                        # look for the socket to that node
                        print(connection.getpeername(), connection.getsockname(), addr)
                        if connection.getpeername() == addr:
                            # send the op
                            connection.send(str.encode('r, ' + str(key)))
                            # return None when operation is sent to other node.
                            return None
                        if connection.getsockname() == addr:
                            # send the op
                            connection.send(str.encode('r, ' + str(key)))
                            # return None when operation is sent to other node
                            return None
            return False
        return value

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

    # manages operations
    # dec_data is a list of all words in the command
    def operations(self, dec_data):
        # dec_data is an array of strings which was tokenized using ','
        # strip white-space
        op = dec_data[0].strip()

        # check operation and format: <w,key,value> or <r, key>
        if op == 'w' and len(dec_data) == 3:
            ret_val = self.write(int(dec_data[1].strip()), \
                                 int(dec_data[2].strip()))
        elif op == 'r' and len(dec_data) == 2:
            sender_port = self.sock.getsockname()
            print(sender_port)
            ret_val = self.read(int(dec_data[1].strip()), sender_port)
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
            # for connection in self.connections:
            # print(connection)
            # connection.send(msg)

    # recieve Msg from other nodes
    # check if Msg is an operation, if so perform it
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
                # print("DEC_DATA", dec_data)
                self.mem_info[(dec_data[1], int(dec_data[2]))] = int(dec_data[3].strip())
                # print("BEFORE", self.mem_info, self.my_mem_thresh)

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
                # it has coma, so assume it is an instruction
                dec_data = dec_data.split(',')
                ret_val = self.operations(dec_data)
                print(ret_val)

    # run
    def run(self):
        if len(self.address_list) != 0:
            # this server has to be conected to (self.address, self.port)
            for address, port in zip(self.address_list, self.port_list):
                newsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                newsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.csock.append(newsock)
                newsock.connect((address, port))
                # add newsoc to connections
                self.connections.append(newsock)
                print("Connected to: ", address, ":", port)
                # Successfully conected. Now thread to recieve data from (address, port)
                # This is a client socket so send only socket to appropriate informal parameter
                rThread = threading.Thread(target=self.recvMsg, args=(None, None, newsock,))
                rThread.daemon = True
                rThread.start()
            # self.csock.send(str.encode("L, " + str(self.my_port)))

        # thread to send data to every node in connections[]
        iThread = threading.Thread(target=self.sendMsg)
        iThread.daemon = True
        iThread.start()

        # listen for incoming connections to this node
        while True:
            conn, address = self.sock.accept()
            print('Accepted connection from ', address, ' successfully!')
            conn.send(str.encode('Welcome:' + self.my_address + ":" + str(self.my_port) + ":" \
                                 + str(self.my_mem_thresh) + ":" + str(self.get_mem_info_max())))
            cThread = threading.Thread(target=self.handler, \
                                       args=(conn, address))
            cThread.daemon = True
            cThread.start()
            self.connections.append(conn)
            # print(self.connections)
            # print(str(address[0]), ':', str(address[1]), 'connected!')

    # handler
    def handler(self, conn, address):
        reply = ""
        self.recvMsg(conn, address)
        # welcome = 'Connection successful. Welcome! \n <w, key, value> OR <r, key>'
        # conn.send(welcome.encode('utf-8'))


'''
        while True:
        # data receiving from client
            data = conn.recv(1024)
            dec_data = data.decode('utf-8')
            # print client msg on server
            print(address, ':', dec_data, "\n")
            dec_data = dec_data.split(',')
            #strip white-space
            op = dec_data[0].strip()
            if op is 'w' or op is 'r':
            #send decoded data from client to operation
                ret_val = self.operations(dec_data)
                print(ret_val)
            # making server's reply to clients
                reply = 'From Server: ' + str(ret_val)
            # send reply to the node that sent request
            conn.send(reply.encode('utf-8'))
            #for connection in self.connections:
                # sending reply to other connected clients but not itself
                #if not connection == conn:
                    #connection.send(str.encode(reply))
                # sends operation status to the client that requested an operation
                #if connection == conn:
                    #connection.send(str.encode(reply))
            # to close a connection # breaking out
            if not data:
                print(str(address[0]), ':', str(address[1]), " disconnected.")
                # remove connection from the list of connections
                self.connections.remove(conn)
                # close the connection
                conn.close()
                break
'''
'''
# Client
class Client:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def sendMsg(self):
        while True:
            self.sock.send(bytes(input(""), 'utf-8'))
    def __init__(self, address):
        self.sock.connect((address, 10000))
        iThread = threading.Thread(target=self.sendMsg)
        iThread.daemon = True
        iThread.start()
        while True:
            data = self.sock.recv(1024)
            if not data:
                break
            print(str(data, 'utf-8'))
'''

if len(sys.argv) == 2:
    server = Server(sys.argv[1], False)
    server.run()
else:
    server = Server(sys.argv[1], True, sys.argv[2:])
    server.run()

'''
cd /Users/jimmysok/PycharmProjects/DB
python server.py 10000

cd /Users/jimmysok/PycharmProjects/DB
python server.py 10005 127.0.0.1 10000

cd /Users/jimmysok/PycharmProjects/DB
python server.py 10010 127.0.0.1 10000 127.0.0.1 10005
'''

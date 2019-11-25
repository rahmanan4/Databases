Exploring a distributed database prototype


Project Description:
	Transitions from centralized to distributed databases have become a popular topic for research. This report seeks to implement a distributed database system that overcomes some issues that centralized systems face such as bottlenecking and scalability. Initial Python implemented prototype was created with serial and concurrent locking mechanisms. Throughput for varying number of interconnected nodes ranged from 2,290-11,681 operations/sec. Minimum latency values for n=4 interconnected nodes  with concurrent execution for 1,000 and 10,000 operations were 0.2643 and 2.6296 sec respectively. Further research will be performed in implementing the model on different programming language platforms.



Prerequisites:
Python 3.6.1 (Install at: https://www.python.org/downloads/release/python-361/)

Use pip install to retrieve the following libraries:
	import socket 
	import sys
	import threading
	import time
	import random



Local Deployment (Mac):
**For deployment on other OS, run server.py file through python console

1.) Run an instance of the terminal


2.) Use the cd command to change the directory to the location where the server.py file is located.


3.) Run the server.py file and pass initial system arguments. Arguments will be a combination of ports and local addresses depending on how many nodes you wish to connect. For n=1:
	Ex: python server.py 10000
	
	10000 is the desired port for the first server node to bind to
	**Note: Choose a port number in the range outside of (0-1023)
	

4.) Once a node or server is bound, you can perform any of the operations in the Running Tests section. However, any read operations can only be performed if there are already values assigned to the database. Please use Test 15 in Running Tests a few times to fill all keys with values.


5.) If you wish to connect more nodes, open up additional instances of terminals. 


6.) Run the server.py file again, however pass these modified arguments. For n=2:
	Ex: python server.py 10005 127.0.0.1 10000
	
	10005 is the desired port for the second server node to bind to
	127.0.0.1 is the local address that the second node will use to connect to other nodes
	10000 is the desired port that server node 2 will connect to


7.) For each additional node you wish to connect, run step 6 again but include all existing nodes ports and addresses you wish to connect to.
	

Running Tests (Transaction Types):
 1.) w, key, value (Ex: w, 10, 10)
	- Will perform a single write operation given a specific key and value. Key must be within existing domains of nodes currently running.


 2.) r, key (Ex. Input: r, 10)
	- Will perform a single read operation given a specific key. Key must be within existing domain of nodes, and there must be a value already assigned to key, otherwise will return 'False'.


 3.) rnlthru (Ex. Input: rnlthru)
	- Will perform a throughput test for number of read operations with no locks for one second. In function 'def rnlthroughput(self, sender_mem, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 4.) rtsthru (Ex. Input: rtsthru)
	- Will perform a throughput test for number of read operations with serial execution for one second. In function 'def rtshroughput(self, sender_mem, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 5.) rtcthru (Ex. Input: rtcthru)
	- Will perform a throughput test for number of read operations with concurrent execution for one second. In function 'def rtchroughput(self, sender_mem, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 6.) wnlthru (Ex. Input: wnlthru)
	- Will perform a throughput test for number of write operations with no locks for one second. In function 'def wnlthroughput(self, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 7.) wtsthru (Ex. Input: wtsthru)
	- Will perform a throughput test for number of write operations with serial execution for one second. In function 'def wtshroughput(self, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 8.) wtcthru (Ex. Input: wtcthru)
	- Will perform a throughput test for number of write operations with concurrent execution for one second. In function 'def wtchroughput(self, seconds)', must set key = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then key = randint(0,399).


 9.) rnl (Ex. Input: rnl)
	- Will perform a latency test for read operations with no locks. In function 'def readtestnolock(self, sender_mem)', must set value = randint(0, max number of domain latest 	connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


10.) rts (Ex. Input: rts)
	- Will perform a latency test for read operations with serial 	execution. In function 'def readtestserial(self, sender_mem)', 	must set value = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


11.) rtc (Ex. Input: rtc)
	- Will perform a latency test for read operations with 	concurrent execution. In function 'def readtestconcurrent(self, sender_mem)', must set value = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


12.) wnl (Ex. Input: wnl)
	- Will perform a latency test for write operations with no locks. In function 'def writetestnolock(self)', must set value = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


13.) wts (Ex. Input: wts)
	- Will perform a latency test for write operations with serial 	execution. In function 'def writetestserial(self)', must set value = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


14.) wtc (Ex. Input: wtc)
	- Will perform a latency test for write operations with concurrent execution. In function 'def writetestconcurrent(self)', must set value = randint(0, max number of domain latest connected node). For example, if there are 4 nodes connected, then value = randint(0,399). Must set num_reads to preferred number of operations to perform.


15.) wfr (Ex. Input: wfr)
	- Will perform a large amount of write operations to fill up the database with values



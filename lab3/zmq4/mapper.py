import pickle
import sys
import time
import constRR
import zmq

import constPipe


#Pipeline Verbindung
me = str(sys.argv[1])
address1 = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT1  # 1st task src
context = zmq.Context()
pull_socket = context.socket(zmq.PULL)  # create a pull socket
pull_socket.connect(address1)  # connect to task source 1
time.sleep(1) 

#Verbidnung zu reducer
address = "tcp://" + constRR.HOST + ":" + constRR.PORT1  # how and where to connect
requester = context.socket(zmq.REQ)  # create request socket

requester.connect(address)  # request connection and go on 

#Verbidnung zu reducer2
address = "tcp://" + constRR.HOST + ":" + constRR.PORT2  # how and where to connect
requester2 = context.socket(zmq.REQ)  # create request socket

requester2.connect(address)  # request connection and go on 


print("{} started".format(me))

while True:
    work = pickle.loads(pull_socket.recv())  # receive work from a source
    print("{} received workload {} from {}".format(me, work[1], work[0]))
    words=work[1].split()
    for word in words:
        first_letter = word[0].lower()
        if first_letter <= 'k':
            requester.send_string(word.lower())  # send message and go on
            print("Sent {}".format(word))  # print ack
            message = requester.recv()  # block until response
        else:
            requester2.send_string(word.lower())  # send message and go on
            print("Sent {}".format(word))  # print ack
            message = requester2.recv()  # block until response
    
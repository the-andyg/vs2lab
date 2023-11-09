import zmq

import constRR

address = "tcp://" + constRR.HOST + ":" + constRR.PORT1  # how and where to connect

context = zmq.Context()
reply_socket = context.socket(zmq.REP)  # create reply socket
 
reply_socket.bind(address)  # bind socket to address

thisdict = {}

while True:
    message = reply_socket.recv()  # wait for incoming message
    if b"STOP" not in message:  # if not to stop...
        messagedecoded=message.decode()
        if messagedecoded in thisdict:
            thisdict[messagedecoded]+=1
        else:
            thisdict[messagedecoded]=1
        print("Received " + message.decode()+" bisheriges Vorkommen: "+str(thisdict[messagedecoded]))
        reply_socket.send((messagedecoded + "*").encode())  # append "*" to message
    else:  # else...
        break  # break out of loop and end

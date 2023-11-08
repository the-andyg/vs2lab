import constRPC
import threading
import time

from context import lab_channel


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client:

    result = ''

    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.ackChan = lab_channel.Channel()
        self.lab = self.ackChan.join('lab_channel')
        self.server = None

    def run(self):
        self.chan.bind(self.client)
        self.ackChan.bind(self.lab)
        self.server = self.chan.subgroup('server')
        self.labMembers = self.ackChan.subgroup('lab_channel')

    def stop(self):
        self.chan.leave('client')
        self.ackChan.leave('lab_channel')

    def receiveCallback(self, result):
        #print("Result is: " + result[1])
        self.result = result[1]

    def rpc_request(self):
        result = self.chan.receive_from(self.server)  # wait for response
        self.receiveCallback(result)

    def append(self, data, db_list):
        print("appending...")
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        print("waiting for ACK")
        result = self.ackChan.receive_from(self.labMembers)
        print(result)
        if(str(result[1]) == "ACK"):
            asyncThread = threading.Thread(target=self.rpc_request)
            asyncThread.start()

            for i in range(0, 10):   #only to show concurrency
                time.sleep(1)
                print("waiting for async result: " +str(10-i))

            asyncThread.join()
        return self.result

class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.labChan = lab_channel.Channel()
        self.labChannel = self.labChan.join('lab_channel')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        time.sleep(10)
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def run(self):
        self.chan.bind(self.server)
        self.labChan.bind(self.labChannel)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                print("RPCAppend: " + constRPC.APPEND + ", msgrpc: " + msgrpc[0])
                if constRPC.APPEND == msgrpc[0]:  # check what is being requested
                    self.labChan.send_to({client}, "ACK") # send ACKnoledgement
                    print("Sent ACK")
                    result = self.append(msgrpc[1], msgrpc[2])  # do local call
                    print("Sending actual result")
                    self.chan.send_to({client}, result)  # return response
                else:
                    pass  # unsupported request, simply ignore
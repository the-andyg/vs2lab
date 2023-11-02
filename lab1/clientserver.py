"""
Client and server using classes
"""

import logging
import socket
import json

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True
    # List of random first names
    first_names = []
    phone_book = {}

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))
        for i in range(5):
            letterone = chr(ord('A') + i )
            for j in range(5):
                lettertwo = chr(ord('A') + j)
                name = letterone + lettertwo
                self.first_names.append(name)
        for name in self.first_names:
            num = 0
            for char in name:
                num = num * 26 + (ord(char) - ord('A') + 1)
            self.phone_book[name] = str(num)
        self._logger.info(str(self.phone_book))

    def serve(self):
        """ Serve echo """
        self.sock.listen(1)
        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                # pylint: disable=unused-variable
                (connection, address) = self.sock.accept()  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024).decode('ascii')  # receive data from client
                    self._logger.info("Received DATA: " + data)
                    if data.startswith("GET "):
                        self._logger.info("Data starts with GET ")
                        #split command and name to process later
                        name = data.split(" ")[1]
                        check = False
                        for key in self.first_names:
                            if (key == name):
                                check = True
                        if check:
                            connection.send(self.phone_book[name].encode('ascii'))
                        else:
                            connection.send(json.dumps("").encode('ascii'))
                    elif data.startswith("GETALL"):
                        self._logger.info("Data starts with GETALL")
                        #return serialized array
                        connection.send(str(self.phone_book).encode('ascii'))
                    if not data:
                        break  # stop if client stopped
                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")


class Client:
    """ The client """
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def call(self, msg_in="Hello, world"):
        """ Call server """
        self.logger.info("Calling Server")

        self.sock.send(msg_in.encode('ascii'))
        data = self.sock.recv(1024)  # receive the response
        msg_out = data.decode('ascii')
        self.logger.info("Received Message: " + msg_out)  # print the result

        #self.sock.close()  # close the connection
        #self.logger.info("Client down.")
        return msg_out

    def close(self):
        """ Close socket """
        self.sock.close()

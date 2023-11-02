"""
Simple client server unit test
"""

import logging
import threading
import unittest
import json

import clientserver
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


class TestEchoService(unittest.TestCase):
    """The test"""
    _server = clientserver.Server()  # create single server in class variable
    _server_thread = threading.Thread(target=_server.serve)  # define thread for running server

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()  # start server loop in a thread (called only once)

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client()  # create new client for each test

    def test_srv_get_bb(self):  # each test_* function is a test
        """Test simple call"""
        msg = self.client.call("GET BB")
        num = 0
        for char in "BB":
            num = num * 26 + (ord(char) - ord('A') + 1)
        self.assertEqual(msg, str(num))

    def test_srv_get_aa(self):  # each test_* function is a test
        """Test simple call"""
        msg = self.client.call("GET AA")
        num = 0
        for char in "AA":
            num = num * 26 + (ord(char) - ord('A') + 1)
        self.assertEqual(msg, str(num))

    def test_srv_get_empty(self):  # each test_* function is a test
        """Test simple call"""
        msg = self.client.call("GET ZZ")
        self.assertEqual(msg, '""')

    def test_srv_get_all(self): # each test_* function is a test
        """Test simple call"""
        msg = self.client.call("GETALL")
        first_names = []
        phone_book = {}
        for i in range(5):
            letterone = chr(ord('A') + i )
            for j in range(5):
                lettertwo = chr(ord('A') + j)
                name = letterone + lettertwo
                first_names.append(name)
        for name in first_names:
            num = 0
            for char in name:
                num = num * 26 + (ord(char) - ord('A') + 1)
            phone_book[name] = str(num)
        self.assertEqual(msg, str(phone_book))

    def tearDown(self):
        self.client.close()  # terminate client after each test

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = False  # break out of server loop. pylint: disable=protected-access
        cls._server_thread.join()  # wait for server thread to terminate


if __name__ == '__main__':
    unittest.main()

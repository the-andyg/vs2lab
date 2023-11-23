import rpc
import logging
import threading
import time

from context import lab_logging

def countdown():
    for i in range(0, 15):   #only to show concurrency
        time.sleep(1)
        print("waiting for async result: " +str(15-i))

lab_logging.setup(stream_level=logging.INFO)

cl = rpc.Client()
cl.run()

base_list = rpc.DBList({'foo'})

countdownThread = threading.Thread(target=countdown)
countdownThread.start()

result_list = cl.append('bar', base_list)

print("Result: {}".format(result_list.value))

cl.stop()
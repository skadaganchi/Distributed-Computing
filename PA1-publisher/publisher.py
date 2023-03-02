import json
import random
import sys
import time
import xmlrpc.client

# Load the data from the JSON file
with open('data-am.json') as f:
    dataset = json.load(f)


worker_port = int(sys.argv[1])
with xmlrpc.client.ServerProxy(f"http://localhost:{worker_port}/") as proxy:
    for record in dataset:
        msg = json.dumps(record).encode()
        msg_len = len(msg).to_bytes(4, byteorder='big')
        proxy.send(msg_len + msg)
        print(msg)
        sleepTime = random.uniform(10, 15)
        print(f"sleeptime {sleepTime}")
        time.sleep(sleepTime)
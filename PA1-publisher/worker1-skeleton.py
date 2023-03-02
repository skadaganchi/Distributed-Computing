from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import sys
import json

# Storage of data
data_table = {}

def processData(message):
# Receive data from the publisher in a loop
 while True:
    # Read the message length
    msg_len = message(4)
    if not msg_len:
        break
    msg_len = int.from_bytes(msg_len, byteorder='big')
    
    # Read the message data
    msg_data = b''
    while len(msg_data) < msg_len:
        chunk = message(msg_len - len(msg_data))
        if not chunk:
            break
        msg_data += chunk
    
    # Parse the message as a JSON record
    record = json.loads(msg_data.decode())
    data_table.update(record)


# return data associated with the given name from the loaded dataset
def getbyname(name):
    if name in data_table:
        return data_table[name]
    else:
        return f"No data found for {name}"

# return data associated with the given location from the loaded dataset
def getbylocation(location):
    print(f'worker received request for getbyname')
    results = []
    for name, data in data_table.items():
        if data['location'] == location:
            results.append((name, data))
    if results:
        return dict(results)
    else:
        return f"No data found for {location}"

# return data associated with the given year & location from the loaded dataset
def getbyyear(location, year):
    results = []
    for name, data in data_table.items():
        if data['location'] == location and data['year'] == year:
            results.append((name, data))
    if results:
        return dict(results)
    else:
        return f"No data found for {location} in {year}"

# Main function
def main():
    if len(sys.argv) < 3:
        print('Usage: worker.py <port>')
        sys.exit(0)

    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Worker 1 listening on port {port}...")

    master_port = int(sys.argv[2])
    with xmlrpc.client.ServerProxy(f"http://localhost:{master_port}/") as proxy:
        print(f"Registering in master...")
        proxy.registerWorker('worker-1', 'http://localhost:' + str(port))
    
    # Registering the RPC functions defined in the scope
    print(f"Registering the functions...")
    server.register_function(getbyname, 'getbyname')
    server.register_function(getbylocation, 'getbylocation')
    server.register_function(getbyyear, 'getbyyear')
    server.register_function(processData, 'send')

    # Asynchronous server initialization - Server keeps waiting for the requests forever
    print(f"worker 1 is running")
    server.serve_forever()

if __name__ == '__main__':
    main()
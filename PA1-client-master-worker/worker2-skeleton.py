from xmlrpc.server import SimpleXMLRPCServer
import sys
import json
import numpy as np

# Storage of data
data_table = {}

# Loads the data based on group value and the portion it handles (am or nz)
def load_data(group):
    if group == "am" :
        label = "data-am.json"
    else:
        label = "data-nz.json"

    with open(label) as f:
            data_table.update(json.load(f))
            print(f'Loading the dataset {label}')

# return data associated with the given name from the loaded dataset
def getbyname(name):
    if name in data_table:
        return data_table[name]
    else:
        return []

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
        return np.empty((0,))

# return data associated with the given year & location from the loaded dataset
def getbyyear(location, year):
    results = []
    for name, data in data_table.items():
        if data['location'] == location and data['year'] == year:
            results.append((name, data))
    if results:
        return dict(results)
    else:
        return np.empty((0,))

# Main function
def main():
    if len(sys.argv) < 3:
        print('Usage: worker.py <port> <group: am or nz>')
        sys.exit(0)

    port = int(sys.argv[1])
    group = sys.argv[2]
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Worker 2 listening on port {port}...")
    load_data(group)
    
    # Registering the RPC functions defined in the scope
    server.register_function(getbyname, 'getbyname')
    server.register_function(getbylocation, 'getbylocation')
    server.register_function(getbyyear, 'getbyyear')

    # Asynchronous server initialization - Server keeps waiting for the requests forever
    server.serve_forever()

if __name__ == '__main__':
    main()
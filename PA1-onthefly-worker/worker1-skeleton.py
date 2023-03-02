from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import sys
import json

# Storage of data
data_table = {}

# Loads the data from json files
def load_data():
    datasets = ["data-am.json", "data-nz.json"]
    for label in datasets:
        with open(label) as f:
            data_table.update(json.load(f))
            print(f'Loading the dataset {label}')

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
    load_data()

    master_port = int(sys.argv[2])
    with xmlrpc.client.ServerProxy(f"http://localhost:{master_port}/") as proxy:
        print(f"Registering in master...")
        proxy.registerWorker('worker-1', 'http://localhost:' + str(port))
    
    # Registering the RPC functions defined in the scope
    print(f"Registering the functions...")
    server.register_function(getbyname, 'getbyname')
    server.register_function(getbylocation, 'getbylocation')
    server.register_function(getbyyear, 'getbyyear')

    # Asynchronous server initialization - Server keeps waiting for the requests forever
    print(f"worker 1 is running")
    server.serve_forever()

if __name__ == '__main__':
    main()
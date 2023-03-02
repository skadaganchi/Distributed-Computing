from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys

# Workers list
workers = {}
current_index = 0 # Initialize the index to 0

# Registering workers
def registerWorker(name, address):
    print(f"Worker {name} at {address} registered.")
    workers[name] = ServerProxy(address)
    return "successful registration"

# Selecting workers from the pool in sequential manner
def getWorker():
    print(f"no of workers available in master is {len(workers)}")
    if not workers:
        return None
    global current_index
    worker_list = list(workers.values())
    worker_count = len(worker_list)
    if current_index >= worker_count:
        current_index = 0
    worker = worker_list[current_index]
    current_index += 1
    return worker


def getbyname(name):
    print(f'Master received request for getByName: {name}')
    worker = getWorker()
    print(f'got the worker')

    try:
        print(f'Sending getbyname request to {worker}')
        return worker.getbyname(name)
    except:
        return f"No data found for {name}"

      
# Get the data based on location provided from a worker
def getbylocation(location):
    print(f'Master received request for getbylocation')
    locationData = []
    worker = getWorker()
    
    print(f'Sending getbylocation request to {worker}')
    try:
        locationData = worker.getbylocation(location)
    except:
        print(f'{worker} is down')

    if locationData:
        return locationData
    else:
        return f"No data found for {location}"


def getbyyear(location, year):
    print(f'Master received request for getbyyear')
    resultData = []

    worker = getWorker()
    try:
        print(f'Sending getbyyear request to {worker}')
        resultData = worker.getbyyear(location, year)
    except:
        print(f'{worker} is down')

    if resultData:
        return resultData
    else:
        return f"No data found for {location} with {year}"

def main():
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Master is listening on port {port}...")

    # Registering the RPC functions defined in scope
    print(f"Registering the functions...")
    server.register_function(registerWorker, "registerWorker")
    server.register_function(getbyname, "getbyname")
    server.register_function(getbylocation, "getbylocation")
    server.register_function(getbyyear, 'getbyyear')
    
    # Asynchronous server initialization - Server keeps waiting for the requests forever
    server.serve_forever()

if __name__ == '__main__':
    main()
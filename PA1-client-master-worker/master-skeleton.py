from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import numpy as np

# Registering workers with master
# This is hardcoded registering
workers = {
    'worker-1': ServerProxy("http://localhost:23001/"),
    'worker-2': ServerProxy("http://localhost:23002/")
}

# Get the data based on name from appropriate workers
def getbyname(name):
    print(f'Master received request for getByName: {name}')
    if name[0] < 'n':
        workerName = 'worker-1'
    else:
        workerName = 'worker-2'
    
    # If worker fails
    # Logging that the server is failed
    try:
        print(f'Sending getbyname request to {workerName}')
        return workers[workerName].getbyname(name)
    except:
        return f"No data found for {name}"

      
# Get the data based on location provided from both the workers
def getbylocation(location):
    print(f'Master received request for getbylocation')
    locationData = []
    
    # Looping through all servers to get complete location data
    for worker in workers:
        print(f'Sending getbylocation request to {worker}')
        # If worker fails
        # Logging that the server is failed
        try:
            data = workers[worker].getbylocation(location)
            if data:
                locationData.append(data)
        except:
            print(f'{worker} is down')

    # If result set is empty, returning no data found string
    if locationData:
        combinedData = {}
        for data in locationData:
            combinedData.update(data)
        return combinedData
    else:
        return f"No data found for {location}"

# Get the data based on location provided from both the workers
# Appends the data based 
def getbyyear(location, year):
    print(f'Master received request for getbyyear')
    resultData = []

    for worker in workers:
        try:
            print(f'Sending getbyyear request to {worker}')
            resultData.append(workers[worker].getbyyear(location, year))
        except:
            print(f'{worker} is down')

    # If result set is empty, returning no data found string
    if resultData:
        combinedData = {}
        for data in resultData:
            combinedData.update(data)
        return combinedData
    else:
        return f"No data found for {location} with {year}"

def main():
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Master is listening on port {port}...")

    # Registering the RPC functions defined in scope
    server.register_function(getbyname, "getbyname")
    server.register_function(getbylocation, "getbylocation")
    server.register_function(getbyyear, 'getbyyear')
    
    # Asynchronous server initialization - Server keeps waiting for the requests forever
    server.serve_forever()

if __name__ == '__main__':
    main()
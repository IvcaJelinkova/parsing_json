#zadani: 
""" 
Vaším úkolem bude parsing přiložených dat. Jedná se o seznam lxc kontejnerů na testovacím serveru ve formátu JSON, 
výstupem by mělo být (pro každý kontejner): 
    name, cpu a memory usage, created_at, status a všechny přiřazené IP adresy. 
    Datumová pole převeďte na UTC timestamp.  
""" 

import datetime
import json
import re
import psycopg2

# Connect to postgres DB
conn = psycopg2.connect("dbname=parsing_json user=postgres password=heslo")

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
cur.execute('DROP TABLE Devices')

cur.execute('''
    CREATE TABLE Devices (
        device_id SERIAL NOT NULL PRIMARY KEY UNIQUE, 
        name TEXT UNIQUE, 
        CPU_usage INTEGER, 
        memory_usage INTEGER, 
        created_at VARCHAR(255),
        status TEXT,
        IPs TEXT
    )
''')

# Retrieve query results
#records = cur.fetchall()

"""Extract nested values from a JSON tree."""
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


# loading the file with data: 
fname = input('Enter file name: ')
if len(fname) < 1: 
    fname = 'sample-data.json'

# data preparation: 
with open(fname) as file: 
    str_data = file.read()
    json_data = json.loads(str_data)
    for data in json_data: 
        #print('memory usage: ', data['state']) # [0]['memory'][0]['usage'])
        #print(json_extract(data, 'memory'))
        name = data['name']
        status = data['status']
        created_at = data['created_at']
        print('name: ', data["name"])
        print('status: ', data['status'])
        print('created at: ', data['created_at'])
        for key, value in data.items(): 
            if key == 'state': 
                state_dict = (value)
                try: 
                    for key, value in state_dict.items(): 
                        if key == 'memory': 
                            memory_dict = value
                            #print('memory: ', memory_dict, type(memory_dict))
                            for key, value in memory_dict.items(): 
                                if key == 'usage': 
                                    memory_usage = value
                                    print('memory usage: ', memory_usage)
                        
                        elif key == 'cpu': 
                            cpu_dict = value
                            for key, value in cpu_dict.items(): 
                                if key == 'usage': 
                                    cpu_usage = value
                                    print('cpu usage: ', cpu_usage)

                        elif key == 'network': 
                            # extrakce IP like – "address" : "127.0.0.1",
                            network_str = str(value)
                            #print('network: ', network_str, type(network_str))
                            addresses = []
                            addresses = re.findall("address': '([0-9a-z:.]*)'", network_str)
                            print('IP: ', len(addresses), addresses)

                            
                except AttributeError: 
                    print('expanded_devices')
                except ValueError: 
                    print('jsem tu')

               

        print()
        print('typy: ', type(name), type(cpu_usage), type(memory_usage), type(created_at), type(status), type(addresses))
        cpu_usage = str(cpu_usage)
        memory_usage = str(memory_usage)
        addresses = str(addresses)
        print('typy: ', type(name), type(cpu_usage), type(memory_usage), type(created_at), type(status), type(addresses))

        print()
        # giving data to sql: 
        cur.executemany('''INSERT INTO Devices (name, CPU_usage, memory_usage, 
                        created_at, status, IPs) VALUES (%s, %s, %s, %s, %s, %s)''', (name, cpu_usage, memory_usage, 
                        created_at, status, addresses))
        conn.commit()


# display dtb
db_version = cur.fetchone()
print(db_version)


# close the communication with the postgreSQL
cur.close()
conn.close()



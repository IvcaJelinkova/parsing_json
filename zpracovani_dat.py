#zadani: 
""" 
Vaším úkolem bude parsing přiložených dat. Jedná se o seznam lxc kontejnerů na testovacím serveru ve formátu JSON, 
výstupem by mělo být (pro každý kontejner): 
    name, cpu a memory usage, created_at, status a všechny přiřazené IP adresy. 
    Datumová pole převeďte na UTC timestamp.  
""" 

import datetime
from datetime import timezone as TZ
import json
import re
import psycopg2


dt = datetime.datetime.now(TZ.utc)
print('dt: ', dt)
utc_time = dt.replace(tzinfo=TZ.utc)
print('utc_time: ', utc_time)
utc_timestamp = utc_time.timestamp()
print(utc_timestamp)



# Connect to postgres DB
conn = psycopg2.connect('dbname=parsing_json user=postgres password=heslo')

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
cur.execute('DROP TABLE Devices')

cur.execute('''
    CREATE TABLE Devices (
        device_id SERIAL PRIMARY KEY, 
        name TEXT UNIQUE, 
        CPU_usage INTEGER, 
        memory_usage INTEGER, 
        created_at TEXT,
        status TEXT,
        IPs TEXT
    )
''')


# loading the file with data: 
fname = input('Enter file name: ')
if len(fname) < 1: 
    fname = 'sample-data.json'

# data preparation: 
with open(fname) as file: 
    str_data = file.read()
    json_data = json.loads(str_data)
    id = 1
    for data in json_data: 
        name = data['name']
        status = data['status']
        created_at = data['created_at']
        print(f'{id}.)')
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
                #except ValueError: 
                #    print('jsem tu')

               
        print()
        # pretypovani kvuli exportu do databaze
        id = str(id)
        cpu_usage = str(cpu_usage)
        memory_usage = str(memory_usage)
        addresses = str(addresses)

        print()
        # giving data to sql: 
        cur.execute('INSERT INTO Devices (name, CPU_usage, memory_usage, created_at, status, IPs) VALUES (%s, %s, %s, %s, %s, %s)', (name, cpu_usage, memory_usage, created_at, status, addresses))
        conn.commit()
        id = int(id)
        id += 1


# display dtb
db_version = cur.fetchone()
print(db_version)


# close the communication with the postgreSQL
cur.close()
conn.close()



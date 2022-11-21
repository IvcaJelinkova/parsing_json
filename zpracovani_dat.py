#zadani: 
""" 
Vaším úkolem bude parsing přiložených dat. Jedná se o seznam lxc kontejnerů na testovacím serveru ve formátu JSON, 
výstupem by mělo být (pro každý kontejner): 
    name, cpu a memory usage, created_at, status a všechny přiřazené IP adresy. 
    Datumová pole převeďte na UTC timestamp.  
""" 

import json
import re
import psycopg2


def parsing_network(value): 
    """extracting IP like – "address" : "127.0.0.1"."""
    network_str = str(value)
    addresses = []
    addresses = re.findall("address': '([0-9a-z:.]*)'", network_str)
    return addresses


def parsing_memory_cpu(value): 
    """Extracting memory and cpu usage from JSON. """
    cpu_or_memory_dict = value
    for key, value in cpu_or_memory_dict.items(): 
            if key == 'usage': 
                return value



def add_device(data): 
    """Parsing JSON file to small pieces."""
    name = ''
    cpu_usage = 0
    memory_usage = 0
    created_at = '-'
    status = '-'
    addresses = []

    name = data['name']
    status = data['status']
    created_at = data['created_at']

    for key, value in data.items(): 
        if key == 'state': 
            state_dict = value
            try: 
                for key, value in state_dict.items(): 
                    if key == 'memory': 
                        memory_usage = parsing_memory_cpu(value)
                    
                    elif key == 'cpu': 
                        cpu_usage = parsing_memory_cpu(value)

                    elif key == 'network': 
                        addresses = parsing_network(value)

            except AttributeError: 
                print('expanded_device')
    return name, status, created_at, memory_usage, cpu_usage, addresses



def giving_data_to_sql(name, cpu_usage, memory_usage, created_at, status): 
    """Sending data to sql""" 
    cur.execute('''INSERT INTO Devices (name, CPU_usage, memory_usage, created_at, status) 
        VALUES (%s, %s, %s, %s, %s)''', (name, cpu_usage, memory_usage, created_at, status))
    cur.execute(f"SELECT id FROM Devices WHERE name = '{name}'")
    device_id = cur.fetchone()[0]

    for pocet_IP in range(len(addresses)): 
        address = addresses.pop()
        cur.execute('INSERT INTO IPs (device_id, IP) VALUES (%s, %s)', (device_id, address))

    conn.commit()
    return


# Connect to postgres DB
conn = psycopg2.connect('dbname=parsing_json user=postgres password=heslo')

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
cur.execute('DROP TABLE IF EXISTS IPs')
cur.execute('DROP TABLE IF EXISTS Devices')


cur.execute('''
    CREATE TABLE Devices (
        id SERIAL PRIMARY KEY, 
        name TEXT UNIQUE, 
        CPU_usage INTEGER, 
        memory_usage INTEGER, 
        created_at TIMESTAMP,
        status TEXT
    )
''')

cur.execute('''
    CREATE TABLE IPs (
        device_id INTEGER REFERENCES Devices (id), 
        IP TEXT
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
    for data in json_data: 
        name, status, created_at, memory_usage, cpu_usage, addresses = add_device(data)
        giving_data_to_sql(name, cpu_usage, memory_usage, created_at, status)


# close the communication with the postgreSQL
cur.close()
conn.close()


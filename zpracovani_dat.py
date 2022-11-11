#zadani: 
""" 
Vaším úkolem bude parsing přiložených dat. Jedná se o seznam lxc kontejnerů na testovacím serveru ve formátu JSON, 
výstupem by mělo být (pro každý kontejner): 
    name, cpu a memory usage, created_at, status a všechny přiřazené IP adresy. 
    Datumová pole převeďte na UTC timestamp.  
""" 

import json, re
import datetime


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
                            

                #print(state_dict)
                #for value in data['memory']: 
                #    usage = value['usage']
                #    print('memory usage: ', usage)
                #if key == 'memory': 
                 #   print(value)
                except AttributeError: 
                    print('expanded_devices')
                    pass
                """ 
                for key in data['state']: 
                    if key == 'memory': 
                        print(key)
                """ 
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 16:42:21 2020

@author: aovch
"""

import json
import tqdm
from clickhouse_driver import Client
import os

path = os.getcwd()

file = '\event-data.json'

with open(path+file,'r', encoding = 'utf-8') as fh:
    file_buffer = fh.read().split('\n')

columns = {'artist',
 'auth',
 'firstName',
 'gender',
 'itemInSession',
 'lastName',
 'length',
 'level',
 'location',
 'method',
 'page',
 'registration',
 'sessionId',
 'song',
 'status',
 'ts',
 'userAgent',
 'userId'}

fullfill = dict(zip(
    list(columns),
    [None for x in range(len(columns))]
    )
    )



##check for max columns

# for each in tqdm.tqdm(file_buffer):
#     if each:
#         line = json.loads(each)
#     else:
#         pass
#     if not set(line.keys()).issubset(columns):
#         if len(line.keys()) > len(columns):
#             columns = set(line.keys())

list_structure = []

def restructure(d):
    d = dict(sorted(d.items()))
    return tuple(d.values())

for each in tqdm.tqdm(file_buffer):
    if each:
        line = json.loads(each)
        fullfiled = { **fullfill,**line}
        list_structure.append(restructure(fullfiled))
    else:
        pass


client = Client(host='localhost')
client.execute('SHOW DATABASES')

string_cols = str(tuple(columns)).replace("'",'')

client.execute(
    'INSERT INTO Dbreport.RawData {}'.format(string_cols),
        list_structure
 )

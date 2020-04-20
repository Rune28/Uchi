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
print(path)

file = '/event-data.json'

with open(path+file,'r', encoding = 'utf-8') as fh:
    file_buffer = fh.read().split('\n')

columns = ['artist',
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
 'userId']

fullfill = dict(zip(
    list(columns),
    ['' for x in range(len(columns))]
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

client = Client(host='localhost')

string_cols = str(tuple(columns)).replace("'",'')


def restructure(d):
    d = dict(sorted(d.items()))
    return d


def tupling(d):
    d = dict(sorted(d.items()))
    return tuple(d.values())


for each in tqdm.tqdm([file_buffer[1]]):
    if each:
        line = json.loads(each)
        fullfiled = tupling({ **fullfill,**line})
        print(tuple([x.replace("'",'')  if isinstance(x,str) else x for x in fullfiled]))
        client.execute(
                        'INSERT INTO Dbreport.RawData {} VALUES'.format(string_cols),
                        tuple([x.replace("'",'')  if isinstance(x,str) else x for x in fullfiled])
                      )
    else:
        pass

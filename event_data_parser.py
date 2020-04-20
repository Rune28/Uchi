# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 16:42:21 2020

@author: aovch
"""

import json
import tqdm
from clickhouse_driver import Client
import os


#json path
path = os.getcwd()

file = '/event-data.json'

print(path+file)

with open(path+file,'r', encoding = 'utf-8') as fh:
    file_buffer = fh.read().split('\n')

print('Done')

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



###if column isnt in row utofill as blanks
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


client = Client(host='localhost')

string_cols = str(tuple(columns)).replace("'",'')

#create table script

# CREATE TABLE Dbreport.RawData (
#   artist  String,
#   auth  String,
#   firstName  String,
#   gender  String,
#   itemInSession  UInt8,
#   lastName  String,
#   length  Float32,
#   level  String,
#   location  String,
#   method  String,
#   page  String,
#   registration  String,
#   sessionId  Int32,
#   song  String,
#   status  UInt8,
#   ts  Int32,
#   userAgent  String,
#   userId  String
# ) ENGINE = MergeTree()
# ORDER BY ts



def tupling(d):
    d = dict(sorted(d.items()))
    return tuple(d.values())


for each in tqdm.tqdm(file_buffer):
    if each:
        line = json.loads(each)
        fullfiled = tupling({ **fullfill,**line})
        client.execute(
                        'INSERT INTO Dbreport.RawData {} VALUES'.format(string_cols),
                        fullfiled
                      )
    else:
        pass

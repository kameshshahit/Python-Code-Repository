# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 13:22:45 2019

@author: a08346
"""

# Read the JSON file
# Objcet Loading and cleaning 
# Normalise the Json file and convert into dict
# Each rEcord should be one dict object
# Load those objects into CSV and connect that to SQL Server

import datetime
now = datetime.datetime.now()
print ("date and time : " + now.strftime("%Y%m%d"))
logfilename = 'log_'+ now.strftime("%Y%m%d")+'.txt'
f = open('//Lngovpfil02.production.local/data_gho_gwia_bb/Data_Analytics_Secure_File_Store/2019/Q1/Aladdin data/2_Fieldwork/1_Data/'+str(logfilename),'w')
f.write('\n' + 'Execution Starts')
f.write("date and time : " + now.strftime("%Y%m%d"))
f.flush()

#import tkinter
import json
import pandas as pd
import csv
import sqlite3
import pickle


import datetime
import sqlalchemy
from pandas.io.json import json_normalize

now = datetime.datetime.now()
print ("Object Load Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
#f.write('\n' + 'Object Load Start date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
#f.flush()
with open('201910418_jsonp1_gt_9.json', 'r', encoding="utf8") as myfile:
    data=myfile.read()
obj = json.loads(data)

#with open('jsonparser.pkl', 'wb') as f:
#   pickle.dump(obj, f)
#df = pd.DataFrame.from_dict(json_normalize(obj), orient='columns')
#with open('jsonparser.pkl', 'rb') as f:
#    mydisksavedlist = pickle.load(f)

# Transform json input to python objects
#input_dict = json.loads(input_json)
###############Pickling Ends
#
# Filter python objects with list comprehensions
now = datetime.datetime.now()
print ("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
output_dict = [x for i,x in enumerate(obj,1) if i >=900000] #run for top 10 also and validate
#
## Transform python object back into json
outp1 = "201910418_jsonp1_gt_9.json"
output_json = json.dumps(output_dict)
with open(outp1, 'w') as fp:
    json.dump(output_dict, fp)
now = datetime.datetime.now()
print ("end date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))


now = datetime.datetime.now()
print ("Object Load End date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
#f.write('\n' + 'Object Load End date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
#f.flush()


now = datetime.datetime.now()
print ("Recursion Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
f.write('\n' + 'Recursion Start date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
f.flush()

import sys
x=1000
sys.setrecursionlimit(x)

def unnest_json(y):
    output = {}

    def Make_Flat_Json(deflat_list_dict, attr_Name=''):
        if type(deflat_list_dict) is dict:
            for a in deflat_list_dict:
                #print(attr_Name,'--',deflat_list_dict[a],'--',a)
                #print('-----')
                Make_Flat_Json(deflat_list_dict[a], attr_Name + a + '.')
        elif type(deflat_list_dict) is list:
            i = 0
           # print(str(deflat_list_dict[i]['_tradeDateEpoch'])+str(deflat_list_dict[i]['_executionDateTimeEpoch']))
            #if str(deflat_list_dict[i]['_tradeDateEpoch'])+str(deflat_list_dict[i]['_executionDateTimeEpoch']) == '15420672000001542121312000':
            for a in deflat_list_dict:
                #print(a)
                #print(i)
                #print('In list',attr_Name,'--')
                Make_Flat_Json(a, attr_Name + str(i) + '.')
                #print(str(a['_tradeDateEpoch']))
                i += 1
        else:
            output[attr_Name[:-1]] = deflat_list_dict

    Make_Flat_Json(y)
    return output

val1 = unnest_json(obj)

now = datetime.datetime.now()
print ("Recursion End date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
f.write('\n' + 'Recursion End date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
f.flush()

now = datetime.datetime.now()
print ("SQLite Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
f.write('\n' + 'SQLite Start date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
f.flush()

import sqlite3
conn = sqlite3.connect("tradedb6.sqlite")
#conn.execute('pragma journal_mode=wal')
conn.commit()
c = conn.cursor()
#c.execute("create table record_normal (Recordno integer, ColumnName varchar ,Data varchar)")
#c.execute('''drop table normalised_d;''')
c.execute('''create table normalised_d (seqno BIGINT ,Recordno BIGINT, ColumnName TEXT,Data TEXT)''')
i=0
for key, value in val1.items():
    i+=1
    c.execute('insert into normalised_d values (?,?,?,?)',[i,key.split(".",1)[0], key.split(".",1)[1], value])
    
for row in c.execute("select max(recordno) from  normalised_d limit 5"):
    print(row)
conn.commit()    
now = datetime.datetime.now()
print ("SQLite End date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
f.write('\n' + 'SQLite End date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
f.flush()

Startcount=360001
endcount=400000
step=40000
for i in range(9,12):
    now = datetime.datetime.now()
    tablename="20191120_tablename_"+str(i)
    print(tablename + " table processing starts.")
    #f.write('\n' + tablename + ' table processing starts. ' + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.flush()
    print ("Pivoting Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.write('\n' + 'Pivoting Start date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.flush()
    sql_stmt = "select * from normalised_d where Cast(Recordno as BIGINT) between "+ str(Startcount) +" and "+ str(endcount) +"    and ColumnName not like '%TRDREL_record%';"
    #df_final = pd.read_sql_query("select * from normalised_d where Cast(Recordno as BIGINT) between 50001 and   80000;",conn)
    df_final = pd.read_sql_query(sql_stmt,conn)
    table = df_final.pivot(index='Recordno',columns='ColumnName',values='Data')  
    now = datetime.datetime.now()
    print ("Pivoting End date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.write('\n' + 'Pivoting End date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.flush()
    now = datetime.datetime.now()
    print ("SQL Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.write('\n' + 'SQL Start date and time : ' + now.strftime("%Y-%m-%d %H:%M:%S"))
   # f.flush()
    #tablename="20190517_normalised_trades_"+str(i)
    engine = sqlalchemy.create_engine('mssql+pyodbc://<ServerName>/<DBName>?trusted_connection=yes&Driver={SQL Server Native Client 11.0}')
    table.to_sql(name=tablename, schema = 'aladdin',con=engine, index=False)
    now = datetime.datetime.now()
    print ("SQL End date and time : "+now.strftime("%Y-%m-%d %H:%M:%S"))

    
    print(str(Startcount) + '-' +str(endcount)+" records processed in table "+ tablename)
    #f.write('\n' + str(Startcount) + '-' +str(endcount)+' records processed in table '+ tablename+ '  '+  now.strftime("%Y-%m-%d %H:%M:%S"))
    #f.flush()
    Startcount=Startcount+step
    endcount=endcount+step
    print("---------------------------------------------------------")
   # f.write('\n' + '----------Batch Completed - ' + now.strftime("%Y-%m-%d %H:%M:%S"))
   # f.flush()
f.close()

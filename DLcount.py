import os
import sys
import subprocess
import ConfigParser
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings 
import pandas as pd
import numpy as np
import csv
import shutil
import sys



def dlcount():    
  
    hdfsFilePath = 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/%s' %landingAreaFolder
    cmd = 'hdfs dfs -find {} -name *.csv'.format(hdfsFilePath).split()
    files = subprocess.check_output(cmd).strip().split('\n')
    for path in files:
        id_list=[]
        dlcount=''
        filename = path.split(os.path.sep)[-1].split('.csv')[0]
        filenameWithExt= filename+".csv"
        filenameWithoutExt = filename
        table_name=filenameWithoutExt[:-11]
        table_name_wod=table_name.replace('.','_')
        countQuery= 'select count(*) from '+rawHiveDbName+'.'+table_name_wod.lower()
        countBeelineCommand = """beeline -u jdbc:hive2://10.21.1.14:10500 hive hive123 -e '%s'""" %countQuery
        print "\n" +countQuery
        line = os.popen(countBeelineCommand,"r")
        for i in line:
            id_list.append(i)
        print 'Before popping the data' +str(id_list)
        if len(id_list)!=0:
           id_list.pop(0)
           id_list.pop(0)
           id_list.pop(0)
           id_list.pop(-1)
        print id_list  #['| 518  |\n']
        for j in id_list:
              firststring=j.replace('|\n','')
              dlcount=firststring.replace('|','')
        
        print 'The final count is '+ str(dlcount)
        
        #result = line.readline()
        #print 'Count in datalake is :' +result

if __name__ == "__main__":

    #global id_list
    #id_list=[]
    global rawHiveDbName
    rawHiveDbName='test_db'
    global landingAreaFolder
    landingAreaFolder='user/script_temp/'
    global localfilename
    localfilename='/home/darshana/dotnet/dotnet_temp.csv'
    global block_blob_service
    global list_blob
    list_blob=[]
    global list_blob_fin
    list_blob_fin=[]
    #listblob_output=list_blobs('strpaplprod','Tp1Y8PsrZpVzwHMcooYacnTZKGOE7JGrFyLpPga7IAQwncUOmKyxN9zf/4g8UoaQiV58fZ7jkQnao0HbN0CpWA==','str-papl-prod')
    #refine_blobs(list_blob,listblob_output,localfilename)
    dlcount()
    print 'Hey its not working '
       
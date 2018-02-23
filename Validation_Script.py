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


def list_blobs(accountname,accountkey,container):
    block_blob_service = BlockBlobService(account_name=accountname, account_key=accountkey)
    generator = block_blob_service.list_blobs(container)
    #block_blob_service.get_blob_to_path(container,BLOBNAME,LOCALFILENAME)
    for blob in generator:
        list_blob.append(blob.name)

    return block_blob_service


def refine_blobs(list_blob,listblob_output,localfilename):

    fields = []
    rows = []
    csv.field_size_limit(sys.maxsize)
    for portal_name in list_blob:
        #print portal_name
        if portal_name.find("user/script_temp/DotNet.") != -1 and portal_name.find(".csv") != -1:
            print portal_name
            tab_name=portal_name.split('/')[-2]
            list_blob_fin.append(tab_name)
            print tab_name
            listblob_output.get_blob_to_path('str-papl-prod',portal_name,localfilename)
            with open(localfilename,'rU') as count_file:
                 csvreader=csv.reader(count_file)
                 fields = csvreader.next()
                 for row in csvreader:
                     rows.append(row)
                 print("Table count of "+str(tab_name)+" is : %d"%((csvreader.line_num)-1))
            #data=list(reader)
            #rowcountblob=len(data)
            #rowcountblob=len(reader.index)
            #print ' Count of table ' +str(tab_name)+ ' is: '+ str(rowcountblob) 
            shutil.rmtree(localfilename,ignore_errors=True)


    hdfsFilePath = 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/%s' %landingAreaFolder
    cmd = 'hdfs dfs -find {} -name *.csv'.format(hdfsFilePath).split()
    files = subprocess.check_output(cmd).strip().split('\n')
    for path in files:
        filename = path.split(os.path.sep)[-1].split('.csv')[0]
        filenameWithExt= filename+".csv"
        filenameWithoutExt = filename
        table_name=filenameWithoutExt[:-11]
        table_name_wod=table_name.replace('.','_')
        countQuery= 'select count(*) from '+rawHiveDbName+'.'+table_name_wod
        countBeelineCommand = """beeline -u jdbc:hive2://10.21.1.14:10500 hive hive123 -e '%s'""" %countQuery
        line = os.popen(countBeelineCommand,"r")
        for i in line:
            id_list.append(i) 

        result = line.readline()
        print 'Count in datalake is :' +result

if __name__ == "__main__":

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
  
    listblob_output=list_blobs('strpaplprod','Tp1Y8PsrZpVzwHMcooYacnTZKGOE7JGrFyLpPga7IAQwncUOmKyxN9zf/4g8UoaQiV58fZ7jkQnao0HbN0CpWA==','str-papl-prod')
    refine_blobs(list_blob,listblob_output,localfilename)
    print 'Hey its not working '
       
#Import required libraries

import os
import subprocess
import ConfigParser
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings 


#Get the blob name from the container using list_blobs function
def list_blobs(accountname,accountkey,container):
    block_blob_service = BlockBlobService(account_name=accountname, account_key=accountkey)
    generator = block_blob_service.list_blobs(container)
    #list_blob=[]
    for blob in generator:
        #print(blob.name)
        list_blob.append(blob.name)


def refine_blobs(list_blob):
    #list_blob_fin=[]
	
    for portal_name in list_blob:
        if portal_name.find("user/script_v1/DotNet.") != -1 and portal_name.find(".csv") != -1:
            print portal_name
            tab_name=portal_name.split('/')[-2]
            #print "Table name " +tab_name
            list_blob_fin.append(tab_name)
            print tab_name
    
	
	#for i in list_blob_fin1:
		#print "Contents from blob "+i



# method to generate create and load scripts for multi-structure files type and execute ddls.

def createMultiFileTypeLoad(rawHiveDbName,landingAreaFolder,tempStagingFolder,fileFormat,FileKey,delimiter,serde,skipHeader):

 hdfsFilePath = 'wasb://str-papl@strpapl.blob.core.windows.net/%s' %landingAreaFolder
 
 
 
 cmd = 'hdfs dfs -find {} -name *.csv'.format(hdfsFilePath).split()
 files = subprocess.check_output(cmd).strip().split('\n')
 for path in files:
   filename = path.split(os.path.sep)[-1].split('.csv')[0]
   filenameWithExt= filename+".csv"
   filenameWithoutExt = filename
   #table_name=filenameWithoutExt.split('_')[0] --modified for accomodating this -> DotNet.Legal.ER_mstTeam_2017_12_14
   table_name=filenameWithoutExt[:-11]
   table_name_wod=table_name.replace('.','_')
   
   #print table_name_wod
   
   #Moving source file to temporary staging path
   
   hdfsFilePath="wasb://str-papl@strpapl.blob.core.windows.net/"+landingAreaFolder+"/%s" % filenameWithExt
   out_path = "wasb://str-papl@strpapl.blob.core.windows.net/"+tempStagingFolder+"/"
   
   cmd = 'hdfs dfs -mkdir -p '+out_path
   #print cmd
   line = os.popen(cmd,"r")
   a = line.readline()
   
   cmd = 'hdfs dfs -mv '+ hdfsFilePath+' '+out_path
   #print cmd
   line = os.popen(cmd,"r")
   a = line.readline()
   
   #Extracting the header of the file
   
   hdfsFilePath="wasb://str-papl@strpapl.blob.core.windows.net/"+tempStagingFolder+"/%s" % filenameWithExt
   cmd="hdfs dfs -cat %s | head -1" % hdfsFilePath
   line = os.popen(cmd,"r")
   header = line.readline()
   doubleQuotesRemoved= header.replace('"','')
   headerNextLineRemoved=  doubleQuotesRemoved.replace('\n','')
   content = headerNextLineRemoved.split(",")
   replaceDelimiterByDatatype = headerNextLineRemoved.replace(delimiter,' string , ').replace('/','').replace('|','').replace('_','')
   #print "\n" + replaceDelimiterByDatatype
   
   #Decide the serde to be used
   
   if serde.lower() == 'csv_serde' :
    #print "\n" +serde.lower()
    serdeString = ' ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde" WITH SERDEPROPERTIES ("separatorChar" = ",","quoteChar" = "\\"")'
   elif serde.lower() == 'multi_serde' :
    serdeString = ' row format serde "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe" with SERDEPROPERTIES ("field.delim"="'+delimiter+'") '
   else:
    serdeString = ' ROW FORMAT DELIMITED FIELDS TERMINATED BY "'+delimiter+'" '
    
   #Decide if the header needs to be skipped
   
   if skipHeader.lower() == 'yes' :
    skipHeaderString =  'tblproperties("skip.header.line.count"="1")'
   else :
    skipHeaderString = ''
    
   # Create Query Formation using the header and execution in beeline shell
   
   try: 
    #folder_name_refined=(folder_name.replace('.','_')).lower()
    cmd="hdfs dfs -rm -r hdfs://hdp-test-01:8020/apps/hive/warehouse/datahub/%s" % table_name_wod
    line = os.popen(cmd,"r")
    print cmd
    dropQuery= 'DROP TABLE IF EXISTS '+rawHiveDbName+'.'+table_name_wod
    createQuery= 'Create external table '+rawHiveDbName+'.'+table_name_wod+'('+replaceDelimiterByDatatype+' string )'+serdeString+skipHeaderString
    print "\n" +createQuery

  
    dropBeelineCommand = """beeline -u jdbc:hive2://10.21.1.11:10000 hive hive123 -e '%s'""" % dropQuery
    print "\n" +dropQuery
    createBeelineCommand = """beeline -u jdbc:hive2://10.21.1.11:10000 hive hive123 -e '%s'""" % createQuery
    
    line = os.popen(dropBeelineCommand,"r")
    result = line.readline()
    
    line = os.popen(createBeelineCommand,"r")
    result = line.readline()
   except:
    print "\n" +"Error :  Exception Occured Table Creation Failed!!!"
    
    
   # Load Query Formation and running and execution in beeline shell
   
   try:
    
    loadQuery = 'LOAD DATA INPATH "'+out_path+'//'+filenameWithExt+'" INTO TABLE  '+rawHiveDbName+'.'+table_name_wod 
    
    print "\n" +loadQuery
    
    loadBeelineCommand= """beeline -u jdbc:hive2://10.21.1.11:10000 hive hive123 -e '%s'""" % loadQuery
    
    line = os.popen(loadBeelineCommand,"r")
    
    result = line.readline()
   except:
    print "\n" +"Error :  Exception Occured Data Load Failed!!!"
   
if __name__ == "__main__":

  # Load the configuration file

  from ConfigParser import SafeConfigParser
  parser = SafeConfigParser()
  parser.read('/home/darshana/home/simple.ini')
  
  # Retrieve the values from conf file 
  
  #jdbcUrl = parser.get('conf_para','beelineJdbcUrl')
  #userName = parser.get('conf_para','username')
  #passWord = parser.get('conf_para','password')
  rawHiveDbName = parser.get('conf_para','rawHiveDbName')
  landingAreaFolder = parser.get('conf_para','landingAreaFolder')
  tempStagingFolder = parser.get('conf_para','tempStagingFolder')
  fileFormat = parser.get('conf_para','fileFormat')
  FileKey = parser.get('conf_para','FileKey')
  delimiter = parser.get('conf_para','delimiter')
  serde = parser.get('conf_para','serde')
  skipHeader = parser.get('conf_para','skipHeader')
  
  global list_blob
  list_blob=[]
  global list_blob_fin
  list_blob_fin=[]
  #global list_blob_fin1
  #list_blob_fin1=[]
  #Calling list blob
  list_blobs('strpapl','9Bp20nOwje07C5FeGt7fp3zOOP6zaKKi+08MRLalSmhBgCe0RLkO0a4yiHe9EEwZlxzel06/Y/j8OhsWDzU+kQ==','str-papl')
  # method to generate create and load scripts for multi-structure files type and execute ddls.

  #Calling refine blob function
  refine_blobs(list_blob)
  
  #list_blob_fin1=list_blob_fin[1:]
  #print "List length earlier" + str(len(list_blob_fin))
  #print "Length of list after removal of script" + str(len(list_blob_fin1))
  
  
  #Invoking Method:- 
  for folder_name in list_blob_fin:
    landingAreaFolder1=landingAreaFolder+folder_name
    tempStagingFolder1=tempStagingFolder+folder_name
    #print "\n" +folder_name
	#print "\n" +landingAreaFolder1
    	
    createMultiFileTypeLoad(rawHiveDbName,landingAreaFolder1,tempStagingFolder1,fileFormat,FileKey,delimiter,serde,skipHeader)
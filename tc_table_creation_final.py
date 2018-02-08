import pandas as pd
import os
import subprocess
import ConfigParser
import sys
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings 
from ConfigParser import SafeConfigParser


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
        if portal_name.find("user/script_v1/DotNet.ConsumerComplaints.") != -1 and portal_name.find(".csv") != -1:
            #print portal_name
            tab_name=portal_name.split('/')[-2]
            #print "Table name " +tab_name
            list_blob_fin.append(tab_name)
            #print tab_name

def read_file(filename):
	
    df=pd.read_csv(filename)
    print list(df.columns.values)
    columns = df['Column_Name'].tolist()
    table_name= df['Table_Name'].tolist()
    data_type= df['Data_Type'].tolist()
    lower_data_type=[i.lower() for i in data_type]
    print lower_data_type
    return df, lower_data_type
	
def get_datatypes(lower_data_type):
    for i in lower_data_type:
        if i == 'varchar':
            refined_data_type.append('string')
        elif i == 'bigint':
            refined_data_type.append('bigint')
        elif i == 'numeric':
            refined_data_type.append('double')
        elif i== 'datetime':
            refined_data_type.append('timestamp')
        elif i== 'int':
            refined_data_type.append('int')
        elif i== 'tinyint':
            refined_data_type.append('int')
        elif i== 'decimal':
            refined_data_type.append('double')
        elif i== 'tinyblob':
            refined_data_type.append('string')
        elif i== 'char':
            refined_data_type.append('string')
        elif i== 'date':
            refined_data_type.append('date')
        elif i== 'timestamp':
            refined_data_type.append('timestamp')
        elif i== 'bit':
            refined_data_type.append('string')
        elif i== 'blob':
            refined_data_type.append('string')
        elif i== 'float':
            refined_data_type.append('double')
        elif i== 'text':
            refined_data_type.append('string')
        elif i== 'mediumblob':
            refined_data_type.append('string')
        elif i== 'longblob':
            refined_data_type.append('string')
        elif i== 'set':
            refined_data_type.append('string')
        elif i== 'nvarchar':
            refined_data_type.append('string')
        elif i== 'mediumtext':
            refined_data_type.append('string')
        elif i== 'smallint':
            refined_data_type.append('string')
        elif i== 'time':
            refined_data_type.append('string')
        else:
            refined_data_type.append('string')
    return refined_data_type

   
   
def createMultiFileTypeLoad(rawHiveDbName,landingAreaFolder,tempStagingFolder,fileFormat,FileKey,delimiter,serde,skipHeader,res):
    query_input=[]
    replaceDelimiterByDatatype=[]
    hdfsFilePath = 'wasb://str-papl-prod@strpapl.blob.core.windows.net/%s' %landingAreaFolder 
    cmd = 'hdfs dfs -find {} -name *.csv'.format(hdfsFilePath).split()
    files = subprocess.check_output(cmd).strip().split('\n')
    for path in files:
        filename = path.split(os.path.sep)[-1].split('.csv')[0]
        filenameWithExt= filename+".csv"
        filenameWithoutExt = filename
       #table_name=filenameWithoutExt.split('_')[0] --modified for accomodating this -> DotNet.Legal.ER_mstTeam_2017_12_14
        table_name=filenameWithoutExt[:-11]
        #The next command should be based on the tables names in each portal. Edit accordingly.
        just_table_name=table_name.split('.')[-1]
        table_name_wod=table_name.replace('.','_')
    
   #Moving source file to temporary staging path
   
        hdfsFilePath="wasb://str-papl-prod@strpapl.blob.core.windows.net/"+landingAreaFolder+"/%s" % filenameWithExt
   #out_path = "wasb://str-papl@strpapl.blob.core.windows.net/"+tempStagingFolder+"/"
   
   #cmd = 'hdfs dfs -mkdir -p '+out_path
   #print cmd
   #line = os.popen(cmd,"r")
   #a = line.readline()
   
   #cmd = 'hdfs dfs -mv '+ hdfsFilePath+' '+out_path
   #print cmd
   #line = os.popen(cmd,"r")
   #a = line.readline()
   
   #Extracting the header of the file
   
   #hdfsFilePath="wasb://str-papl@strpapl.blob.core.windows.net/"+tempStagingFolder+"/%s" % filenameWithExt
        cmd="hdfs dfs -cat %s | head -1" % hdfsFilePath
        line = os.popen(cmd,"r")
        header = line.readline()
        doubleQuotesRemoved= header.replace('"','')
        headerNextLineRemoved=  doubleQuotesRemoved.replace('\n','').replace('\r','')
        content = headerNextLineRemoved.split(",")
        print just_table_name
   
   #table_name_list=['mstCategory','mstCCMenu','mstCCWorkFlow','mstFlowType','mstPlantType','mstProduct','mstProductType','mstResponseType','mstRights','mstRole','mstSKU','mstSourceOfComplaint','mstStatus','mstSubCategory','mstUser','trnCCAttachments','trnCCDetails','trnCCFlow','trnSentEmails','trnUserLoginLog','trnUserRoleLocationMapping']
   
   #for j in table_name_list:
        df_filtered = res[(res.Table_Name == just_table_name)]
        flag='0'
        for i in content:
            for index, row in df_filtered.iterrows():
                if row['Column_Name'] == i :
                    interim=row['Column_Name']+" "+row['Refined_Data_Type']+","
                    query_input.append(interim)
                    flag='1'
            if flag != '1':
                interim=str(i)+"  string,"
                query_input.append(interim)
           
   
        query_input.append(query_input[-1].replace(',',''))
        query_input.pop(-2)
        print query_input
        replaceDelimiterByDatatype = "".join(query_input)
        print replaceDelimiterByDatatype
        #print len(replaceDelimiterByDatatype)
   
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
            #cmd="hdfs dfs -rm -r hdfs://hdp-test-01:8020/apps/hive/warehouse/datahub/%s" % table_name_wod
            #line = os.popen(cmd,"r")
            #print cmd
            dropQuery= 'DROP TABLE IF EXISTS '+rawHiveDbName+'.'+table_name_wod
            createQuery= 'Create external table '+rawHiveDbName+'.'+table_name_wod+'('+replaceDelimiterByDatatype+' )'+serdeString+skipHeaderString
            print "\n" +createQuery

  
            dropBeelineCommand = """beeline -u jdbc:hive2://10.21.1.11:10000  hive hive123 -e '%s'""" % dropQuery
            print "\n" +dropQuery
            createBeelineCommand = """beeline -u jdbc:hive2://10.21.1.11:10000  hive hive123 -e '%s'""" % createQuery
    
            line = os.popen(dropBeelineCommand,"r")
            result = line.readline()
    
            line = os.popen(createBeelineCommand,"r")
            result = line.readline()
        except:
            print "\n" +"Error :  Exception Occured Table Creation Failed!!!"
    
    
   # Load Query Formation and running and execution in beeline shell
   
if __name__ == '__main__':
	
	
  # Load the configuration file

  
  parser = SafeConfigParser()
  parser.read('/home/sonal/prod_simple.ini')
  
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
  
  global data_type
  data_type=[]
  global columns
  columns=[]
  global table_name
  table_name=[]
  global refined_data_type
  refined_data_type=[]
  global lower_data_type
  lower_data_type=[]
  global res
  global query_input
  #query_input=[]
  global df_filtered
  
	
	
  #global list_blob_fin1
  #list_blob_fin1=[]
  #Calling list blob
  list_blobs('strpapl','9Bp20nOwje07C5FeGt7fp3zOOP6zaKKi+08MRLalSmhBgCe0RLkO0a4yiHe9EEwZlxzel06/Y/j8OhsWDzU+kQ==','str-papl-prod')
  # method to generate create and load scripts for multi-structure files type and execute ddls.

  #Calling refine blob function
  refine_blobs(list_blob)
  
  #list_blob_fin1=list_blob_fin[1:]
  #print "List length earlier" + str(len(list_blob_fin))
  #print "Length of list after removal of script" + str(len(list_blob_fin1))
  
  res,lower=read_file('/home/sonal/ConsumerComplaints_Tabledetails.csv')		
  refined=get_datatypes(lower)
  print len(refined)
	
	#print res.head()
  res['Refined_Data_Type']=refined
  print res.head()
  
  #Invoking Method:- 
  for folder_name in list_blob_fin:
    landingAreaFolder1=landingAreaFolder+folder_name
    print landingAreaFolder1
    tempStagingFolder1=tempStagingFolder+folder_name
    #print "\n" +folder_name
	#print "\n" +landingAreaFolder1

    

    createMultiFileTypeLoad(rawHiveDbName,landingAreaFolder1,tempStagingFolder1,fileFormat,FileKey,delimiter,serde,skipHeader,res)
	
	
	
	#res = pd.dataframe()

		
	
	
	
#wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullAttendanceAPI_Bailley/
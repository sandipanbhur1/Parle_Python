#Packages to be imported
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
import os
import paramiko 
import shutil
from pathlib import Path
import subprocess

#Portal list - can be changed according to the new portals
portal_list=['HR_Assist','CMS']
#portal_list = ['BPS','ApprovalSystem','CapEx','CFAComplaint','ChangeRecord','ConsumerComplaints','ContractManpower'
#,'Cos','ITHelpDesk_New','Legal','PMS','POP','PPMS','ProductionSystem','QualityOperations','Scheme','SSales_Dev',
#'SSales_Horeca','SSales_MT','TravelDesk','VRS','WOSystem']

#Upload a file from local to blob
def upload_to_blob(accountname,accountkey,container,blobname,path_of_file_to_upload):
    block_blob_service = BlockBlobService(account_name=accountname, account_key=accountkey)
    block_blob_service.create_blob_from_path(container,blobname,path_of_file_to_upload,timeout=300)

#Removing the local folders	
def remove_folder(path):
    # check if folder exists
    if os.path.exists(path):
         # remove the contents of the folder if exists
        shutil.rmtree(path,ignore_errors=True)

		 
#def my_callback(filename, bytes_so_far, bytes_total):
    #print ('Transfer of %r is in progress' % filename) 

	
def get_sftp(ip,user_name,pass_word,remotepath,localpath):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip,22,username=user_name, password=pass_word)
    sftp = ssh.open_sftp()
    sftp.chdir(remotepath) #Change SFTP directory to (eg) /papl/csv_files/BPS/ (portal name is specified here as well)
    
	#For each filename in the Portal folder on sftp which starts with DotNet. , create localpath and remotepath variables which will 
	# contain the full path with filename on sftp and local respectively. 
    for filename in sorted(sftp.listdir()):
        if filename.startswith('DotNet.'):
            local_path = str(localpath)+str(filename)
            print local_path
            open(local_path,'a').close() #Create the local file in append mode
            remote_path=str(remotepath)+str(filename)
            print remote_path
            sftp.get(remote_path, local_path) #Transfer of file from sftp to local in the above declared path
            my_file = Path(local_path)
            if my_file.is_file(): #If local file present then  remove the date part from the file to form basename 
                #base_filename=filename.split('_')[0]
                base_filename=filename[:-14] 
                blob_name=base_filename+'/'+filename #This will be the blob name - folder/filename.csv is the new blob which we want to be created
                #Upload to blob function will create the blob which will have the csv files
                
                upload_to_blob('strpaplprod','Tp1Y8PsrZpVzwHMcooYacnTZKGOE7JGrFyLpPga7IAQwncUOmKyxN9zf/4g8UoaQiV58fZ7jkQnao0HbN0CpWA==','str-papl-prod/user/script_v1',blob_name,local_path)
				#upload_to_blob('strpapl','9Bp20nOwje07C5FeGt7fp3zOOP6zaKKi+08MRLalSmhBgCe0RLkO0a4yiHe9EEwZlxzel06/Y/j8OhsWDzU+kQ==','str-papl/user/script_v1',blob_name,local_path)
		
    sftp.close()
    ssh.close()
	

	
	#data = sftp.listdir_attr()
	#time_stamps = sorted([f.st_mtime for f in data])
	#result = time_stamps[-1]
	#print(result)

	#for file in data:
		#if file.st_mtime == result:
			#callback_for_filename = functools.partial(my_callback, file.filename)
			#sftp.get(file.filename, file.filename, callback=callback_for_filename)
	
#ssh.connect('168.62.234.4',22,'somanis','hj3er$EtqWb4AmCj') port no. should be integer not string
#localpath='/home/sonal/BPS/'
#remotepath='/papl/csv files/BPS/'

for portal in portal_list:
    portal_files_path='/papl/Sachin CSV IT/'+portal+'/' #SFTP path
    home_folder_path='/home/sonal/dotnet/'+portal+'/' #Local folder path
    if not os.path.exists(home_folder_path):
        os.makedirs(home_folder_path)
        
    get_sftp('168.62.234.4','somanis','hj3er$EtqWb4AmCj',portal_files_path,home_folder_path)
    remove_folder(home_folder_path)


#upload_to_blob('strpapl','9Bp20nOwje07C5FeGt7fp3zOOP6zaKKi+08MRLalSmhBgCe0RLkO0a4yiHe9EEwZlxzel06/Y/j8OhsWDzU+kQ==','str-papl','/user/sonal/test',local_path)

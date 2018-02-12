hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullAttendanceAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullAttendanceAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullAttendanceAPI_Bailley.csv /user/antuit_user/staging/SFA_PullAttendanceAPI_Bailley


CREATE EXTERNAL TABLE test_db.sfa_pullattendanceapi_bailley(
autoid string,
attendanceid string,
user_id string,
username string,
headquarter_id string,
headquartername string,
leavetype_id string,
leavetypename string,
presenttype_id string,
presenttypename string,
exceptionreason_id string,
exceptionreasonname string,
fordate string,
picturelocation string,
status string,
logtime string,
created string,
modified string,
approvalstatus string,
final string,
markedbyuser_id string,
rejectedreason_id string,
rejectedreasonname string,
createddate string,
createddate_dd string,
ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullAttendanceAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");




CREATE EXTERNAL TABLE ant_prod.SFA_PullAttendanceAPI_Bailley(
Autoid int
,Attendanceid int
,user_id int
,userName string
,headquarter_id int
,headquarterName string
,leavetype_id int
,leavetypeName string
,presenttype_id int
,presenttypeName string
,exceptionreason_id int
,exceptionreasonName string
,fordate string
,picturelocation string
,status string
,logtime string
,created string
,modified string
,approvalstatus string
,final int
,markedbyuser_id int
,rejectedreason_id int
,rejectedreasonName string
,CreatedDate string
,CreatedDate_DD string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullAttendanceAPI_Bailley/';





INSERT INTO ant_prod.SFA_PullAttendanceAPI_Bailley select
Autoid
,Attendanceid
,user_id
,userName
,headquarter_id
,headquarterName
,leavetype_id
,leavetypeName
,presenttype_id
,presenttypeName
,exceptionreason_id
,exceptionreasonName
,fordate
,picturelocation
,status
,logtime
,created
,modified
,approvalstatus
,final
,markedbyuser_id
,rejectedreason_id
,rejectedreasonName
,CreatedDate
,CreatedDate_DD
,current_timestamp as ingestedtime from test_db.SFA_PullAttendanceAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ __ _ _ __ __ _ _ _ _ __ _  _ _ _ _ __ _ __ __ _ _ _ _ _  _ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullCallsAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullCallsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullCallsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullCallsAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullCallsAPI_Bailley.csv /user/antuit_user/staging/SFA_PullCallsAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullCallsAPI_Bailley(
auto_id string
,callid string
,outlet_id string
,callbeatname string
,user_id string
,transactionid string
,fordate string
,pjpoutlet string
,payment_id string
,order_id string
,is_stock_taken string
,activity_id string
,is_salesreturn_taken string
,created string
,modified string
,end_time string
,callbeat_id string
,beatjump_id string
,start_time string
,on_location string
,latitude string
,longitude string
,workflowreason_id string
,workflowreason string
,outletname string
,outleterp_id string
,warehousename string
,areaname string
,warehouseerpid string
,type string
,outletcategory_id string
,beatname string
,beatid string
,userid string
,username string
,useremployeeid string
,headquartername string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullCallsAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullCallsAPI_Bailley(
auto_id int
,callid int
,outlet_id int
,callbeatname string
,user_id int
,transactionid string
,fordate string
,pjpoutlet int
,payment_id int
,order_id int
,is_stock_taken int
,activity_id int
,is_salesreturn_taken int
,created string
,modified string
,end_time string
,callbeat_id int
,beatjump_id int
,start_time string
,on_location int
,latitude string
,longitude string
,workflowreason_id int
,workflowreason string
,outletname string
,outleterp_id int
,warehousename string
,areaname string
,warehouseerpid string
,type string
,outletcategory_id int
,beatname string
,beatid int
,userid int
,username string
,useremployeeid string
,headquartername string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullCallsAPI_Bailley/';


INSERT INTO ant_prod.SFA_PullCallsAPI_Bailley select
auto_id
,callid
,outlet_id
,callbeatname
,user_id
,transactionid
,fordate
,pjpoutlet
,payment_id
,order_id
,is_stock_taken
,activity_id
,is_salesreturn_taken
,created
,modified
,end_time
,callbeat_id
,beatjump_id
,start_time
,on_location
,latitude
,longitude
,workflowreason_id
,workflowreason
,outletname
,outleterp_id
,warehousename
,areaname
,warehouseerpid
,type
,outletcategory_id
,beatname
,beatid
,userid
,username
,useremployeeid
,headquartername
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullCallsAPI_Bailley;

__ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ __  _  ___ _ _ _ _ _ _ __ _ _ _ _ _  __ _  __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullCPMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullCPMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullCPMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullCPMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullCPMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullCPMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullCPMasterAPI_Bailley(
auto_id string
,zoneid string
,zonename string
,subzoneid string
,subzonename string
,warehouseid string
,warehousename string
,warehouse_erpid string
,address string
,isactive string
,phoneno string
,taxno string
,email string
,pin string
,tnc string
,jurisdiction string
,cstno string
,areapincode string
,warehousetype string
,modified string
,created string
,businesstypename string
,popsstrataname string
,state string
,city string
,district string
,result string
,reason string
,response string
,createddate string
,createddate_dd string
,ingestedtime string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullCPMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullCPMasterAPI_Bailley(
auto_id int
,zoneid int
,zonename string
,subzoneid int
,subzonename string
,warehouseid int
,warehousename string
,warehouse_erpid  string
,address string
,isactive string
,phoneno int
,taxno string
,email string
,pin int
,tnc int
,jurisdiction string
,cstno int
,areapincode int
,warehousetype string
,modified string
,created string
,businesstypename string
,popsstrataname string
,state string
,city string
,district string
,result int
,reason string
,response int
,createddate string
,createddate_dd string
,ingestedtime string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullCPMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullCPMasterAPI_Bailley select
auto_id
,zoneid
,zonename
,subzoneid
,subzonename
,warehouseid
,warehousename
,warehouse_erpid
,address
,isactive
,phoneno
,taxno
,email
,pin
,tnc
,jurisdiction
,cstno
,areapincode
,warehousetype
,modified
,created
,businesstypename
,popsstrataname
,state
,city
,district
,result
,reason
,response
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullCPMasterAPI_Bailley;


_ _ __ _ _ _ _ _ __ _ _ _ _ _ __ _ _ _ _ __ _  _ _ _ _ _ _ _ __ _ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullListOrdersAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullListOrdersAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullListOrdersAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullListOrdersAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullListOrdersAPI_Bailley.csv /user/antuit_user/staging/SFA_PullListOrdersAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullListOrdersAPI_Bailley(
auto_id string
,id string
,transactionid string
,amount string
,pjpoutlet string
,discount string
,schemediscount string
,picturelocation string
,outlet_id string
,latitude string
,longitude string
,fordate string
,comment string
,created string
,modified string
,user_id string
,orderstate_id string
,beat_id string
,beat_name string
,areaname string
,warehousename string
,warehouseerpid string
,erp_id string
,type string
,warehouseid string
,area_id string
,username string
,userid string
,username1 string
,employeeid string
,orderstatename string
,headquartername string
,outletcategoryname string
,start_time string
,end_time string
,skuunit_id string
,quantity string
,unitprice string
,order_detailamount string
,order_id1 string
,order_detailid string
,order_detaildiscount string
,schemediscount1 string
,skucode string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullListOrdersAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullListOrdersAPI_Bailley(
auto_id int
,id int
,transactionid string
,amount int
,pjpoutlet int
,discount int
,schemediscount int
,picturelocation string
,outlet_id int
,latitude string
,longitude string
,fordate string
,comment string
,created string
,modified string
,user_id int
,orderstate_id int
,beat_id int
,beat_name string
,areaname string
,warehousename string
,warehouseerpid string
,erp_id string
,type string
,warehouseid int
,area_id int
,username string
,userid int
,username1 string
,employeeid string
,orderstatename string
,headquartername string
,outletcategoryname string
,start_time string
,end_time string
,skuunit_id int
,quantity int
,unitprice string
,order_detailamount int
,order_id1 int
,order_detailid int
,order_detaildiscount int
,schemediscount1 int
,skucode string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullListOrdersAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullListOrdersAPI_Bailley select
auto_id
,id
,transactionid
,amount
,pjpoutlet
,discount
,schemediscount
,picturelocation
,outlet_id
,latitude
,longitude
,fordate
,comment
,created
,modified
,user_id
,orderstate_id
,beat_id
,beat_name
,areaname
,warehousename
,warehouseerpid
,erp_id
,type
,warehouseid
,area_id
,username
,userid
,username1
,employeeid
,orderstatename
,headquartername
,outletcategoryname
,start_time
,end_time
,skuunit_id
,quantity
,unitprice
,order_detailamount
,order_id1
,order_detailid
,order_detaildiscount
,schemediscount1
,skucode
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullListOrdersAPI_Bailley;


_ __ _  _ _ _ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ __ _  _ _ _ _ _ _ _ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullPJPMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullPJPMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullPJPMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullPJPMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullPJPMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullPJPMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullPJPMasterAPI_Bailley(
auto_id string
,pjp_id string
,beat_id string
,user_id string
,fordate string
,created string
,modified string
,pjpstartdate string
,frequency string
,beatname string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullPJPMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullPJPMasterAPI_Bailley(
auto_id int
,pjp_id int
,beat_id int
,user_id int
,fordate  string
,created string
,modified string
,pjpstartdate string
,frequency int
,beatname string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullPJPMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullPJPMasterAPI_Bailley select
auto_id
,pjp_id
,beat_id
,user_id
,fordate
,created
,modified
,pjpstartdate
,frequency
,beatname
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullPJPMasterAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullSalesAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullSalesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullSalesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullSalesAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullSalesAPI_Bailley.csv /user/antuit_user/staging/SFA_PullSalesAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullSalesAPI_Bailley(
auto_id string
,paymentid string
,amount string
,invoiceid string
,order_id string
,outlet_id string
,user_id string
,fordate string
,mode string
,discount string
,grn string
,po string
,orderuser string
,schemediscount string
,custom_invoice string
,approval_status string
,created string
,modified string
,beat_id string
,beat_name string
,fromputlet_id string
,warehousename string
,warehouse_erpid string
,outletname string
,outletwarehousename string
,areaname string
,outletwarehouseerpid string
,type string
,outletcategory_id string
,area_id string
,userid string
,username string
,employeeid string
,username1 string
,headquartername string
,orderfordate string
,orderamount string
,ordercreated string
,ordermodified string
,outletcategoryname string
,paymentdetailid string
,unitprice string
,paymentdetailamount string
,quantity string
,paymentdetaildiscount string
,paymentdetailinvoiceid string
,paymentdetailskunit_id string
,paymentdetailcreated string
,paymentdetailmodified string
,vat string
,batch_id string
,paymentdetailschemediscount string
,is_free string
,for_skunit_id string
,skunit_id string
,skunit_name string
,skucode string
,mrp string
,company_id string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullSalesAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullSalesAPI_Bailley(
auto_id int
,paymentid int
,amount int
,invoiceid  string
,order_id string
,outlet_id string
,user_id int
,fordate string
,mode string
,discount string
,grn string
,po string
,orderuser string
,schemediscount int
,custom_invoice string
,approval_status string
,created string
,modified string
,beat_id int
,beat_name string
,fromputlet_id int
,warehousename string
,warehouse_erpid string
,outletname string
,outletwarehousename string
,areaname string
,outletwarehouseerpid string
,type string
,outletcategory_id int
,area_id int
,userid int
,username string
,employeeid string
,username1 string
,headquartername string
,orderfordate string
,orderamount int
,ordercreated string
,ordermodified string
,outletcategoryname string
,paymentdetailid int
,unitprice string
,paymentdetailamount int
,quantity int
,paymentdetaildiscount int
,paymentdetailinvoiceid int
,paymentdetailskunit_id int
,paymentdetailcreated string
,paymentdetailmodified string
,vat int
,batch_id int
,paymentdetailschemediscount int
,is_free string
,for_skunit_id string
,skunit_id int
,skunit_name string
,skucode string
,mrp int
,company_id int
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullSalesAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullSalesAPI_Bailley select
auto_id
,paymentid
,amount
,invoiceid
,order_id
,outlet_id
,user_id
,fordate
,mode
,discount
,grn
,po
,orderuser
,schemediscount
,custom_invoice
,approval_status
,created
,modified
,beat_id
,beat_name
,fromputlet_id
,warehousename
,warehouse_erpid
,outletname
,outletwarehousename
,areaname
,outletwarehouseerpid
,type
,outletcategory_id
,area_id
,userid
,username
,employeeid
,username1
,headquartername
,orderfordate
,orderamount
,ordercreated
,ordermodified
,outletcategoryname
,paymentdetailid
,unitprice
,paymentdetailamount
,quantity
,paymentdetaildiscount
,paymentdetailinvoiceid
,paymentdetailskunit_id
,paymentdetailcreated
,paymentdetailmodified
,vat
,batch_id
,paymentdetailschemediscount
,is_free
,for_skunit_id
,skunit_id
,skunit_name
,skucode
,mrp
,company_id
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullSalesAPI_Bailley;

__ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ __ _ _ _ _ __ _  _ _ __ _ _ _ _ __ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullStockatOutletAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullStockatOutletAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullStockatOutletAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullStockatOutletAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullStockatOutletAPI_Bailley.csv /user/antuit_user/staging/SFA_PullStockatOutletAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullStockatOutletAPI_Bailley(
auto_id string
,stockatoutletid string
,outlet_id string
,user_id string
,transactionid string
,fordate string
,skunit_id string
,availablequantity string
,comment string
,cstock string
,cprice string
,created string
,modified string
,valueaddedstock string
,skunit_expirydate string
,beat_id string
,beat_name string
,outletname string
,warehousename string
,areaname string
,warehouseerpid string
,type string
,skunitname string
,skucode string
,beatname string
,beatid string
,userid string
,username string
,headquartername string
,outletcategoryname string
,result string
,reason string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullStockatOutletAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullStockatOutletAPI_Bailley(
auto_id int
,stockatoutletid int
,outlet_id int
,user_id int
,transactionid string
,fordate string
,skunit_id int
,availablequantity int
,comment string
,cstock string
,cprice int
,created string
,modified string
,valueaddedstock int
,skunit_expirydate string
,beat_id int
,beat_name string
,outletname string
,warehousename string
,areaname string
,warehouseerpid string
,type string
,skunitname string
,skucode string
,beatname string
,beatid int
,userid int
,username string
,headquartername string
,outletcategoryname string
,result string
,reason string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullStockatOutletAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullStockatOutletAPI_Bailley select
auto_id
,stockatoutletid
,outlet_id
,user_id
,transactionid
,fordate
,skunit_id
,availablequantity
,comment
,cstock
,cprice
,created
,modified
,valueaddedstock
,skunit_expirydate
,beat_id
,beat_name
,outletname
,warehousename
,areaname
,warehouseerpid
,type
,skunitname
,skucode
,beatname
,beatid
,userid
,username
,headquartername
,outletcategoryname
,result
,reason
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullStockatOutletAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ __ _ _ _ _ __ _ _ _ _ _ __ _ _ __ _ __ _ _ _ __ _ _ _ _ _ _ _ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullProductsAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullProductsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullProductsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullProductsAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/PullProductsAPI_Bailley.csv /user/antuit_user/staging/SFA_PullProductsAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullProductsAPI_Bailley(
auto_id string
,skuunitid string
,name string
,mrp string
,vat string
,price string
,fordevice string
,active string
,abbr string
,comment string
,company_id string
,created string
,modified string
,unitspercase string
,skucode string
,erpgroup_id string
,brand_id string
,variant_id string
,packaging_id string
,size_id string
,description string
,focussku string
,unitcase string
,standardcase string
,freesku_id string
,freeskuquantity string
,skuorder string
,servesize_id string
,subbrand_id string
,subpack_id string
,businesstype_id string
,skunit_id string
,skunit_erpid string
,category string
,subcategory string
,inventorycreated string
,inventorymodified string
,saleable string
,nonsaleable string
,intransit string
,brandname string
,packagingname string
,variantname string
,categoryname string
,subcategoryname string
,result string
,reason string
,response string
,stockdate string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullProductsAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullProductsAPI_Bailley(
auto_id int
,skuunitid int
,name string
,mrp int
,vat string
,price string
,fordevice int
,active int
,abbr string
,comment string
,company_id int
,created string
,modified string
,unitspercase int
,skucode string
,erpgroup_id int
,brand_id int
,variant_id int
,packaging_id int
,size_id int
,description string
,focussku string
,unitcase int
,standardcase int
,freesku_id int
,freeskuquantity int
,skuorder int
,servesize_id int
,subbrand_id int
,subpack_id int
,businesstype_id int
,skunit_id int
,skunit_erpid string
,category string
,subcategory string
,inventorycreated string
,inventorymodified string
,saleable int
,nonsaleable int
,intransit int
,brandname string
,packagingname string
,variantname string
,categoryname string
,subcategoryname string
,result string
,reason string
,response string
,stockdate string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullProductsAPI_Bailley/';


INSERT INTO ant_prod.SFA_PullProductsAPI_Bailley select
auto_id
,skuunitid
,name
,mrp
,vat
,price
,fordevice
,active
,abbr
,comment
,company_id
,created
,modified
,unitspercase
,skucode
,erpgroup_id
,brand_id
,variant_id
,packaging_id
,size_id
,description
,focussku
,unitcase
,standardcase
,freesku_id
,freeskuquantity
,skuorder
,servesize_id
,subbrand_id
,subpack_id
,businesstype_id
,skunit_id
,skunit_erpid
,category
,subcategory
,inventorycreated
,inventorymodified
,saleable
,nonsaleable
,intransit
,brandname
,packagingname
,variantname
,categoryname
,subcategoryname
,result
,reason
,response
,stockdate
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullProductsAPI_Bailley;

_ _ _ _ _ _ __ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ __ _ _ _ _ ___

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullUserMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullUserMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullUserMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullUserMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/PullUserMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullUserMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullUserMasterAPI_Bailley(
auto_id string
,userid string
,username string
,name string
,email string
,phoneno string
,address string
,employeeid string
,active string
,creditlimit string
,balance string
,inactivedate string
,created string
,modified string
,reportingto string
,headquarter_id string
,role string
,roleid string
,headquarter string
,areaid string
,areaname string
,warehouseid string
,warehousename string
,warehouseerp_id string
,zoneid string
,zonename string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullUserMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullUserMasterAPI_Bailley(
auto_id int
,userid int
,username string
,name string
,email string
,phoneno int
,address string
,employeeid string
,active int
,creditlimit int
,balance string
,inactivedate string
,created string
,modified string
,reportingto int
,headquarter_id int
,role string
,roleid int
,headquarter string
,areaid int
,areaname string
,warehouseid int
,warehousename string
,warehouseerp_id string
,zoneid int
,zonename string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullUserMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullUserMasterAPI_Bailley select
auto_id
,userid
,username
,name
,email
,phoneno
,address
,employeeid
,active
,creditlimit
,balance
,inactivedate
,created
,modified
,reportingto
,headquarter_id
,role
,roleid
,headquarter
,areaid
,areaname
,warehouseid
,warehousename
,warehouseerp_id
,zoneid
,zonename
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullUserMasterAPI_Bailley;

_ _ _ _ _ __ _ _ _ _ __ _ _ _ _ _ _ __ _ _ _ _ __ _ _ _ _ __ __  __ _ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullAreaMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullAreaMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullAreaMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullAreaMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullAreaMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullAreaMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullAreaMasterAPI_Bailley(
auto_id string
,zoneid string
,zonename string
,subzoneid string
,subzonename string
,warehouseid string
,warehousename string
,warehouse_erpid string
,areaid string
,areaname string
,modified string
,created string
,result string
,reason string
,response string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullAreaMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullAreaMasterAPI_Bailley(
auto_id int
,zoneid int
,zonename string
,subzoneid int
,subzonename string
,warehouseid int
,warehousename string
,warehouse_erpid string
,areaid int
,areaname string
,modified string
,created string
,result int
,reason string
,response int
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullAreaMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullAreaMasterAPI_Bailley select
auto_id
,zoneid
,zonename
,subzoneid
,subzonename
,warehouseid
,warehousename
,warehouse_erpid
,areaid
,areaname
,modified
,created
,result
,reason
,response
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullAreaMasterAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _  __ _ _ _ _ _ _ _ _ _ _ _ _ _  _  __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullBeatMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullBeatMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullBeatMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullBeatMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullBeatMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullBeatMasterAPI_Bailley


CREATE EXTERNAL TABLE test_db.SFA_PullBeatMasterAPI_Bailley(
auto_id string
,id string
,name string
,zone_id string
,erpid string
,created string
,modified string
,active string
,beat_id string
,beat_name string
,beatcreated string
,beatmodified string
,beatdetailid string
,beat_id1 string
,outlet_id string
,beatdetailcreated string
,beatdetailmodified string
,sequence string
,fromdate string
,todate string
,beatdetail_id1 string
,beatdetailcreated1 string
,beatdetailmodified1 string
,area_id string
,outlet_erpid string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullBeatMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullBeatMasterAPI_Bailley(
auto_id int
,id int
,name string
,zone_id int
,erpid string
,created string
,modified string
,active int
,beat_id int
,beat_name string
,beatcreated string
,beatmodified string
,beatdetailid int
,beat_id1 int
,outlet_id int
,beatdetailcreated string
,beatdetailmodified string
,sequence int
,fromdate string
,todate string
,beatdetail_id1 int
,beatdetailcreated1 string
,beatdetailmodified1 string
,area_id int
,outlet_erpid string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullBeatMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullBeatMasterAPI_Bailley select
auto_id
,id
,name
,zone_id
,erpid
,created
,modified
,active
,beat_id
,beat_name
,beatcreated
,beatmodified
,beatdetailid
,beat_id1
,outlet_id
,beatdetailcreated
,beatdetailmodified
,sequence
,fromdate
,todate
,beatdetail_id1
,beatdetailcreated1
,beatdetailmodified1
,area_id
,outlet_erpid
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullBeatMasterAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ __ _ _ _ _ _ _ _ ___ 

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullOutletsMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullOutletsMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullOutletsMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullOutletsMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullOutletsMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullOutletsMasterAPI_Bailley



CREATE EXTERNAL TABLE test_db.SFA_PullOutletsMasterAPI_Bailley(
auto_id string
,outletsmasterid string
,name string
,address string
,keycustomeracc string
,phoneno string
,email string
,ownername string
,ownermobile string
,taxno string
,cstno string
,isdistributor string
,type string
,outlet_level string
,credit_limit string
,credit_days string
,longitude string
,latitude string
,created string
,modified string
,approved string
,balance string
,code string
,pin string
,outletstate_id string
,user_id string
,headquarter_id string
,headquarter string
,outletcategory string
,beatname string
,city string
,state string
,area string
,areaid string
,warehouse string
,warehouseid string
,zone string
,zoneid string
,outlet_vicinity string
,category_sales_from_the_outlet_beverages string
,papl_sales_at_the_outlet string
,count_of_double_door_vizicooler_fridge string
,count_of_single_door_vizicooler_fridge string
,count_of_double_door_vizicooler string
,count_of_single_door_vizicooler string
,count_of_deep_freezers string
,chilling_aid_company string
,count_of_ice_boxes string
,capture_outlet_image string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullOutletsMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullOutletsMasterAPI_Bailley(
auto_id int
,outletsmasterid int
,name string
,address string
,keycustomeracc string
,phoneno int
,email string
,ownername string
,ownermobile int
,taxno string
,cstno string
,isdistributor string
,type string
,outlet_level string
,credit_limit string
,credit_days string
,longitude string
,latitude string
,created string
,modified string
,approved int
,balance int
,code string
,pin int
,outletstate_id int
,user_id int
,headquarter_id int
,headquarter string
,outletcategory string
,beatname string
,city string
,state string
,area string
,areaid int
,warehouse string
,warehouseid int
,zone string
,zoneid int
,outlet_vicinity string
,category_sales_from_the_outlet_beverages string
,papl_sales_at_the_outlet string
,count_of_double_door_vizicooler_fridge string
,count_of_single_door_vizicooler_fridge string
,count_of_double_door_vizicooler string
,count_of_single_door_vizicooler string
,count_of_deep_freezers string
,chilling_aid_company string
,count_of_ice_boxes string
,capture_outlet_image string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullOutletsMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullOutletsMasterAPI_Bailley select
auto_id
,outletsmasterid
,name
,address
,keycustomeracc
,phoneno
,email
,ownername
,ownermobile
,taxno
,cstno
,isdistributor
,type
,outlet_level
,credit_limit
,credit_days
,longitude
,latitude
,created
,modified
,approved
,balance
,code
,pin
,outletstate_id
,user_id
,headquarter_id
,headquarter
,outletcategory
,beatname
,city
,state
,area
,areaid
,warehouse
,warehouseid
,zone
,zoneid
,outlet_vicinity
,category_sales_from_the_outlet_beverages
,papl_sales_at_the_outlet
,count_of_double_door_vizicooler_fridge
,count_of_single_door_vizicooler_fridge
,count_of_double_door_vizicooler
,count_of_single_door_vizicooler
,count_of_deep_freezers
,chilling_aid_company
,count_of_ice_boxes
,capture_outlet_image
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullOutletsMasterAPI_Bailley;

_ _ _ _ _ __ _ _ _ _ _ _ __ _ _ _ __ _ _ __ _ _ _ _ __ _ _ _ _ __ _ _  _ __ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullInventoryTransactionsAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullInventoryTransactionsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullInventoryTransactionsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullInventoryTransactionsAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullInventoryTransactionsAPI_Bailley.csv /user/antuit_user/staging/SFA_PullInventoryTransactionsAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullInventoryTransactionsAPI_Bailley(
auto_id string
,ivdate string
,time string
,skuid string
,skucode string
,skuname string
,quantitycaseunit string
,updateinvtcaseunit string
,quantitylitres string
,updateinvtlitres string
,transaction string
,typeoftransaction string
,commment string
,username string
,userid string
,level string
,warehouse_id string
,warehouse_erpid string
,created string
,modified string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullInventoryTransactionsAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullInventoryTransactionsAPI_Bailley(
auto_id int
,ivdate string
,time string
,skuid int
,skucode string
,skuname string
,quantitycaseunit string
,updateinvtcaseunit string
,quantitylitres string
,updateinvtlitres string
,transaction string
,typeoftransaction string
,commment string
,username string
,userid int
,level string
,warehouse_id int
,warehouse_erpid string
,created string
,modified string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullInventoryTransactionsAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullInventoryTransactionsAPI_Bailley select
auto_id
,ivdate
,time
,skuid
,skucode
,skuname
,quantitycaseunit
,updateinvtcaseunit
,quantitylitres
,updateinvtlitres
,transaction
,typeoftransaction
,commment
,username
,userid
,level
,warehouse_id
,warehouse_erpid
,created
,modified
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullInventoryTransactionsAPI_Bailley;

_ _ _ _ _ _ __ _ _ _ _ __ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ __ _ _ _ _ _ __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullSalesReturnsAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullSalesReturnsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullSalesReturnsAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullSalesReturnsAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullSalesReturnsAPI_Bailley.csv /user/antuit_user/staging/SFA_PullSalesReturnsAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullSalesReturnsAPI_Bailley(
auto_id string
,salesreturnsid string
,quantity string
,reason string
,fordate string
,user_id string
,skunit_id string
,outlet_id string
,created string
,modified string
,unitprice string
,actualprice string
,doneby string
,transactionid string
,batch_id string
,batchno string
,mfgdate string
,return_quantity string
,salesreturnstate_id string
,reason_id string
,payment_id string
,is_active string
,outlet_erpid string
,outlet_classid string
,outlet_class string
,warehouse_id string
,warehouse_erpid string
,area_id string
,area_erpid string
,skucode string
,beat_name string
,hq_name string
,employeeid string
,username string
,user_name string
,reason_name string
,salesreturnstate_name string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullSalesReturnsAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullSalesReturnsAPI_Bailley(
auto_id int
,salesreturnsid int
,quantity int
,reason string
,fordate string
,user_id int
,skunit_id int
,outlet_id int
,created string
,modified string
,unitprice int
,actualprice int
,doneby string
,transactionid string
,batch_id string
,batchno string
,mfgdate string
,return_quantity int
,salesreturnstate_id int
,reason_id int
,payment_id string
,is_active int
,outlet_erpid string
,outlet_classid int
,outlet_class string
,warehouse_id int
,warehouse_erpid string
,area_id int
,area_erpid string
,skucode string
,beat_name string
,hq_name string
,employeeid string
,username string
,user_name string
,reason_name string
,salesreturnstate_name string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullSalesReturnsAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullSalesReturnsAPI_Bailley select
auto_id
,salesreturnsid
,quantity
,reason
,fordate
,user_id
,skunit_id
,outlet_id
,created
,modified
,unitprice
,actualprice
,doneby
,transactionid
,batch_id
,batchno
,mfgdate
,return_quantity
,salesreturnstate_id
,reason_id
,payment_id
,is_active
,outlet_erpid
,outlet_classid
,outlet_class
,warehouse_id
,warehouse_erpid
,area_id
,area_erpid
,skucode
,beat_name
,hq_name
,employeeid
,username
,user_name
,reason_name
,salesreturnstate_name
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullSalesReturnsAPI_Bailley;

____ __ _  _ __ _ _  __ _ __ _ _ _ _ __ _ _ _ __ __ _  _  _ __ _ _ _ _ __ _  __ _ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullActivitiesAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullActivitiesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullActivitiesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullActivitiesAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullActivitiesAPI_Bailley.csv /user/antuit_user/staging/SFA_PullActivitiesAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullActivitiesAPI_Bailley(
auto_id string
,id string
,activityformfield_id string
,modified string
,fordate string
,user_id string
,outlet_id string
,longitude string
,latitude string
,question string
,answer string
,activityformfieldid string
,activityformfieldname string
,outletid string
,outleterp_id string
,outletname string
,userid string
,userusername string
,username string
,headquarterid string
,headquartername string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullActivitiesAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");



CREATE EXTERNAL TABLE ant_prod.SFA_PullActivitiesAPI_Bailley(
auto_id
,id
,activityformfield_id
,modified
,fordate
,user_id
,outlet_id
,longitude
,latitude
,question
,answer
,activityformfieldid
,activityformfieldname
,outletid
,outleterp_id
,outletname
,userid
,userusername
,username
,headquarterid
,headquartername
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullActivitiesAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullActivitiesAPI_Bailley select
auto_id
,id
,activityformfield_id
,modified
,fordate
,user_id
,outlet_id
,longitude
,latitude
,question
,answer
,activityformfieldid
,activityformfieldname
,outletid
,outleterp_id
,outletname
,userid
,userusername
,username
,headquarterid
,headquartername
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullActivitiesAPI_Bailley;


_ _ _ __ _ _ _ _ _ __ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ __ _ _ _ _ __ _ _ _ _ _  __ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullCounterSalesAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullCounterSalesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullCounterSalesAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullCounterSalesAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullCounterSalesAPI_Bailley.csv /user/antuit_user/staging/SFA_PullCounterSalesAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullCounterSalesAPI_Bailley(
auto_id string
,warehouse_id string
,warehouse_erpid string
,warehouse_name string
,username string
,username1 string
,employeeid string
,amount string
,share string
,skunits_name string
,skucode string
,unitprice string
,quantity string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullCounterSalesAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullCounterSalesAPI_Bailley(
auto_id
,warehouse_id
,warehouse_erpid
,warehouse_name
,username
,username1
,employeeid
,amount
,share
,skunits_name
,skucode
,unitprice
,quantity
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullActivitiesAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullCounterSalesAPI_Bailley select
auto_id
,warehouse_id
,warehouse_erpid
,warehouse_name
,username
,username1
,employeeid
,amount
,share
,skunits_name
,skucode
,unitprice
,quantity
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullCounterSalesAPI_Bailley;

_ _ _ _ _ _ _ _ _  __  _ _ _ _ _ __  _ __ _ _ _ _ _ _ _  _ _ _ _ _ _ _ _ __  __ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullDiscountMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullDiscountMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullDiscountMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullDiscountMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullDiscountMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullDiscountMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullDiscountMasterAPI_Bailley(
auto_id string
,id string
,discount string
,skunit_id string
,outlet_id string
,user_id string
,fromdate string
,todate string
,status string
,approveduser_id string
,is_active string
,created string
,modified string
,outletid string
,outleterp string
,skunitid string
,skunitname string
,skucode string
,userid string
,username string
,userusername string
,createddate string
,createddate_dd string
,ingestedtime current_timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullDiscountMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullDiscountMasterAPI_Bailley(
auto_id
,id
,discount
,skunit_id
,outlet_id
,user_id
,fromdate
,todate
,status
,approveduser_id
,is_active
,created
,modified
,outletid
,outleterp
,skunitid
,skunitname
,skucode
,userid
,username
,userusername
,createddate
,createddate_dd
,ingestedtime current_timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullDiscountMasterAPI_Bailley/';


INSERT INTO ant_prod.SFA_PullDiscountMasterAPI_Bailley select
auto_id
,id
,discount
,skunit_id
,outlet_id
,user_id
,fromdate
,todate
,status
,approveduser_id
,is_active
,created
,modified
,outletid
,outleterp
,skunitid
,skunitname
,skucode
,userid
,username
,userusername
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullDiscountMasterAPI_Bailley;

___ _ _ _ _ _ _ _ _ _ _ _ _ __ _  _ __ _ _ _ _  _ __ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullGenericFormAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullGenericFormAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullGenericFormAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullGenericFormAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullGenericFormAPI_Bailley.csv /user/antuit_user/staging/SFA_PullGenericFormAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullGenericFormAPI_Bailley(
auto_id string
,id string
,genericformfield_id string
,modified string
,fordate string
,user_id string
,longitude string
,latitude string
,question string
,answer string
,userid string
,username string
,user_name string
,headquarterid string
,headquarter_name string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullGenericFormAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullGenericFormAPI_Bailley(
auto_id
,id
,genericformfield_id
,modified
,fordate
,user_id
,longitude
,latitude
,question
,answer
,userid
,username
,user_name
,headquarterid
,headquarter_name
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullGenericFormAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullGenericFormAPI_Bailley select
auto_id
,id
,genericformfield_id
,modified
,fordate
,user_id
,longitude
,latitude
,question
,answer
,userid
,username
,user_name
,headquarterid
,headquarter_name
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullGenericFormAPI_Bailley;

_ _ _ _ __ _ _ _ _ __ _ _ _ _ __ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _  _ _ __  __ _ _ 

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullListOrder_SchemedetailAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullListOrder_SchemedetailAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullListOrder_SchemedetailAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullListOrder_SchemedetailAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA.PullListOrder_SchemedetailAPI_Bailley.csv /user/antuit_user/staging/SFA_PullListOrder_SchemedetailAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullListOrder_SchemedetailAPI_Bailley(
auto_id string
,schemedetailid string
,schemedetailorder_id string
,schemedetailskunit_id string
,scheme_id string
,schemedetaildiscount_id string
,for_skunit_id string
,for_quantity string
,free_cash_scheme string
,free_cash_discount string
,free_quantity string
,created string
,modified string
,skucode string
,skuprice string
,for_skucode string
,for_skuprice string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullListOrder_SchemedetailAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullListOrder_SchemedetailAPI_Bailley(
auto_id
,schemedetailid
,schemedetailorder_id
,schemedetailskunit_id
,scheme_id
,schemedetaildiscount_id
,for_skunit_id
,for_quantity
,free_cash_scheme
,free_cash_discount
,free_quantity
,created
,modified
,skucode
,skuprice
,for_skucode
,for_skuprice
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullListOrder_SchemedetailAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullListOrder_SchemedetailAPI_Bailley select
auto_id
,schemedetailid
,schemedetailorder_id
,schemedetailskunit_id
,scheme_id
,schemedetaildiscount_id
,for_skunit_id
,for_quantity
,free_cash_scheme
,free_cash_discount
,free_quantity
,created
,modified
,skucode
,skuprice
,for_skucode
,for_skuprice
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullListOrder_SchemedetailAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  _ _ _ _  _ _  _ _ _ _ _ _ _ __  __ _ _ _ _ _ _ _ _ _ __ _ 


hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullSales_SchemedetailAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullSales_SchemedetailAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullSales_SchemedetailAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullSales_SchemedetailAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA_PullSales_SchemedetailAPI_Bailley.csv /user/antuit_user/staging/SFA_PullSales_SchemedetailAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullSales_SchemedetailAPI_Bailley(
auto_id string
,schemedetailid string
,schemedetailpayment_id string
,schemedetailskunit_id string
,scheme_id string
,schemedetaildiscount_id string
,for_skunit_id string
,for_quantity string
,free_cash_scheme string
,free_cash_discount string
,free_quantity string
,created string
,modified string
,skucode string
,skuprice string
,for_skucode string
,for_skuprice string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullSales_SchemedetailAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullSales_SchemedetailAPI_Bailley(
auto_id
,schemedetailid
,schemedetailpayment_id
,schemedetailskunit_id
,scheme_id
,schemedetaildiscount_id
,for_skunit_id
,for_quantity
,free_cash_scheme
,free_cash_discount
,free_quantity
,created
,modified
,skucode
,skuprice
,for_skucode
,for_skuprice
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullListOrder_SchemedetailAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullSales_SchemedetailAPI_Bailley select
auto_id
,schemedetailid
,schemedetailpayment_id
,schemedetailskunit_id
,scheme_id
,schemedetaildiscount_id
,for_skunit_id
,for_quantity
,free_cash_scheme
,free_cash_discount
,free_quantity
,created
,modified
,skucode
,skuprice
,for_skucode
,for_skuprice
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullSales_SchemedetailAPI_Bailley;

_ _ _ _ _ _ _ _ __  __ _ _ _ _ _ _  __ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ _ _ _ _ _ _  __

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullSchemeMasterAPI_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullSchemeMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullSchemeMasterAPI_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullSchemeMasterAPI_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA_PullSchemeMasterAPI_Bailley.csv /user/antuit_user/staging/SFA_PullSchemeMasterAPI_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullSchemeMasterAPI_Bailley(
auto_id string
,schemeid string
,holderid string
,holdertype string
,warehouse_erpid string
,enddate string
,schemesid string
,schemesname string
,forquantity string
,active string
,user_id string
,unitcasecalculation string
,enddatetime string
,created string
,modified string
,saverupee string
,maxsavecash string
,freequantity string
,max_quantity string
,main_skus_id string
,main_skus_name string
,skucode string
,skus string
,result string
,reason string
,response string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullSchemeMasterAPI_Bailley/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE ant_prod.SFA_PullSchemeMasterAPI_Bailley(
auto_id
,schemeid
,holderid
,holdertype
,warehouse_erpid
,enddate
,schemesid
,schemesname
,forquantity
,active
,user_id
,unitcasecalculation
,enddatetime
,created
,modified
,saverupee
,maxsavecash
,freequantity
,max_quantity
,main_skus_id
,main_skus_name
,skucode
,skus
,result
,reason
,response
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullSchemeMasterAPI_Bailley/';

INSERT INTO ant_prod.SFA_PullSchemeMasterAPI_Bailley select
auto_id
,schemeid
,holderid
,holdertype
,warehouse_erpid
,enddate
,schemesid
,schemesname
,forquantity
,active
,user_id
,unitcasecalculation
,enddatetime
,created
,modified
,saverupee
,maxsavecash
,freequantity
,max_quantity
,main_skus_id
,main_skus_name
,skucode
,skus
,result
,reason
,response
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullSchemeMasterAPI_Bailley;

_ _ _ _ _ _ _ _ _ _ _ __ _ _ _  __ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _ _ _ _ _ _  __  \

hdfs dfs -mkdir /user/antuit_user/staging/SFA_PullTargetsAPIHQ_Bailley
hdfs dfs -mkdir /user/antuit_user/prod/SFA_PullTargetsAPIHQ_Bailley
hdfs dfs -chmod 777 /user/antuit_user/staging/SFA_PullTargetsAPIHQ_Bailley
hdfs dfs -chmod 777 /user/antuit_user/prod/SFA_PullTargetsAPIHQ_Bailley
hdfs dfs -put /home/nithin/DDL_Bailley/DotNet.SFA_PullTargetsAPIHQ_Bailley.csv /user/antuit_user/staging/SFA_PullTargetsAPIHQ_Bailley

CREATE EXTERNAL TABLE test_db.SFA_PullTargetsAPIHQ_Bailley(
auto_id string
,targetsid string
,user_id string
,tlsd string
,dropsize string
,totalcalls string
,percentproductivecall string
,ipsc string
,newoutlets string
,startdate string
,enddate string
,amount string
,created string
,modified string
,userid string
,username string
,headquarterid string
,headquartername string
,targetdetailid string
,targetid string
,skunitid string
,quantity string
,targetdetailmodified string
,skucode string
,result string
,reason string
,response string
,createddate string
,createddate_dd string
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/staging/SFA_PullTargetsAPIHQ_Bailley/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE ant_prod.SFA_PullTargetsAPIHQ_Bailley(
auto_id
,targetsid
,user_id
,tlsd
,dropsize
,totalcalls
,percentproductivecall
,ipsc
,newoutlets
,startdate
,enddate
,amount
,created
,modified
,userid
,username
,headquarterid
,headquartername
,targetdetailid
,targetid
,skunitid
,quantity
,targetdetailmodified
,skucode
,result
,reason
,response
,createddate
,createddate_dd
,ingestedtime timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION 'wasb://str-papl-prod@strpaplprod.blob.core.windows.net/user/antuit_user/prod/SFA_PullTargetsAPIHQ_Bailley/';


INSERT INTO ant_prod.SFA_PullTargetsAPIHQ_Bailley select
auto_id
,targetsid
,user_id
,tlsd
,dropsize
,totalcalls
,percentproductivecall
,ipsc
,newoutlets
,startdate
,enddate
,amount
,created
,modified
,userid
,username
,headquarterid
,headquartername
,targetdetailid
,targetid
,skunitid
,quantity
,targetdetailmodified
,skucode
,result
,reason
,response
,createddate
,createddate_dd
,current_timestamp as ingestedtime from test_db.SFA_PullTargetsAPIHQ_Bailley;

_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  __ _ _ _ _ _ _ _ _ _ _ __ _ _ _ _ _ _ _  _






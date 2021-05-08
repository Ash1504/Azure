# Import Required Libraries
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import *
from azure.storage.blob import ContainerClient

blob_account_name = "<your-storage-account-name>"
blob_container_name = "<your-container-name>"
blob_folder_name = "<your-folder-in-container>"
blob_relative_path = blob_folder_name + "/Inbound"
blob_sas_token = r”<your-storage-account-sas-token>”
blobwasbspath = "<your wasbs path for blob>"

# For e.g., wasbs://<blob_container_name> @blob_account_name.blob.core.windows.net/
blob_account_key = "<your_blob_account_key>"

# wasbs path for a blob and spark conf set
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)

spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
print('Remote blob path: ' + wasbs_path)

#connection string for blob
connection_string = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net" % (blob_account_name, blob_account_key)

# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key. <storage-account-name>. blob.core.windows.net",
  "<blob_account_key>")

#connection details for synapse analytics
dwServer = "<your-synapse-workspace>. sql.azuresynapse.net"
dwPort = "1433"
dwDBName = "<db name>"

#Retrieve sql username and password from key vault scope
dwUserName = dbutils.secrets.get(scope = "key-vault-secrets", key = "sqluser")
dwPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "sqlpassword")

#Create link to connect with the Synapse via JDBC url
tempDir = "wasbs://" + blob_container_name + "@" + blob_account_name + ".blob.core.windows.net/" + blob_folder_name
dwUrl = "jdbc:sqlserver://"+dwServer+":"+dwPort+";database="+dwDBName+";user="+dwUserName+";password="+dwPassword+";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

#Container connection to access blobs in a container
container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=blob_container_name)

#list of all files in a container
files = []
count = 0
blob_list = container.list_blobs()
for blob in blob_list:
   files.append(blob.name)
   count=count+1
print("Files to be processed are: -")
print(files)


#read the csv files and populate the corresponding tables
table_names = ["I","PR","RS","TS","ASGN_BLS","PG_BS","TS_BS","RESOURCES","TmSet"]

#loop to table names and process the csv files to Synapse Database
for name in table_names:
  for file in files:
    if name in file:
      print("File name:- {}".format(file))
      df = spark.read.csv(wasbs_path + "/"+file, header = 'true')

      # Add Load Date column in the data frame as current timestamp
      df = df.withColumn("LOAD_DATE",current_timestamp())
      
      #Convert date columns in file to datatype datetime
      datecols = [x for x in df.columns if x.endswith('Date')]
      for col in datecols:
        df = df.withColumn(col,to_timestamp(df[col], 'yyyy-MM-dd HH:mm:ss'))

      #write the data from the file to table
      df.write \
        .format("com.databricks.spark.sqldw") \
        .option("url", dwUrl) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", "dbo."+name+"_RAW ") \
        .option("tempDir", tempDir) \
        .option("truncate","true") \
        .option("maxStrLength", "4000" ) \
        .mode("overwrite") \              
        
      print("File {} is loaded to synpase".format(file))

# Function to Copy blob from one folder to another
def copy_blob(account_name,container,folder,file,con_str):
    status = None
    blob_service_client = BlobServiceClient.from_connection_string(con_str)
    source_blob = "https://%s.blob.core.windows.net/%s/%s/Inbound/%s" % (account_name,container,folder,file)
    copied_blob = blob_service_client.get_blob_client(container+"/"+folder+"/Archive",file)
    
    copied_blob.start_copy_from_url(source_blob)
    for i in range(10):
      props = copied_blob.get_blob_properties()
      status = props.copy.status
      print("Copy Status: "+status)
      if status == "success":
        break
      time.sleep(3)
    
    if status != "success":
      # if not finished after 30s, cancel the operation
      props = copied_blob.get_blob_properties()
      print(props.copy.status)
      copy_id = props.copy.id
      copied_blob.abort_copy(copy_id)
      props = copied_blob.get_blob_properties()
      print(props.copy.status)

# Function to delete blob
def delete_blob(container_client,path,file):
  full_path = path+"/"+file
  container_client.delete_blob(full_path)
  print("File {} has been deleted".format(file))

##############################################################################
## Get the Access token and Sheet Id from ADF Pipeline
##############################################################################
dbutils.widgets.text("access_token", "","")
access_token = dbutils.widgets.get("access_token")
print ("Parameter Access token :")
print (access_token)

dbutils.widgets.text("sheetId", "" ,"")
sheetId = dbutils.widgets.get("sheetId")
print ("Parameter Sheet ID  :")
# Convert sheet ID to interger type
sheetid = int(sheetId)
print (sheetId)

dbutils.widgets.text("tableSchema", "","")
tableSchema = dbutils.widgets.get("tableSchema")
print ("Parameter Table Schema :")
print (tableSchema)

dbutils.widgets.text("tableName", "","")
tableName = dbutils.widgets.get("tableName")
print ("Parameter Table Name :")
print (tableName)

table = tableSchema + "." + tableName
print(table)

##############################################################################
## Import Required Libraries
##############################################################################

import smartsheet
import pandas as pd
# Initialize client
smartsheet_client = smartsheet.Smartsheet(access_token = access_token)

# Make sure we don't miss any errors
smartsheet_client.errors_as_exceptions(True)

##############################################################################
## Get the sheet
##############################################################################
sheet = smartsheet_client.Sheets.get_sheet(sheetId)

##############################################################################
## Get Column Id and Name from sheet.columns
##############################################################################
column_id = [i.id for i in sheet.columns]
column_name = [i.title for i in sheet.columns]


##############################################################################
## Map column id and its corresponding name and present it in a dataframe
##############################################################################
dict_columns = {"columnId":column_id, "Name":column_name}
df_columns = pd.DataFrame.from_dict(dict_columns)
print(df_columns.head())

##############################################################################
## created an empty list "rows" to store the cells data in sheet rows after converting 
## it to a dictionary. Append the dictionary data into above list
##############################################################################
rows=[]
for i in sheet.rows:
  data = i.to_dict()
  rows.append(data["cells"])
  
##############################################################################
## create a dict with keys as the column id and value as an empty list.
##############################################################################
data = {i:[] for i in column_id}
print(data)

##############################################################################
## Loop through data in rows. Each row in rows has column id and value of the 
## cell. If value is not present then it is to be considered empty else fetch
## the value from row dictionary. Append this value into the list of values for the
## key present in the data dictionary. 
##############################################################################
# rows is the list of dictionary. 
for row in rows:
  for key in row:
    if 'value' not in key:
      value = None
    else:
      value = key['value']
    data[key['columnId']].append(value)
#print(sample_dict)

##############################################################################
## Load the data in dataframe and rename the columns
##############################################################################
df = pd.DataFrame(data)
df.columns = column_name
df.fillna("", inplace=True)
print(df.tail())

##############################################################################
## Function to find rows with data as None in all columns
##############################################################################
def cleaned_df(df):
  for index, rows in df.iterrows():
    row = []
    for i in df.columns:
      row.append(rows[i])
      ##### create a set to get only unique values
    row_set = set(row)
    if len(row_set) == 1:
      df.drop(index, inplace = True)
  return df

# call the function
df = cleaned_df(df)
print(df.tail())

# replace unwanted characters from the columns and rename them
cols = [i.replace(".","").replace("(","").replace(")","").replace("/","").replace(" ","_").replace(",","").replace(";","").replace("{","").replace("}","").replace("\n","").replace("\t","").replace("=","") for i in df.columns]
df.columns = cols

# convert column to string. This is so that it is easy to convert the df to sparkDF
for i in df.columns:
  if df[i].dtype =='object':
    df[i] = df[i].astype('str') 
print(df.tail())

# convert pandas to sparkdf
spark.conf.set("spark.sql.execution.arrow.enabled","true")
sparkDF=spark.createDataFrame(df)
display(sparkDF)

blob_account_name = ""
blob_container_name = "landing-area"
blob_folder_name = "ADBS_Logs"
blob_account_key = ""

#connection details for synapse analytics
dwServer = ""
dwPort = "1433"
dwDBName = "BGNE_DW"

#Retrieve sql username and passwrod from key vault scope
dwUserName = dbutils.secrets.get(scope = "synapse-secret-scope", key = "sqluser")
dwPassword = dbutils.secrets.get(scope = "synapse-secret-scope", key = "sqlpassword")

#Create link to connect with the Synapse via JDBC url
tempDir = "wasbs://" + blob_container_name + "@" + blob_account_name + ".blob.core.windows.net/" + blob_folder_name
dwUrl = "jdbc:sqlserver://"+dwServer+":"+dwPort+";database="+dwDBName+";user="+dwUserName+";password="+dwPassword+";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

#connection string for blob
connection_string = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net" % (blob_account_name,blob_account_key)

# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.bgnesynapse.blob.core.windows.net",
  blob_account_key)
  
##############################################################################
## Write Dataframe to Synapse table
##############################################################################
sparkDF.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", dwUrl) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", table) \
  .option("tempDir", tempDir) \
  .option("truncate","true") \
  .option("maxStrLength", "4000" ) \
  .mode("overwrite") \
  .save()
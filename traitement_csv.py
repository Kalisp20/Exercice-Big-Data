import os, uuid
from azure.storage.blob import BlobServiceClient

connect_str = 'DefaultEndpointsProtocol***'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# PARTIE TRAITEMENT

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("fs.azure.account.key***")
container_name ='input'

def process_datafile(input_path, output_path):

  df = spark.read.csv(input_path, inferSchema = True, header=True)#récupère csv d'après son path
  df.createOrReplaceTempView('table_transaction')# le csv est chargé en df sous le nom de table_transaction
  
  result = spark.sql("""SELECT country_sender, AVG(amount) as avg_amount, current_timestamp() as date FROM table_transaction GROUP BY country_sender""")
  try :
    df2 = spark.read.csv(output_path, inferSchema = True, header = True)
    df3 = df2.union(result)
    df3.coalesce(1).write.mode("append").option("header","true").format("com.databricks.spark.csv").save(output_path)
  except :
    result.coalesce(1).write.mode("append").option("header","true").format("com.databricks.spark.csv").save(output_path)

input_path = "wasbs:***"
output_path = "wasbs:***"

process_datafile(input_path,output_path)


#PARTIE FICHIER SUPPRIMÉ

def delete_file(container_name) :
    container_client = blob_service_client.get_container_client(container_name)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list :
        print(blob.name)
        container_client.delete_blob(blob)
    print('Every files have been deleted')

delete_file(container_name)

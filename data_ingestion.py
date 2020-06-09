import pandas as pd
import time
import uuid

time_interval_between_sending = 5 * 60

data_size_in_mb = 1000

def get_data_to_send(i):
    return '../transactions_{}.csv'.format(i)



def send_data(upload_file_path):
    
    import os, uuid
    from azure.storage.blob import BlobServiceClient

    connect_str= '***'
    blob_service_client= BlobServiceClient.from_connection_string(connect_str)
    container_name=['input', 'archives']
    for container in container_name :
        blob_client= blob_service_client.get_blob_client(container=container, blob = 'transactions'+ str(uuid.uuid4()))
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)


    """
        This function is in charge to send the data
        to your infrastructure. You have to implement it the way
        you want : blob storage, kafka, SQL database, etc. etc.

    """
    pass


while True:

    t1= time.time()
    df= get_data_to_send(1)

    print('Sending data to the infrastructure')
    send_data(df)
    print('Done. Waiting before sending next data...')
    t2= time.time()

    time_spent_sending_data = t2 - t1

    if time_spent_sending_data < time_interval_between_sending:

        time.sleep(time_interval_between_sending - time_spent_sending_data)

import io
import os
from dotenv import load_dotenv
from azure.storage.blob import (
    BlockBlobService
)
import pandas as pd

load_dotenv()

output = io.StringIO()

accountName = os.environ['ACCOUNT_NAME']
accountKey = os.environ['STORAGE_KEY']
containerName = os.environ['containerName']

def blob_uploader(data,file_name):
    output = data.to_csv(index=False,encoding="utf-8")

    blobService = BlockBlobService(account_name=accountName, account_key=accountKey)

    blobService.create_blob_from_text(containerName, file_name,output)
    
    print('data upload successful')

    return f"https://{accountName}.blob.core.windows.net/{containerName}/{file_name}"

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from google.cloud import storage
import datetime
from google.cloud import secretmanager
import pandas as pd
import os
from get_supplier_config import update_df_base_supplier_config
from send_model_result_to_pubsub import gcs_function
import json
from  convertion import schema_convert, detect_CN
import uuid

# credential_path = ""
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

model_id = "Compose-15-10-2024"

endpoint = "https://cc-ai-invoice-extract-prod.cognitiveservices.azure.com/"
key = ""


# bucket_name = "azure-invoice-pdf-error"
# blob_name = "INV01626030-3.pdf"

def get_credentials_from_secret():
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/547666409003/secrets/cc-docai-service-account-key/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = response.payload.data.decode("UTF-8")

    # Write to a temporary file or use in-memory
    credentials_path = "/tmp/"  # Cloud Function has /tmp writable space
    with open(credentials_path, "w") as f:
        f.write(secret)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    return credentials_path 

from google.cloud import storage

def generate_signed_url(bucket_name, blob_name):
    client = storage.Client()  # Uses default credentials
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        url = blob.generate_signed_url(
            version='v4',
            expiration=datetime.timedelta(minutes=15),  # Adjust as needed
            method='GET'
        )
        print(f"Generated URL: {url}")  # Debug statement
        return url
    except Exception as e:
        print(f"Error generating signed URL: {e}")
        return None
    

def extract_items_to_df(item_field,blob_name):
    extracted_items = []
    
    # item_field is expected to be a list, so directly iterate through it
    for document_field in item_field:
        item_data = {'invoiceID': blob_name}
        
        # Dynamically extract all key-value pairs in the dictionary
        for key, value in document_field.value.items():
            item_data[key] = value.value
        
        # Append the extracted data for this item
        extracted_items.append(item_data)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(extracted_items)
    
    return df

def insert_pdf_to_model(bucket_name, blob_name):
    # get_credentials_from_secret()
    formUrl = generate_signed_url(bucket_name, blob_name)

    print(f"Type of formUrl: {type(formUrl)}")  # Debug statement
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    poller = document_analysis_client.begin_analyze_document_from_url(model_id, formUrl)
    result = poller.result()
    base_name = os.path.basename(blob_name)
    file_id = os.path.splitext(base_name)[0]
    if file_id.endswith('-0'):
        file_id = file_id[:-2]
        print(file_id)

    for idx, document in enumerate(result.documents):
        print("--------Analyzing document--------") 
        fields = document.fields
        doc_type = document.doc_type
        ModelName = doc_type.split(":")[-1]
        unique_id = str(uuid.uuid4())
        invoice_no = fields.get('InvoiceNo').value if fields.get('InvoiceNo') else None
        InvoiceID =  f"{unique_id}-{invoice_no}" if invoice_no else unique_id
        new_file_name = f'{InvoiceID}.pdf' 
        
        # Extract header fields
        header_data = {
            'InvoiceID':  InvoiceID,
            'ModelName': ModelName,
            'InvoiceType': invoice_no,
            'InvoiceNo': fields.get('InvoiceNo').value if fields.get('InvoiceNo') else None,
            'InvoiceDate': fields.get('InvoiceDate').value if fields.get('InvoiceDate') else None,
            'PoNo': fields.get('PoNo').value if fields.get('PoNo') else None,
            'NetAmount': fields.get('NetAmount').value if fields.get('NetAmount') else None,
            'TaxAmount': fields.get('TaxAmount').value if fields.get('TaxAmount') else None,
            'InvoiceTotal': fields.get('InvoiceTotal').value if fields.get('InvoiceTotal') else None,
            'Freight': fields.get('Freight').value if fields.get('Freight') else None,
            'CustomerAccount': fields.get('CustomerAccount').value if fields.get('CustomerAccount') else None,
            'SupplierFull': fields.get('SupplierFull').value if fields.get('SupplierFull') else None,
            'ABN': fields.get('ABN').value if fields.get('ABN') else None,
            'CustomerName': fields.get('CustomerName').value if fields.get('CustomerName') else None,
        }
        df_header = pd.DataFrame([header_data])  # Convert header_data to a single-row DataFrame
        # Check if 'Item' exists and extract if valid
        items_data = fields.get('Item').value if fields.get('Item') else []
        # Convert items to a DataFrame
        df_items = extract_items_to_df(items_data, InvoiceID)

        # List of required columns
        df_header, df_items = schema_convert(df_header,df_items)
        df_header, df_items = update_df_base_supplier_config(df_header,df_items, ModelName)
        df_header, df_items = detect_CN(df_header, df_items)

    return df_header, df_items, new_file_name

def convert_result_send_to_pubsub(df_header, df_items):
    json_header = df_header.to_json(orient='records')
    json_item = df_items.to_json(orient='records')
    header_obj = json.loads(json_header)  # this will be a list of header records
    item_obj = json.loads(json_item)   
    combined_json = {
        "header": header_obj,
        "item": item_obj
    }
    print(combined_json)
    gcs_function(combined_json)


# insert_pdf_to_model(bucket_name, blob_name)
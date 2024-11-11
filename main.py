import functions_framework
from google.cloud import storage
from call_azure_get_result import insert_pdf_to_model,convert_result_send_to_pubsub
# Triggered by a change in a Cloud Storage bucket
@functions_framework.cloud_event


# Triggered by a change in a Cloud Storage bucket
def main(event):
    print(f"event:{event}")
    # Extract information from the event payload
    bucket_name = event.data['bucket']
    file_name = event.data['name']
    content_type = event.data['contentType']

    print(f"File {file_name} uploaded to {bucket_name} with content type {content_type}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)

    # Define the names of the success and error buckets
    success_bucket_name = 'azure-invoice-pdf-transite'
    error_bucket_name = 'azure-invoice-pdf-error'

    try:
        df_header, df_items, new_file_name = insert_pdf_to_model(bucket_name, file_name)
        convert_result_send_to_pubsub(df_header, df_items)
        # If download succeeds, move the file to the success bucket
        success_bucket = storage_client.bucket(success_bucket_name)
        bucket.copy_blob(blob, success_bucket, new_file_name)
        print(f"File {new_file_name} moved to success bucket {success_bucket_name}")
        # Optionally delete the file from the original bucket after success
        blob.delete()
        print(f"File {new_file_name} deleted from source bucket {bucket_name}")

    except Exception as e:
        # If download fails, move the file to the error bucket
        print(f"Failed to process {file_name}. Error: {e}")
        error_bucket = storage_client.bucket(error_bucket_name)
        bucket.copy_blob(blob, error_bucket, file_name)
        print(f"File {file_name} moved to error bucket {error_bucket_name}")
        # Optionally delete the file from the original bucket after failure
        blob.delete()
        print(f"File {file_name} deleted from source bucket {bucket_name}")

if __name__ == '__main__':
    main('data')
import json
import os
from loguru import logger
from dotenv import load_dotenv
from data_download import data_url, load_excel_into_memory, upload_to_s3


def main(): 
    # Your AWS credentials and S3 bucket details
    load_dotenv()
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    bucket_name = os.getenv('bucket_name')
    object_name = os.getenv('object_name')

    # The URL of the webpage containing the Excel file link
    web_url = "https://datamillnorth.org/dataset/2o13g/houses-in-multiple-occupation-licence-register/"

    # Fetch the download URL and load the Excel file into a DataFrame
    download_url = data_url(web_url)
    df = load_excel_into_memory(download_url)

    # Upload the DataFrame as an Excel file to the S3 bucket
    if df is not None:
        upload_to_s3(df, bucket_name, object_name, aws_access_key_id, aws_secret_access_key)
    else:
        logger.error("DataFrame is empty. Nothing to upload.")
        

def lambda_handler(event, context):
    try:
        main()
        return {
            'statusCode': 200,
            'body': json.dumps("Process completed successfully.")
        }
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps("Failed to complete the process.")
        }
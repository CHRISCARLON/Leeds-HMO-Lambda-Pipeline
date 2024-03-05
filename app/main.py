import json
import boto3
from loguru import logger
from data_download import data_url, load_excel_into_memory, upload_to_s3

def get_secrets(secret_name, region_name="eu-west-2"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {str(e)}")
        raise e
    else:
        # Assuming the secret is stored as a JSON string
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

def main(): 
    secrets = get_secrets("leedshmopipecreds")  

    # AWS credentials and S3 bucket details 
    aws_access_key_id = secrets['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = secrets['AWS_SECRET_ACCESS_KEY']
    bucket_name = secrets['BUCKET_NAME']
    object_name = secrets['OBJECT_NAME']

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

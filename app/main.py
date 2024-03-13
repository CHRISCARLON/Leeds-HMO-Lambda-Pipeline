import json
import os
import boto3
from loguru import logger
import pandas as pd
from data_download import data_url, load_excel_into_memory, upload_to_s3, retrieve_from_s3
from data_transform import date_from_url, remove_dots_from_date, new_date_added_column, add_postcode_column ,extract_unique_postcodes, get_coords, rename_cols
from duckdb_push import connect_to_motherduck, load_into_duckdb

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
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

def main(): 
    # AWS credentials, S3 bucket, and MotherDuck details 
    secrets = get_secrets("leedshmopipecreds")
    aws_access_key_id = secrets['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = secrets['AWS_SECRET_ACCESS_KEY']
    bucket_name = secrets['BUCKET_NAME']
    object_name = secrets['OBJECT_NAME']
    token = secrets["MOTHER_TOKEN"]
    database = secrets["MOTHERDB_NAME"]

    # STEP 1
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
    
    # STEP 2
    # Pull the excel back into df for transformation
    df = retrieve_from_s3(bucket_name, object_name, aws_access_key_id, aws_secret_access_key)
    
    df = rename_cols(df)
    
    date_from_excel = date_from_url(download_url)
    date_from_excel_nodots = remove_dots_from_date(date_from_excel)
    df = new_date_added_column(df, date_from_excel_nodots)

    df = add_postcode_column(df)
    
    post_code_list = extract_unique_postcodes(df)
    coords = get_coords(post_code_list)
    
    df = pd.merge(df, coords, on='postcode', how='left')
    print("done")
    
    # STEP 3
    # Load into MotherDuck
    home_directory = "/tmp/duckdb"
    os.environ["HOME"] = home_directory
    os.makedirs(home_directory, exist_ok=True)

    con = connect_to_motherduck(database, token)
    load_into_duckdb(con, date_from_excel_nodots, df)
    
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

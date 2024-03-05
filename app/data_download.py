import requests
from bs4 import BeautifulSoup
from io import BytesIO
import pandas as pd
from loguru import logger
import boto3

def data_url(url):
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        link_container = soup.find('div', class_='dstripe__body')
        
        if link_container:
            a_tag = link_container.find('a', href=True)
            if a_tag:
                href_value = a_tag['href']
                logger.info("URL found")
                return href_value
            else:
                logger.warning("Link not found.")
        else:
            logger.warning("Container not found.")
    else:
        logger.warning("Failed to fetch the URL:", url)
        

def load_excel_into_memory(url):
    response = requests.get(url)
    if response.status_code == 200:
        # Use BytesIO to handle the binary content and read it into a pandas DataFrame
        # This loads the content into memory instead of creating a local file
        excel_data = BytesIO(response.content)
        df = pd.read_excel(excel_data)
        
        logger.success("Excel file loaded into memory successfully.")
        return df
    else:
        logger.warning("Failed to load the Excel file.")
        return None
    

def upload_to_s3(df, bucket_name, object_name, aws_access_key_id, aws_secret_access_key):
    # Convert DataFrame to Excel format 
    excel_buffer = BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)

    # Initialise a session using Amazon S3 and upload file
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3_resource = session.resource('s3')

    # Upload the Excel file to the specified S3 bucket
    s3_resource.Bucket(bucket_name).put_object(Key=object_name, Body=excel_buffer.getvalue())
    logger.success(f"Excel file uploaded to S3 bucket '{bucket_name}' successfully.")

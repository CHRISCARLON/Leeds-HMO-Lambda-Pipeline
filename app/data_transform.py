import re
import pandas as pd
import json 
import requests
from loguru import logger

####
"""Date Column"""
def date_from_url(url):
    # Extract the date from the URL using a regular expression
    # Url will look like this "https://datamillnorth.org/download/2o13g/c476394e-8294-4c15-b1ff-44d32e6809c2/15.02.2024.xlsx"
    # Need the 15.02.2024 part 
    date_pattern = r"\d{2}\.\d{2}\.\d{4}"
    match = re.search(date_pattern, url)
    if match:
        date_str = match.group(0)
    else:
        raise ValueError("No valid date found in URL.")

    return date_str

def remove_dots_from_date(date_str):
    # Change 15.02.2024 -> 15022024 
    return date_str.replace('.', '')

def new_date_added_column(df, date):
    df["date_published"] = date
    return df

####
"""Postcode Column"""
def extract_postcode(address):
    # Extract the postcode from the address string, looking for 'Leeds', 'Pudsey', or 'Otley' before the postcode
    match = re.search(r'(Leeds|Pudsey|Otley|Wetherby)\s+(\S+\s+\S+)$', address, re.IGNORECASE)
    return match.group(2) if match else None

def add_postcode_column(df):
    # Add a 'Postcode' column to the DataFrame
    df['postcode'] = df['address'].apply(extract_postcode)
    return df

def extract_unique_postcodes(df):
    # Create a new DataFrame with unique postcodes
    unique_postcodes = df['postcode'].unique().tolist()
    return unique_postcodes

def bulk_lookup_postcodes(postcodes):
    # Match postcodes to their Point coordinates
    url = "https://api.postcodes.io/postcodes"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps({"postcodes": postcodes}))
    if response.status_code == 200:
        response_data = response.json()
        lat_lon_dict = {}
        for result in response_data['result']:
            if result['result']:
                postcode = result['result']['postcode']
                latitude = result['result']['latitude']
                longitude = result['result']['longitude']
                lat_lon_dict[postcode] = (latitude, longitude)
        return lat_lon_dict
    else:
        return None

def get_coords(df):
    # Define chunk size for postcode lookups
    chunk_size = 1
    all_lat_lon = {}
    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]
        lookup_result = bulk_lookup_postcodes(chunk)
        if lookup_result:
            all_lat_lon.update(lookup_result)
        else:
            logger.warning("Failed to get data from postcodes.io for chunk:", chunk)

    # Create DataFrame from the collected data
    data_for_export = [{'postcode': pc, 'coordinates': f"{coords[0]}, {coords[1]}"} 
    for pc, coords in all_lat_lon.items()]
    df_export = pd.DataFrame(data_for_export)
    return df_export

####
"""Rename Original Columns"""
def rename_cols(df):
    # Tidy the original column names with new ones 
    return df.rename(columns={
        'Street name': 'street_name',
        'Address': 'address',
        'Renewal Date': 'renewal_date',
        'Licence Holder': 'licence_holder',
        'Maximum Permitted Number of Tenants': 'max_tenants'
    })
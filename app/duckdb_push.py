import duckdb
from loguru import logger

def connect_to_motherduck(database, token):
    # Create connection object to MotherDuck
    if token is None:
        raise ValueError("MotherDuck token not found in environment variables")

    connection_string = f'md:{database}?motherduck_token={token}'
    con = duckdb.connect(connection_string)
    logger.success("Connection Made")
    return con

def load_into_duckdb(con, date, df):  
    # Create table in MotherDuck with final df 
    query = f"CREATE OR REPLACE TABLE leeds_hmo_{date} AS SELECT * FROM df"
    try:
        con.execute(query)
        logger.success("MotherDuck Upload Success")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
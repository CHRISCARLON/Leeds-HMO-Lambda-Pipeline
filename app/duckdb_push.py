import duckdb

def connect_to_motherduck(database, token):
    
    if token is None:
        raise ValueError("MotherDuck token not found in environment variables")

    connection_string = f'md:{database}?motherduck_token={token}'
    con = duckdb.connect(connection_string)
    print("Connection made")
    return con

def load_into_duckdb(con, date, df):  
    # Create table in db with final df  
    query = f"CREATE TABLE leeds_hmo_{date} AS SELECT * FROM df"
    try:
        con.execute(query)
        print("Upload success")
    except Exception as e:
        print(f"An error occurred: {e}")
import pyodbc
mssql_config = {"SERVER":"127.0.0.1",
                "DATABASE":"miniproject",
                "UID":"sa",
                "PWD":"Passw0rd"}
def get_conn_str():
    return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={mssql_config['SERVER']};DATABASE={mssql_config['DATABASE']};UID={mssql_config['UID']};PWD={mssql_config['PWD']}"

def get_jdbc_url():
    return f"jdbc:sqlserver://;serverName={mssql_config['SERVER']};databaseName={mssql_config['DATABASE']}"
 
def get_connection_properties():
    return {
        "user": f"{mssql_config['UID']}",
        "password": f"{mssql_config['PWD']}",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def get_engine_url():
    return f'mssql+pyodbc://{mssql_config["UID"]}:{mssql_config["PWD"]}@{mssql_config["SERVER"]}/{mssql_config["DATABASE"]}?driver=ODBC+Driver+17+for+SQL+Server'

'''
check the number of records of source and target
'''
def check_records_num(target_table,source_file,records_num):
    with pyodbc.connect(get_conn_str()) as conn:
        cursor = conn.cursor()
        # SQL query to get the number of rows in the table
        query = f"SELECT COUNT(*) FROM {target_table}"
        # Execute query
        cursor.execute(query)
        mssql_row_num = cursor.fetchone()[0]
        print(f"Number of rows in the MSSQL table: {mssql_row_num}")
        # Close the database connection
        cursor.close()
        if records_num == mssql_row_num:
            print(f"import successfully:file:{source_file},source record_count:{records_num} equal to target_table:{target_table},mssql_row_num:{mssql_row_num}")
        else:
            print(f"import failures:file:{source_file},source record_count:{records_num} equal to target_table:{target_table},mssql_row_num:{mssql_row_num}")    

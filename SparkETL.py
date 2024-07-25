from pyspark.sql import SparkSession
import pyodbc
import pandas as pd 
from sqlalchemy import create_engine
from datetime import datetime
import uuid

def check_records_num(table_name,source_file,records_num):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={mssql_config['SERVER']};" \
               f"DATABASE={mssql_config['DATABASE']};UID={mssql_config['UID']};PWD={mssql_config['PWD']}"
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        # SQL query to get the number of rows in the table
        query = f"SELECT COUNT(*) FROM {table_name}"
        # Execute query
        cursor.execute(query)
        mssql_row_num = cursor.fetchone()[0]
        print(f"Number of rows in the MSSQL table: {mssql_row_num}")
        # Close the database connection
        cursor.close()
        if records_num == mssql_row_num:
            print(f"import successfully:file:{source_file},record_count:{records_num} equal to sql_table:{table_name},mssql_row_num:{mssql_row_num}")
        else:
            print(f"import failures:file:{source_file},record_count:{records_num} equal to sql_table:{table_name},mssql_row_num:{mssql_row_num}")    

def parquet_to_mssql(parquet_file,sql_table):
    # Read Parquet file into DataFrame
    df = spark.read.parquet(f"source/{parquet_file}")
    # Count the number of records
    record_num = df.count()
    print(f"{parquet_file}'s Number of records: {record_num}")
    print(f"Records columns: {df.columns}")
    df.show(20, truncate=False)
    # Define JDBC connection properties
    jdbc_url = f"jdbc:sqlserver://;serverName={mssql_config['SERVER']};databaseName={mssql_config['DATABASE']}"
    connection_properties = {
        "user": f"{mssql_config['UID']}",
        "password": f"{mssql_config['PWD']}",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    # Write DataFrame to MSSQL
    # because this is a demo project, I use 'ignore' mode for the static data file. We get used to overwrite mode in the real project
    df.write.jdbc(url=jdbc_url, table=f"{sql_table}", mode="ignore", properties=connection_properties)
    check_records_num(sql_table,parquet_file,record_num)
    
def csv_to_mssql(csv_file,sql_table):
    df = pd.read_csv(f"source/{csv_file}")
    record_num = len(df)
    print(f"{csv_file}'s Number of records: {record_num}")
    print(f"Records columns: {df.columns}")
    print(df.head(20))
    #Create SQLAlchemy engine
    engine = create_engine(f'mssql+pyodbc://{mssql_config["UID"]}:{mssql_config["PWD"]}@{mssql_config["SERVER"]}/{mssql_config["DATABASE"]}?driver=ODBC+Driver+17+for+SQL+Server')
    #Load DataFrame into MSSQL
    df.to_sql(name=sql_table, con=engine, if_exists='replace', index=False)
    check_records_num(sql_table,csv_file,record_num)
    
def tramsform():
    #I will set these constants as configured parameters in real project instead of hard code
    parquet_to_mssql("fhv_tripdata_2023-01.parquet","fhv")
    parquet_to_mssql("fhvhv_tripdata_2023-01.parquet","fhvhv")
    parquet_to_mssql("green_tripdata_2023-01.parquet","green")
    parquet_to_mssql("yellow_tripdata_2023-01.parquet","yellow")
    csv_to_mssql("taxi_zone_lookup.csv",'taxi_zone')

def generate_date_dim():
   # Define the start and end dates
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 2, 1)
    # Generate a date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    # Create a DataFrame with the date dimension
    
    date_dim = pd.DataFrame({
        'date': date_range,
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'weekday': date_range.day_name(),
        'date_id': [i+1 for i in range(len(date_range))]  # Generate a unique ID for each date
    })
    print(date_dim.head(100))

def load():
    
    return #todo

def data_analysis():
    return #todo

generate_date_dim()

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.master("local[*]").appName("ParquetToMSSQL") \
        .config("spark.jars.repositories","https://maven-central.storage-download.googleapis.com/maven2/") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8") \
        .getOrCreate()
    # Set MSSQL configuration
    mssql_config = {"SERVER":"127.0.0.1",
                    "DATABASE":"miniproject",
                    "UID":"sa",
                    "PWD":"Passw0rd"}
    #1. Write scripts to transform the raw data into the designed schema.
    # tramsform()
    #2. Load the transformed data into the fact and dimension tables. 
    # load()
    '''3. Perform the following analyses using SQL queries:
    # - Calculate the total number of trips per day.
    # - Identify the top 5 zones with the highest total fare amount.
    # - Calculate the average trip distance by borough.
    # - Determine the most common pickup and dropoff locations.
    # - Calculate the total tip amount per passenger count.'''
    # data_analysis()
    # Stop Spark Session
    spark.stop()
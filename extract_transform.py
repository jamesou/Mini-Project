from pyspark.sql import SparkSession
import pandas as pd 
from sqlalchemy import create_engine
import tool

def parquet_to_mssql(parquet_file,sql_table):
    # Read Parquet file into DataFrame
    df = spark.read.parquet(f"source/{parquet_file}")
    # Count the number of records
    record_num = df.count()
    print(f"{parquet_file}'s Number of records: {record_num}")
    print(f"Records columns: {df.columns}")
    df.show(20, truncate=False)
    # Write DataFrame to MSSQL
    # because this is a demo project, I use 'ignore' mode for the static data file. We get used to overwrite mode in the real project
    df.write.jdbc(url=tool.get_jdbc_url(), table=f"{sql_table}", mode="ignore", properties=tool.get_connection_properties())
    tool.check_records_num(sql_table,parquet_file,record_num)
    
def csv_to_mssql(csv_file,sql_table):
    df = pd.read_csv(f"source/{csv_file}")
    record_num = len(df)
    print(f"{csv_file}'s Number of records: {record_num}")
    print(f"Records columns: {df.columns}")
    print(df.head(20))
    #Create SQLAlchemy engine
    engine = create_engine(tool.get_engine_url())
    #Load DataFrame into MSSQL
    df.to_sql(name=sql_table, con=engine, if_exists='replace', index=False)
    tool.check_records_num(sql_table,csv_file,record_num)
    
def extract_transform():
    #I will set these constants as configured parameters in real project instead of hard code
    parquet_to_mssql("fhv_tripdata_2023-01.parquet","fhv")
    parquet_to_mssql("fhvhv_tripdata_2023-01.parquet","fhvhv")
    parquet_to_mssql("green_tripdata_2023-01.parquet","green")
    parquet_to_mssql("yellow_tripdata_2023-01.parquet","yellow")
    csv_to_mssql("taxi_zone_lookup.csv",'taxi_zone')

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.master("local[*]").appName("ParquetToMSSQL") \
        .config("spark.jars.repositories","https://maven-central.storage-download.googleapis.com/maven2/") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8") \
        .getOrCreate()
    #Write scripts to transform the raw data into the designed schema.
    extract_transform()
    # Stop Spark Session
    spark.stop()
from pyspark.sql import SparkSession
import pyodbc
import pandas as pd 
from sqlalchemy import create_engine
from datetime import datetime
import tool
def load_date_dim():
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
    with pyodbc.connect(tool.get_conn_str()) as conn:
        cursor = conn.cursor()
        table_name = "date_dim"
        delete_sql = f"delete from {table_name};"
        cursor.execute(delete_sql)
        # Define the insert query
        insert_query = f""" INSERT INTO {table_name} (date, year, month, day, weekday) VALUES (?, ?, ?, ?, ?) """
        for index, row in date_dim.iterrows():
            cursor.execute(insert_query,  row['date'], row['year'], row['month'], row['day'], row['weekday'])
        # Commit the transaction
        conn.commit()
        #Close the connection
        cursor.close()
    tool.check_records_num(table_name,None,len(date_dim))

def get_affected_rows(cursor):
    cursor.execute("SELECT @@ROWCOUNT;")
    affected_rows = cursor.fetchone()[0]
    return affected_rows

def load_location_dim():
    with pyodbc.connect(tool.get_conn_str()) as conn:
        cursor = conn.cursor()
        source_table="taxi_zone"
        execute_sql = f"""
            MERGE location_dim AS target
            USING (
                SELECT
                    LocationID AS location_id,
                    Borough AS borough,
                    Zone AS zone,
                    service_zone AS service_zone
                FROM {source_table}
            ) AS source
            ON (target.location_id = source.location_id)
            WHEN MATCHED THEN
                UPDATE SET 
                    target.borough = source.borough,
                    target.zone = source.zone,
                    target.service_zone = source.service_zone
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (location_id, borough, zone, service_zone)
                VALUES (source.location_id, source.borough, source.zone, source.service_zone);
    """
        print(f"load_location_dim:execute_sql:{execute_sql}")
        # Commit the transaction
        cursor.execute(execute_sql)
        conn.commit()
        records_num = get_affected_rows(cursor)
        #Close the connection
        cursor.close()
    tool.check_records_num(source_table,None,records_num)

def load_fhvhv(target_table):
    source_table = "fhvhv"
    pickup_datetime = "Pickup_datetime"
    pickup_location_id = "PULocationID"
    dropoff_datetime = "DropOff_datetime"
    dropoff_location_id = "DOLocationID"
    with pyodbc.connect(tool.get_conn_str()) as conn:
        try:
             fare_amount = "base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee"
             sql=f"""
             select {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}, 
                    0 as passenger_count,trip_miles as Trip_distance,({fare_amount}) as fare_amount,
                    tips as tip_amount,({fare_amount}+tips) as total_amount,
                    ROW_NUMBER() over (PARTITION by {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}
                                        order by Dispatching_base_num) as row_num 
                    from {source_table} where  {pickup_datetime} is not null 
                    and {pickup_location_id} is not null 
                    and {dropoff_datetime} is not null 
                    and {dropoff_location_id} is not null
             """
             query_sql = f"select count(*) from ({sql}) cleaned_table where row_num=1 and total_amount >0 and fare_amount >=0"
             cursor = conn.cursor()
             print(f"query_sql:{query_sql}")
             cursor.execute(query_sql)
             source_row_num = cursor.fetchone()[0]
             #67963,3040990 rows
             execute_sql = f"""
                        MERGE {target_table} AS target 
                        USING (
                            select * from (
                                 {sql}
                            ) cleaned_table where row_num=1 and total_amount >0 and fare_amount >=0
                        ) AS source 
                        ON (
                        target.pickup_datetime = source.{pickup_datetime} AND
                        target.pickup_location_id = source.{pickup_location_id} AND
                        target.dropoff_datetime = source.{dropoff_datetime} AND
                        target.dropoff_location_id = source.{dropoff_location_id}
                        )
                        WHEN MATCHED THEN
                            UPDATE SET 
                                target.passenger_count = source.passenger_count,
                                target.trip_distance = source.trip_distance,
                                target.fare_amount = source.fare_amount,
                                target.Tip_amount = source.Tip_amount,
                                target.Total_amount = source.Total_amount
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (pickup_datetime, pickup_location_id, dropoff_datetime, dropoff_location_id,
                                    passenger_count,trip_distance,fare_amount,Tip_amount,Total_amount)
                            VALUES (source.{pickup_datetime}, source.{pickup_location_id}, source.{dropoff_datetime}, source.{dropoff_location_id},
                                    source.passenger_count, source.trip_distance, source.fare_amount, source.Tip_amount,source.Total_amount);
                        """
             print(f"execute_sql:{execute_sql}")
             cursor.execute(execute_sql)
             conn.commit
             affected_rows = get_affected_rows(cursor)
             print(f"source_table:{source_table},source_row_num:{source_row_num},affected_rows:{affected_rows}")
        except pyodbc.Error as e:
            #We will use logging lib in real project
            print(f"An error occurred: {e}")
        finally:
            # Close cursor
            cursor.close()
def load_fhv(target_table):
    source_table = "fhv"
    pickup_datetime = "Pickup_datetime"
    pickup_location_id = "PULocationID"
    dropoff_datetime = "DropOff_datetime"
    dropoff_location_id = "DOLocationID"
    with pyodbc.connect(tool.get_conn_str()) as conn:
        try:
             sql=f"""
             select {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}, 
                    0 as passenger_count,0 as fare_amount,0 as tip_amount,0 as total_amount, 
                    ROW_NUMBER() over (PARTITION by {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}
                                        order by Dispatching_base_num) as row_num 
                    from {source_table} where  {pickup_datetime} is not null 
                    and {pickup_location_id} is not null 
                    and {dropoff_datetime} is not null 
                    and {dropoff_location_id} is not null
             """
             query_sql = f"select count(*) from ({sql}) cleaned_table where row_num=1"
             cursor = conn.cursor()
             print(f"query_sql:{query_sql}")
             cursor.execute(query_sql)
             source_row_num = cursor.fetchone()[0]
             #67963,3040990 rows
             execute_sql = f"""
                        MERGE {target_table} AS target 
                        USING (
                            select * from (
                                 {sql}
                            ) cleaned_table where row_num=1  
                        ) AS source 
                        ON (
                        target.pickup_datetime = source.{pickup_datetime} AND
                        target.pickup_location_id = source.{pickup_location_id} AND
                        target.dropoff_datetime = source.{dropoff_datetime} AND
                        target.dropoff_location_id = source.{dropoff_location_id}
                        )
                        WHEN MATCHED THEN
                            UPDATE SET 
                                target.passenger_count = source.passenger_count,
                                target.fare_amount = source.fare_amount,
                                target.Tip_amount = source.Tip_amount,
                                target.Total_amount = source.Total_amount
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (pickup_datetime, pickup_location_id, dropoff_datetime, dropoff_location_id,
                                    passenger_count,fare_amount,Tip_amount,Total_amount)
                            VALUES (source.{pickup_datetime}, source.{pickup_location_id}, source.{dropoff_datetime}, source.{dropoff_location_id},
                                    source.passenger_count, source.fare_amount, source.Tip_amount,source.Total_amount);
                        """
             print(f"execute_sql:{execute_sql}")
             cursor.execute(execute_sql)
             conn.commit
             affected_rows = get_affected_rows(cursor)
             print(f"source_table:{source_table},source_row_num:{source_row_num},affected_rows:{affected_rows}")
        except pyodbc.Error as e:
            #We will use logging lib in real project
            print(f"An error occurred: {e}")
        finally:
            # Close cursor
            cursor.close()

'''
clean data sql script
select * from (
    select lpep_pickup_datetime,PULocationID,lpep_dropoff_datetime,DOLocationID,
    Passenger_count,Trip_distance,Fare_amount,Tip_amount,Total_amount,
    ROW_NUMBER() over (PARTITION by lpep_pickup_datetime,PULocationID,lpep_dropoff_datetime,DOLocationID
                        order by total_amount desc) as row_num
    from green where  lpep_pickup_datetime is not null 
    and PULocationID is not null
    and lpep_dropoff_datetime is not null
    and DOLocationID is not null 
    and total_amount >0 --remove invalid data for accuracy analysis
    and fare_amount >0  --remove invalid data for accuracy analysis 
) cleaned_green where row_num=1 --get the one of duplicate data
'''
def load_green_yellow(target_table,source_table,pickup_datetime,pickup_location_id,dropoff_datetime,dropoff_location_id):
    with pyodbc.connect(tool.get_conn_str()) as conn:
        try:
             sql=f"""
             select {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}, 
                    Passenger_count,Trip_distance,Fare_amount,Tip_amount,Total_amount, 
                    ROW_NUMBER() over (PARTITION by {pickup_datetime},{pickup_location_id},{dropoff_datetime},{dropoff_location_id}
                                        order by total_amount desc) as row_num 
                    from {source_table} where  {pickup_datetime} is not null 
                    and {pickup_location_id} is not null 
                    and {dropoff_datetime} is not null 
                    and {dropoff_location_id} is not null
                    and total_amount >0
                    and fare_amount >=0
             """
             query_sql = f"select count(*) from ({sql}) cleaned_table where row_num=1"
             cursor = conn.cursor()
             print(f"query_sql:{query_sql}")
             cursor.execute(query_sql)
             source_row_num = cursor.fetchone()[0]
             #67963,3040990 rows
             execute_sql = f"""
                        MERGE {target_table} AS target 
                        USING (
                            select * from (
                                 {sql}
                            ) cleaned_table where row_num=1  
                        ) AS source 
                        ON (
                        target.pickup_datetime = source.{pickup_datetime} AND
                        target.pickup_location_id = source.{pickup_location_id} AND
                        target.dropoff_datetime = source.{dropoff_datetime} AND
                        target.dropoff_location_id = source.{dropoff_location_id}
                        )
                        WHEN MATCHED THEN
                            UPDATE SET 
                                target.passenger_count = source.passenger_count,
                                target.trip_distance = source.trip_distance,
                                target.fare_amount = source.fare_amount,
                                target.Tip_amount = source.Tip_amount,
                                target.Total_amount = source.Total_amount
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (pickup_datetime, pickup_location_id, dropoff_datetime, dropoff_location_id,
                                    passenger_count,trip_distance,fare_amount,Tip_amount,Total_amount)
                            VALUES (source.{pickup_datetime}, source.{pickup_location_id}, source.{dropoff_datetime}, source.{dropoff_location_id},
                                    source.passenger_count, source.trip_distance, source.fare_amount, source.Tip_amount,source.Total_amount);
                        """
             print(f"execute_sql:{execute_sql}")
             cursor.execute(execute_sql)
             conn.commit
             affected_rows = get_affected_rows(cursor)
             print(f"source_table:{source_table},source_row_num:{source_row_num},affected_rows:{affected_rows}")
        except pyodbc.Error as e:
            #We will use logging lib in real project
            print(f"An error occurred: {e}")
        finally:
            # Close cursor
            cursor.close()

def load_green(target_table):
    source_table = "green"
    pickup_datetime = "lpep_pickup_datetime"
    pickup_location_id = "PULocationID"
    dropoff_datetime = "lpep_dropoff_datetime"
    dropoff_location_id = "DOLocationID"
    load_green_yellow(target_table,source_table,pickup_datetime,pickup_location_id,dropoff_datetime,dropoff_location_id)

def load_yellow(target_table):
    source_table = "yellow"
    pickup_datetime = "tpep_pickup_datetime"
    pickup_location_id = "PULocationID"
    dropoff_datetime = "tpep_dropoff_datetime"
    dropoff_location_id = "DOLocationID"
    load_green_yellow(target_table,source_table,pickup_datetime,pickup_location_id,dropoff_datetime,dropoff_location_id)
   
     
def load_trips_fact():
     target_table="trips_fact"
     load_green(target_table)
     load_yellow(target_table)
     load_fhv(target_table)
     load_fhvhv(target_table)

def load():
    load_date_dim()
    load_location_dim()
    load_trips_fact()
  

if __name__ == "__main__":
    #Load the transformed data into the fact and dimension tables. 
    load()
 
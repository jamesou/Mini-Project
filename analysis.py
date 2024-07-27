import pandas as pd 
from sqlalchemy import create_engine
import pyodbc
import tool
 
 
def daily_trip_totals(cursor):
    table_name = 'daily_trip_totals'
    sql = f"truncate table {table_name}"
    print(f"sql:{sql}")
    cursor.execute(sql)
    sql=f"""
    INSERT INTO daily_trip_totals (date, total_trips)
    SELECT  dd.date, COUNT(tf.trip_id) AS total_trips
        FROM date_dim dd left join trips_fact tf on dd.date = CAST(pickup_datetime AS DATE)
    GROUP BY 
        dd.date
    ORDER BY 
        dd.date;
    """
    print(f"sql:{sql}")
    cursor.execute(sql)
    print(f"daily_trip_totals successfully!")
   
def top_zone_fare(cursor):
    table_name = 'top_zone_fare'
    sql = f"truncate table {table_name}"
    print(f"sql:{sql}")
    cursor.execute(sql)
    sql=f"""
    INSERT INTO {table_name} (zone, total_fare)
    select top 5 * from (
        SELECT 
            ld.zone,
            SUM(tf.fare_amount) AS total_fare_amount
        FROM 
            trips_fact tf left JOIN location_dim ld ON tf.pickup_location_id = ld.location_id
        GROUP BY ld.zone
    ) a ORDER BY total_fare_amount DESC
    """
    print(f"sql:{sql}")
    cursor.execute(sql)
    print(f"top_zone_fare successfully!")
    
def avg_trip_dist(cursor):
    table_name = 'avg_trip_dist'
    sql = f"truncate table {table_name}"
    print(f"sql:{sql}")
    cursor.execute(sql)
    #filter the FHV data
    sql=f"""
    INSERT INTO {table_name} (borough, avg_trip_distance)
    SELECT ld.borough, AVG(tf.trip_distance) AS average_trip_distance
    FROM trips_fact tf
    LEFT JOIN location_dim ld ON  tf.pickup_location_id  = ld.location_id
    where trip_distance is not NULL  
    GROUP BY 
        ld.borough
    ORDER BY 
        average_trip_distance DESC;
    """
    print(f"sql:{sql}")
    cursor.execute(sql)
    print(f"top_zone_fare successfully!")

def common_locations(cursor):
    table_name = 'common_locations'
    sql = f"truncate table {table_name}"
    print(f"sql:{sql}")
    cursor.execute(sql)
    #filter the FHV data
    sql=f"""
    INSERT INTO common_locations (location_type, zone, trip_count)
    select * from (
        SELECT top 8 
            'pickup' AS location_type,
            ld.zone AS zone,
            COUNT(tf.trip_id) AS trip_count
        FROM 
            trips_fact tf
        LEFT JOIN 
            location_dim ld ON tf.pickup_location_id = ld.location_id
        GROUP BY 
            ld.zone
        ORDER BY trip_count DESC 
    ) x
	UNION ALL 
    select * from (	
        SELECT top 8  
            'dropoff' AS location_type,
            ld.zone AS zone,
            COUNT(tf.trip_id) AS trip_count
        FROM 
            trips_fact tf
        LEFT JOIN 
            location_dim ld ON tf.dropoff_location_id = ld.location_id
        GROUP BY 
            ld.zone
        ORDER BY trip_count DESC 
    ) y
    """
    print(f"sql:{sql}")
    cursor.execute(sql)
    print(f"common_locations successfully!")

def total_tip_passenger(cursor):
    table_name = 'total_tip_passenger'
    sql = f"truncate table {table_name}"
    print(f"sql:{sql}")
    cursor.execute(sql)
    #filter the FHV data
    sql=f"""
    INSERT INTO total_tip_passenger (passenger_count, total_tip)
    select * from (
        SELECT 
            tf.passenger_count,
            SUM(tf.tip_amount) AS total_tip_amount
        FROM 
            trips_fact tf
        WHERE passenger_count is not null and passenger_count>0
        GROUP BY
            tf.passenger_count
    ) x ORDER BY total_tip_amount desc;
    """
    print(f"sql:{sql}")
    cursor.execute(sql)
    print(f"total_tip_passenger successfully!")

def data_analysis():
    '''3. Perform the following analyses using SQL queries:
    # - Calculate the total number of trips per day.
    # - Identify the top 5 zones with the highest total fare amount.
    # - Calculate the average trip distance by borough.
    # - Determine the most common pickup and dropoff locations.
    # - Calculate the total tip amount per passenger count.'''
    with pyodbc.connect(tool.get_conn_str()) as conn:
        try:
             cursor = conn.cursor()
             daily_trip_totals(cursor)
             top_zone_fare(cursor)
             avg_trip_dist(cursor)
             common_locations(cursor)
             total_tip_passenger(cursor)
             conn.commit()
             print(f"produce data insight successfully!")
        except pyodbc.Error as e:
            #We will use logging lib in real project
            print(f"An error occurred: {e}")
        finally:
            # Close cursor
            cursor.close()
    
if __name__ == "__main__":
    data_analysis()
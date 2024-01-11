import duckdb
import os
import requests
from . import constants
from dagster import asset

@asset
def taxi_trips_file():
    """The raw taxi trips data fetched from NYCData in a parquet file"""
    month_to_fetch = "2023-03"
    try:
        raw_trips_response = requests.get(
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
        )
        raw_trips_response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code

        with open(
            constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
        ) as output_file:
            output_file.write(raw_trips_response.content)
    except Exception as e:
        print(f"Error fetching or writing taxi trips data: {e}")

@asset
def taxi_zones_file():
    """The raw taxi zones data fetched for NYCData in a csv file"""
    try:
        raw_taxi_zones_response = requests.get(
            "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
        )
        raw_taxi_zones_response.raise_for_status()

        with open(
            constants.TAXI_ZONES_FILE_PATH, "wb"
        ) as output_file:
            output_file.write(raw_taxi_zones_response.content)
    except Exception as e:
        print(f"Error fetching or writing taxi zones data: {e}")

@asset(deps=["taxi_trips_file"])
def taxi_trips():
    """taxi_trips table generated from taxi_trips_file loaded in DuckDB"""
    sql_query = """
        create or replace table trips as (
            select
                VendorID as vendor_id,
                PULocationID as pickup_zone_id,
                DOLocationID as dropoff_zone_id,
                RatecodeID as rate_code_id,
                payment_type as payment_type,
                tpep_dropoff_datetime as dropoff_datetime,
                tpep_pickup_datetime as pickup_datetime,
                trip_distance as trip_distance,
                passenger_count as passenger_count,
                total_amount as total_amount
            from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """
    conn = None
    try:
        conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
        conn.execute(sql_query)
    except Exception as e:
        print(f"Error loading taxi trips data into DuckDB: {e}")
    finally:
        if conn:
            conn.close()

@asset(deps=["taxi_zones_file"])
def taxi_zones():
    """taxi_zones table generated from taxi_zones_file loaded in DuckDB """
    sql_query = """
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from 'data/raw/taxi_zones.csv'
        );
    """
    conn = None
    try:
        conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
        conn.execute(sql_query)
    except Exception as e:
        print(f"Error loading taxi zones data into DuckDB: {e}")
    finally:
        if conn:
            conn.close()

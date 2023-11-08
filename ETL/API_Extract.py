import pandas as pd
import getpass
import snowflake.connector

def snowflake_connect():
    # Connection parameters for Pagila Database
    my_user = input("Input your Snowflake username: ")
    pwd = getpass.getpass("Input your Snowflake Password: ")

    # Set up the connection parameters
    account = 'lg11988.eu-west-2.aws'
    user = my_user
    password = pwd
    warehouse = 'COMPUTE_WH'
    database = 'UFC_EVENTS'
    schema = 'PUBLIC'

    # Connect to Snowflake
    global conn, cur
    
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    
    
    cur = conn.cursor()

    return cur, conn


def api_load():

    # URL of the COVID-19 API
    #event = input("Please enter the UFC event number: ")
    #fighter = input("Please enter full name of the Fighter: ")
    global api_url
    api_url = f"https://fightingtomatoes.com/API/40f545077d95388cb0ec9e354e131c6db126f7c4/any/any/any"

    return api_url
    
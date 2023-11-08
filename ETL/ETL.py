import requests
import json
import snowflake.connector
import getpass
import numpy as np
import pandas as pd

def extract_data():
    # Your existing extract_data function
    api_url = "https://fightingtomatoes.com/API/40f545077d95388cb0ec9e354e131c6db126f7c4/any/any/any"
    response = requests.get(api_url)
    try:
        # Send a GET request to the API
        response = requests.get(api_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            response_text = response.text
        else:
            print(f"Request failed with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    
    # Find the start and end of the JSON data within the response_text
    start = response_text.find("[{")
    end = response_text.rfind("}]") + 2  # Adding 2 to include the closing bracket

    # Extract the JSON data
    json_data = response_text[start:end]

    data = json.loads(json_data)
    return data

def transform_load(data):
    # Your existing transform_load function
    ddict_main = []
    ddict_small = []

    my_user = input("Input your Snowflake username: ")
    pwd = getpass.getpass("Input your Snowflake Password: ")

    # Set up the connection parameters
    account = 'lg11988.eu-west-2.aws'
    user = my_user
    password = pwd
    warehouse = 'COMPUTE_WH'
    database = 'UFC_EVENTS'
    schema = 'PUBLIC'

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    
    cur = conn.cursor()

    for fights in data:
        fight_data = {
                'UFCEVENT': fights['event'],    
                'FightPriority': fights['card_placement'],
                'Fighter1': fights['fighter_1'],
                'Fighter2': fights['fighter_2'],
                'Winner': fights['winner'],
                'Round': fights['round'],
                'Method': fights['method'],
                'Rating': fights['fighting_tomatoes_aggregate_rating'],
                'Date': fights['date']
            }
            # Append into a list for every fight
        ddict_main.append(fight_data)

    sorted_dict = sorted(ddict_main, key=lambda x: int(x['FightPriority']))

    for fights in data:
        fight_data = {}
        card_placement = fights.get('card_placement')
        if card_placement == str(1):
            fight_data.update({
                'UFCEVENT': fights['event'],  # Use the correct key 'event'
                'FightPriority': card_placement,
                'Fighter1': fights['fighter_1'],
                'Fighter2': fights['fighter_2'],
                'Winner': fights['winner'],
                'Round': fights['round'],
                'Method': fights['method'],
                'Rating': fights['fighting_tomatoes_aggregate_rating'],
                'Date': fights['date']
            })
            # Append into a list for every fight
            ddict_small.append(fight_data)
            #print(fight_data)  # Add a print statement to check the dictionary
        #else:
            #print(f"Skipping fight with card_placement: {card_placement}")

#print("dict", ddict)

    
    #print(sorted_dict)

    try:
        
        
        # This creates SQL table for all the events and for a specific fighter
            
            print("Attempting Small Table")
            
            # Create table and name it after the event number the User chooses
            create_table_sql = f"""
            CREATE OR REPLACE TABLE UFC_SMALL (
                UFCEVENT STRING,
                FightPriority NUMBER,
                Fighter1 STRING,
                Fighter2 STRING,
                Winner STRING,
                Round NUMBER,
                Method STRING,
                Rating NUMBER,
                Date DATE
                
            )
            """

            # Execute the SQL statement to create the table
            cur.execute(create_table_sql)
            
            # Define the SQL query to insert a row into the table
            insert_query = f"INSERT INTO UFC_SMALL (UFCEVENT, FightPriority, Fighter1, Fighter2, Winner, Round, Method, Rating, Date) VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s)"

            # Extract the values from the dictionary and convert them to the correct data types
            values = [(d['UFCEVENT'], int(d['FightPriority']), d['Fighter1'], d['Fighter2'], d['Winner'], int(d['Round']), d['Method'] , d['Rating'] , d['Date']) for d in ddict_small]
            #values = [(d.get('UFCEVENT', ''), int(d.get('FightPriority', 0)), d.get('Fighter1', ''), d.get('Fighter2', ''), d.get('Winner', ''), int(d.get('Round', 0)), d.get('Method', ''), d.get('Rating', 0), d.get('Date', '')) for d in ddict]
            #print(values)
            cur.executemany(insert_query, values)
            conn.commit()
        
        # This creates SQL table for all the events and every fighter either main or all cards
            
            print("Attempting Main Table")
            # Create table and name it after the event number the User chooses
            create_table_sql = """
            CREATE OR REPLACE TABLE UFC_MAIN (
                UFCEVENT STRING,
                FightPriority NUMBER,
                Fighter1 STRING,
                Fighter2 STRING,
                Winner STRING,
                Round NUMBER,
                Method STRING,
                Rating NUMBER,
                Date DATE
                
            )
            """

            # Execute the SQL statement to create the table
            cur.execute(create_table_sql)
            
            # Define the SQL query to insert a row into the table
            insert_query2 = "INSERT INTO UFC_MAIN (UFCEVENT, FightPriority, Fighter1, Fighter2, Winner, Round, Method, Rating, Date) VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s)"

            # Extract the values from the dictionary and convert them to the correct data types
            values2 = [(d['UFCEVENT'], int(d['FightPriority']), d['Fighter1'], d['Fighter2'], d['Winner'], int(d['Round']), d['Method'] , d['Rating'] , d['Date']) for d in ddict_main]
            #values = [(d.get('UFCEVENT', ''), int(d.get('FightPriority', 0)), d.get('Fighter1', ''), d.get('Fighter2', ''), d.get('Winner', ''), int(d.get('Round', 0)), d.get('Method', ''), d.get('Rating', 0), d.get('Date', '')) for d in ddict]
            #print(values)
            cur.executemany(insert_query2, values2)
            conn.commit()
            print("Tables Loaded Successfully")
 

    #except Exception as e:
        #print(f"An error occurred: {str(e)}")
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()
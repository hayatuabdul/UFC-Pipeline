import numpy as np
import pandas as pd
import os
from API_Extract import snowflake_connect, api_load


api_url, event, fighter = api_load()
cur, conn = snowflake_connect()


def sql_to_df():
    # This function allows the User to directly query data from the Snowflake Database. This method doesn't rely on the API
    try:  
            sql_fighter = input("Please enter the name of your fighter: ")
            query = f"""
                    SELECT
                        DISTINCT(UFCEVENT),
                        FIGHTPRIORITY,
                        FIGHTER1,
                        FIGHTER2,
                        WINNER,
                        ROUND,
                        METHOD,
                        RATING,
                        DATE

                    FROM
                        UFC_EVENTS.PUBLIC.UFC_MAIN
                    WHERE
                        FIGHTER1 ILIKE '{sql_fighter}' 
                        OR FIGHTER2 ILIKE '{sql_fighter}' 
                    ORDER BY
                        DATE DESC;
                """

            cur.execute(query)

            # Fetch all the results
            results = cur.fetchall()
            df_sql = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
            
            
            sql_fighter_name = sql_fighter.split()
            sql_new_fighter_name = '_'.join(sql_fighter_name)
            # Create table and name it after the event number the User chooses
            create_table_sql = f"""
            CREATE OR REPLACE TABLE UFC_{sql_new_fighter_name} (
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
            
            
            sql_data = [tuple(x) for x in df_sql.to_records(index=False)]
            #print(sql_data)
            
            # Define the SQL query to insert a row into the table
            sql_insert_query = f"INSERT INTO UFC_{sql_new_fighter_name} (UFCEVENT, FightPriority, Fighter1, Fighter2, Winner, Round, Method, Rating, Date) VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s)"

            cur.executemany(sql_insert_query, sql_data)
            conn.commit()
            # Convert the results to a DataFrame
            
            #print(df_sql)
            
            csv_filename2 = f"UFC_{sql_fighter}.csv"
            # Check if the CSV file already exists
            if os.path.isfile(csv_filename2):
                # If it exists, open it in "write" mode to overwrite the existing content
                mode = 'w'
            else:
                # If it doesn't exist, open it in "write" mode to create a new file
                mode = 'x'

            # Write the DataFrame to the CSV file
            df_sql.to_csv(csv_filename2, index=False, mode=mode)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    sql_to_df()
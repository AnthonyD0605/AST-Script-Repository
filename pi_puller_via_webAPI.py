import requests
import os
import json
import time
from datetime import datetime
import pandas as pd
import snowflake.connector
import glob

SNOWFLAKE_QUERY = """select circuit_id, pi_tag_web_id, pi_tag_name
                    from prod_edm_prod_db.pi_asset_framework.af_feeder_mapping
                    where mapping_method != 'UNMAPPED'
                        and not endswith(pi_tag_name, '.IV')
                    order by circuit_id;
                """
VERSION = "1.0"

WORKING_DIR = r'C:\Users\deyoea\Documents\GridternProjects\Pi Data Related Projects'
OUTPUT_DIR = os.path.join(WORKING_DIR, "pi output")
RAW_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "RAW_OUTPUT")


# LOGGER CLASS TO GIVE US THE MORE CLEAN PRINT STATEMENTS
class Logger(object):
    def __init__(self, output_dir="", write_to_log_file=False):
        self.output_dir = output_dir
        self.log_file_name = "Status Log - {} - {}.txt".format(os.path.basename(__file__), datetime.now().strftime("%m-%d-%Y_%H.%M.%S"))
        self.write_to_console = True
        self.write_to_log_file = write_to_log_file

    def enable_file_output(self, output_dir):
        self.output_dir = output_dir
        self.write_to_log_file = True

    def disable_file_output(self):
        self.write_to_log_file = False

    def log_message(self, message, indent_level=0):
        if self.write_to_log_file:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

        message = message.rjust(len(message) + 4 * indent_level, " ")

        if self.write_to_console:
            print("{} | {}".format(datetime.now().strftime("%H:%M:%S.%f")[:-3], message))

        if self.write_to_log_file:
            with open(os.path.join(self.output_dir, self.log_file_name), 'a') as f:
                f.write("{} | {}\n".format(datetime.now().strftime("%H:%M:%S.%f")[:-3], message))
        return

# Declare a global logger that can be used throughout the program.
global logger
logger = Logger(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Status Logs"), False)

def snowflake_connector():
    SNOWFLAKE_CREDS = {
        'account': 'ngprod.east-us-2.privatelink',
        'port': 443,
        'user': 'anthony.deyoe@us.nationalgrid.com',
        'database': 'PROD_EDM_PROD_DB',
        'schema': 'PI_HISTORIAN',
        'warehouse': 'PROD_OMS_QRY_WH',
        'role': 'AZ_APP_SNOWFLAKEPROD_GS_EDM_READ_TX_PII_PROD',
        'authenticator': 'externalbrowser'
    }
    
    ctx = snowflake.connector.connect(
        account=SNOWFLAKE_CREDS['account'],
        port=SNOWFLAKE_CREDS['port'],
        user=SNOWFLAKE_CREDS['user'],
        database=SNOWFLAKE_CREDS['database'],
        schema=SNOWFLAKE_CREDS['schema'],
        warehouse=SNOWFLAKE_CREDS['warehouse'],
        role=SNOWFLAKE_CREDS['role'],
        authenticator=SNOWFLAKE_CREDS['authenticator']
    )
    
    return ctx

def snowflake_query_to_df(conn, sql_query):
    cur = conn.cursor().execute(sql_query)
    df = pd.DataFrame.from_records(iter(cur), columns=[col.name for col in cur.description])
    return df

def json_to_dataframe(pi_tag_web_id):
    # Define the base URL for the PI Web API
    base_url = "https://azcpwppusa01.na.ngrid.net:444/piwebapi"

    # Set up authentication (if required)
    username = "anthony.deyoe@us.nationalgrid.com"
    password = "AdBdCdCd2002."
    auth = (username, password)

    # Define the endpoint for the specific data you want to retrieve
    stream_id = pi_tag_web_id
    start_time = "2024-01-01"
    end_time = "2025-01-01"
    summary_type = "Average"
    summary_duration = "1h"
    interval = "1h"

    # Construct the full URL for the API request
    url = f"{base_url}/streams/{stream_id}/summary?startTime={start_time}&endTime={end_time}&summaryType={summary_type}&summaryDuration={summary_duration}&interval={interval}"

    # Send a GET request to the URL
    response = requests.get(url, auth=auth, verify=False)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        pi_data = response.json()
        
        # Extract relevant data (example: timestamps and values)
        results = []

        for data in pi_data.get('Items', []):
            value = data.get('Value')
            timestamp = value.get('Timestamp')
            recorded_value = value.get('Value')
            if recorded_value == None:
                recorded_value = 0
            # Append a dictionary with keys 'timestamp' and 'value'
            results.append([timestamp, recorded_value])
        
        # Create a DataFrame from the results
        df = pd.DataFrame(results, columns=['Timestamp', 'Recorded Value'])  # Specify column order
        return df  # Return the DataFrame
    else:
        print(f"Failed to retrieve data: {response.status_code} - {response.text}")
        return None  # Return None if the request fails

def aggregate_raw_pi_to_feeder(feeder):
    # Get all raw files that match the feeder circuit_id
    list_of_raw_files = glob.glob(os.path.join(RAW_OUTPUT_DIR, f"{feeder}_*.csv"))
    
    output_headers = ["Timestamp"]
    df_agg_pi = pd.DataFrame()
    for file in list_of_raw_files:
        tag_name = str("_".join(os.path.basename(file).split('_')[3:]).replace(".csv", ""))
        output_headers.append(tag_name)
        df_raw_pi = pd.read_csv(file, header=None, dtype=object, names=["Timestamp", tag_name])
        #Dropping the extra row with the timestamp and 'recorded value'
        df_raw_pi.drop(df_raw_pi.index[0], inplace=True)
        if df_agg_pi.empty:
            # If this is the first file, set df_agg_pi to raw pi
            df_agg_pi = df_raw_pi
        else:
            # If df_agg_pi contains data, only pull the values from df_raw_pi and add as additional column
            df_agg_pi[tag_name] = df_raw_pi[tag_name]

    # Now write aggregated PI data to new file
    output_file = os.path.join(OUTPUT_DIR, f"{feeder}.csv")
    df_agg_pi.to_csv(output_file,index=False)

    # Now that an aggregated output file has been written, delete the raw files.
    for file in list_of_raw_files:
        os.remove(file)

    return output_file

def main():
    logger.log_message("Script '{}' (v{}) execution started at {}".format(os.path.basename(__file__), VERSION,
                                                                          datetime.now().strftime(
                                                                              "%H:%M:%S on %m-%d-%Y")))
    start_time = time.time()
    
    global conn
    conn = snowflake_connector()
    
    pi_asset_framework = snowflake_query_to_df(conn, SNOWFLAKE_QUERY)
    for index, row in pi_asset_framework.iterrows():
        web_id = row["PI_TAG_WEB_ID"]
        circuit_id = row["CIRCUIT_ID"]  # Assuming CIRCUIT_ID is the column name in the DataFrame
        measurement_value = row["PI_TAG_NAME"]
        df = json_to_dataframe(web_id)
        if df is not None:
            # Save the DataFrame to a CSV file using circuit_id
            csv_file_name = f"{circuit_id}_{measurement_value}.csv"  # Create a unique filename for each circuit_id
            df.to_csv(os.path.join(RAW_OUTPUT_DIR, csv_file_name), index=False)  # Save DataFrame to CSV without the index
            logger.log_message(f"Data for circuit ID {circuit_id} saved to {csv_file_name}.")
    # Aggregate raw files for each unique circuit_id\
    filtered_af = pi_asset_framework['CIRCUIT_ID'].unique()
    for feeder in filtered_af:
        print(f"Combining files for feeder {feeder}")
        data_output_file = aggregate_raw_pi_to_feeder(feeder)
    logger.log_message("Processed {} networks in {:.1f}s.".format(len(pi_asset_framework), time.time() - start_time))
    
if __name__ == "__main__":
    main()

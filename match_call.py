import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

# Set the local path for where you want to save your data
local_path = '/Users/evando/code/riot/match_data'

# # Initialize an empty dictionary to store the API responses
# match_data = {}
# prefix = 'NA1'
# num_start = 5147448578
# match_ids = [f"{prefix}_{num_start + i}" for i in range(1)]
# api_key = os.getenv("RIOT_API_KEY")



# Set list of match_ids we would like to ingest
match_ids = []

# Start by retrieving match_list for one player
puuid = 'TgWQ6HYb9hq14-S8hWWq_PebGl-ERUOTMysqGzUBMETXYPIqDVgZi5AufN_K1kS-_98oAjc8ypKaLQ'
# define function to return to us the initial match we will use to crawl
def initial_match(puuid):
    api_url_m = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=20&api_key=RGAPI-29cf9fca-b991-4935-87e4-72b62be9e585"
    response = requests.get(api_url_m)  # Returns the response as a Python response object
    if response.status_code == 200:
        match_list = response.json()
        print(f"API call success for match {puuid}")
        # take just the first value within the list to test for requirements
        return print(match_list[0])

# Define requirements for the match to be added to the list of matches to be ingested
# condition the loop to run until the list hits 1000 matches
def match_filtering(m_id):
    




# # Ingest the list of match_ids
# for m_id in match_ids:
#     api_url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{m_id}/timeline?api_key={api_key}"
#     response = requests.get(api_url)  # Returns the response as a Python response object
#     if response.status_code == 200:
#         match_data[m_id] = response.json() 
#         print(f"API call success for match {m_id}")

#         all_events = []
#         frames = match_data[m_id]["info"]["frames"]
#         # Loop through frames and extract events
#         for frame in frames:
#             events = frame.get("events", [])
#             all_events.extend(events)

#         # Create a DataFrame from the extracted events
#         events_df = pd.DataFrame(all_events)
#         # filter rows
#         selected_rows = ['TURRET_PLATE_DESTROYED','ELITE_MONSTER_KILL','CHAMPION_SPECIAL_KILL','BUILDING_KILL','CHAMPION_KILL']
#         filt = events_df['type'].isin(selected_rows)
#         events_df = events_df[filt]
#         # filter columns
#         events_df['match_id'] = m_id
#         base_columns = ['match_id','timestamp','type','killerId']
#         events_df = events_df[base_columns]
#         # nest_columns = ['killType','killerTeamId','monsterType','laneType','teamId','monsterSubType','buildingType','towerType','bounty']
#         nest_columns = ~events_df.columns.isin(base_columns)
#         # create nested details
#         def create_details(row):
#             # row[nest_columns] selects only specfic columns from that each row
#             return row[nest_columns].dropna().to_dict()

#         events_df['details'] = events_df.apply(create_details, axis=1)
#         events_df = events_df[base_columns + ['details']]

#         csv_file_path = os.path.join(local_path, f"all_events.csv")
#         events_df.to_csv(csv_file_path, index=False)
#         print(f"Data saved to {csv_file_path}")

#     else:
#         error_code = response.status_code
#         print(f"Failed to fetch data for match {m_id} due to error {error_code}, {response.reason}")







##############################################################

# # Save all the collected data in one file as a dictionary
# file_path = os.path.join(local_path, 'all_match_data.json')
# with open(file_path, 'w') as file:
#     json.dump(match_data, file, indent=4)  # Save the dictionary to a single file

# print("All match data saved successfully!")

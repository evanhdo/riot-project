import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
api_key = os.getenv("RIOT_API_KEY")

match_ids = ['NA1_5157109235']

# Ingest the list of match_ids
for m_id in match_ids:
    api_url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{m_id}/timeline?api_key={api_key}"
    response = requests.get(api_url)  # Returns the response as a Python response object
    if response.status_code == 200:
        match_data = response.json() 
        print(f"API call success for match {m_id}")

        all_events = []
        frames = match_data["info"]["frames"]
        save_path = "/Users/evando/code/riot/test_data.json"
        with open(save_path, 'w') as file:
            json.dump(frames, file, indent=4)  # Use indent=4 for readability

    
    else: 
        print(response.status_code)
    #         events = frame.get("events", [])
    #         all_events.extend(events)

    #     # Create a DataFrame from the extracted events
    #     events_df = pd.DataFrame(all_events)
    #     # filter rows
    #     selected_rows = ['TURRET_PLATE_DESTROYED','ELITE_MONSTER_KILL','CHAMPION_SPECIAL_KILL','BUILDING_KILL','CHAMPION_KILL']
    #     filt = events_df['type'].isin(selected_rows)
    #     events_df = events_df[filt]
    #     # filter columns
    #     events_df['match_id'] = m_id
    #     base_columns = ['match_id','timestamp','type','killerId']
    #     events_df = events_df[base_columns]
    #     # nest_columns = ['killType','killerTeamId','monsterType','laneType','teamId','monsterSubType','buildingType','towerType','bounty']
    #     nest_columns = ~events_df.columns.isin(base_columns)
    #     # create nested details
    #     def create_details(row):
    #         # row[nest_columns] selects only specfic columns from that each row
    #         return row[nest_columns].dropna().to_dict()

    #     events_df['details'] = events_df.apply(create_details, axis=1)
    #     events_df = events_df[base_columns + ['details']]

    #     csv_file_path = os.path.join(local_path, f"all_events.csv")
    #     events_df.to_csv(csv_file_path, index=False)
    #     print(f"Data saved to {csv_file_path}")

    # else:
    #     error_code = response.status_code
    #     print(f"Failed to fetch data for match {m_id} due to error {error_code}, {response.reason}")


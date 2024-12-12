import pandas as pd
import requests, json, os, sys, random, time
from dotenv import load_dotenv, find_dotenv

#environment variables
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
api_key = os.getenv("RIOT_API_KEY")

# Set the local path for where you want to save your data
local_path = '/Users/evando/code/riot/match_data'
cleaned_data_path = '/Users/evando/code/riot/cleaned_data'

########################### ########################### ########################### ###########################
# Define requirements for the match to be added to the list of matches to be ingested
# condition the loop to run until the list hits 1000 matches
def match_filtering(df):
    # Check if required columns exist
    required_columns = {'monsterType', 'monsterSubType'}
    if not required_columns.issubset(df.columns):
        print("Required columns missing... moving onto next match")
        return False

    # Get teams that killed Baron Nashor
    baron_kills = df[df['monsterType'] == 'BARON_NASHOR']
    if not baron_kills.empty:
        baron_kills = baron_kills[['killerTeamId','monsterType','monsterSubType','timestamp']]
    else:
        print("no baron found... moving onto next match")
        return False

    # Get teams with at least 4 dragon kills (Dragon Soul)
    dragons = df[(df['monsterType'] == 'DRAGON') & (df['monsterSubType'] != 'ELDER_DRAGON')].copy()
    dragons['dragon_kill_count'] = dragons.groupby('killerTeamId').cumcount() + 1
    dragon_soul_kills = dragons[dragons['dragon_kill_count'] == 4]
    if not dragon_soul_kills.empty:
        dragon_soul_kills = dragon_soul_kills[['killerTeamId','monsterType','monsterSubType','timestamp']]
    else:
        print("no dragon soul found... moving onto next match")
        return False

    combined_df = pd.concat([baron_kills, dragon_soul_kills])
    combined_df = combined_df.sort_values(by='timestamp').reset_index(drop=True)

    # Determine if there was an instance where baron and dragon soul were taken within 75 seconds of each other by opposing teams
    # This will be considered a trade
    # Set the current row and previous row
    for i in range(1, len(combined_df)):
        current_row = combined_df.iloc[i]
        previous_row = combined_df.iloc[i - 1]
        
        # Check if the difference in timestamps is less than or equal to 75 seconds (75,000 milliseconds)
        if abs(current_row['timestamp'] - previous_row['timestamp']) <= 750000:
            # Check if opposing teams took the objectives
            if current_row['killerTeamId'] != previous_row['killerTeamId']:
                return True
        else:
            return False
########################### ########################### ########################### ###########################

# Start by retrieving match_list for one player
puuid = 'TgWQ6HYb9hq14-S8hWWq_PebGl-ERUOTMysqGzUBMETXYPIqDVgZi5AufN_K1kS-_98oAjc8ypKaLQ'
# Ingest the list of match_ids
i=1
m_id = None
retries = 0
final_df = pd.DataFrame()
while i <= 3:
    initial_call = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=20&api_key={api_key}"
    response = requests.get(initial_call)  # Returns the response as a Python response object

    if response.status_code == 429:  # Handle rate limiting
        print("Rate limit hit. Pausing for 150 seconds...")
        time.sleep(150)  # Wait for 2 minutes and 30 seconds
        continue
    elif response.status_code == 200:
        player_matches = response.json() 
        print(f"API call success for player {puuid}")
        m_id = random.choice(player_matches)
    
    api_url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{m_id}/timeline?api_key={api_key}"
    response = requests.get(api_url)  # Returns the response as a Python response object

    if response.status_code == 429:  # Handle rate limiting
        print("Rate limit hit. Pausing for 150 seconds...")
        time.sleep(150)  # Wait for 2 minutes and 30 seconds
        continue
    elif response.status_code == 200:
        match_data = response.json() 
        print(f"API call success for match {m_id}")
    else:
        retries += 1 
        error_code = response.status_code
        print(f"Failed to fetch data for match {m_id} due to error {error_code}, {response.reason}")
        if retries == 3:
            print("max retries has been reach... code will now exit")
            break
        continue

    all_events = []
    frames = match_data['info']['frames']
    # Loop through frames and extract events
    for frame in frames:
        events = frame.get("events", [])
        all_events.extend(events)
        # Create a DataFrame from the extracted events
        events_df = pd.DataFrame(all_events)

    # check if this match meets criteria, if so continue to filtering
    if match_filtering(events_df) == True:
        print("baron and dragon soul trade found")

        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 
        checkpoint1 = f"{cleaned_data_path}/cp1_df.csv"
        events_df.to_csv(checkpoint1, index = False)
        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 

        # filter rows
        selected_rows = ['TURRET_PLATE_DESTROYED','ELITE_MONSTER_KILL','CHAMPION_SPECIAL_KILL','BUILDING_KILL','CHAMPION_KILL']
        filt = events_df['type'].isin(selected_rows)
        events_df = events_df[filt]

        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 
        checkpoint2 = f"{cleaned_data_path}/cp2_df.csv"
        events_df.to_csv(checkpoint2, index = False)
        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 

        # filter columns
        events_df['match_id'] = m_id #first we create a column to capture match_id
        base_columns = ['match_id','timestamp','type','killerId'] # then we set our base columns within our table
        nest_columns = ~events_df.columns.isin(base_columns) # all of the other columns will be nested to save memory 
        # create nested details functions to drop all empty values and turn the rest into a dictionary which we can turn into a nested value
        def create_details(row):
            # row[nest_columns] selects all nested columns, then dropna() gets rid of any null values, and to_dict() converts them to a dictionary so we can nest them
            return row[nest_columns].dropna().to_dict()
        events_df['details'] = events_df.apply(create_details, axis=1) # apply the function we created to each row
        events_df = events_df[base_columns + ['details']] # then combine the base columns with all the column of nested values we just created

        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 
        checkpoint3 = f"{cleaned_data_path}/cp3_df.csv"
        events_df.to_csv(checkpoint3, index = False)
        ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT ############ CHECKPOINT 
        
        # append the valid match to the existing df
        final_df = pd.concat([final_df, events_df], ignore_index=True)
        print(f"{i} match worth of data has been appended")
        # retrieve the next puuid that we will randomly select a match from for processing
        puuid = random.choice(match_data['metadata']['participants'])
        i += 1
    else:
        print("match does not contain a baron/dragon trade... moving on to process the next match")
        participants_response = (match_data)['metadata']['participants']
        puuid = random.choice(participants_response)
        continue

##############################################################
final_df.to_csv(f"{cleaned_data_path}/test1.csv", index=False)
print("final df has been saved")

# # Save all the collected data in one file as a dictionary
# file_path = os.path.join(local_path, '1000_match_data.json')
# with open(file_path, 'w') as file:
#     json.dump(final_df, file, indent=4)  # Save the dictionary to a single file

# print("All match data saved successfully!")
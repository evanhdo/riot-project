# Extracting "events" data from the JSON
all_events = []
match_data = data["NA1_5147448576"]
frames = match_data["info"]["frames"]

# Loop through frames and extract events
for frame in frames:
    events = frame.get("events", [])
    all_events.extend(events)

# Create a DataFrame from the extracted events
events_df = pd.DataFrame(all_events)

# Display the DataFrame
print(events_df)
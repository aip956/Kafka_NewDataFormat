import requests
from urllib.parse import urlencode

# Base URL for the FastAPI App
base_url = "http://localhost:8000/send/"

def read_events_data(file_path):
    with open(file_path, mode='r') as file:
        lines = file.readlines()


    events = []
    for i in range(0, len(lines), 4):
        if i + 3 < len(lines):
            index = lines[i].strip()
            event_type = lines[i + 1].split('\t')[1].strip('"\n ')
            priority = lines[i + 2].split('\t')[1].strip('"\n ')
            description = lines[i + 3].split('\t')[1].strip('"\n ')
            events.append({
                'index': index,
                'event_type': event_type,
                'priority': priority,
                'description': description
            })
        else:
            print(f"Incomplete event datastarting at line {i}")
            # i += 5 # Move to next event block
    return events

events_data = read_events_data('events_data.txt')

for event in events_data:
    topic = event["event_type"]
    query_params = urlencode({"message": event["description"], "priority":event["priority"]})
    url = f"{base_url}{topic}?{query_params}"
    response = requests.post(url)

    if response.status_code == 200:
        print(f"Successfully sent event: {event['event_type']}")  
    else: 
        print(f"Failed to send event: {event['event_type']}, Status Code: {response.status_code}")  
    try: 
        print(response.json())
    except ValueError:
        print(f"Failed to send event: {event['event_type']}, Status Code: {response.status_code}")
    # print(response.json())
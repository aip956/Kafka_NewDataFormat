import requests
import time
import urllib.parse
from asyncio import AIOKafkaProducer
import json

# Base URL for the FastAPI App
base_url = "http://localhost:8000/send_event/"

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
    return events


def generate_event(event):
    query_params = urllib.parse.urlencode({
        "event_type": event["event_type"],
        "priority": event["priority"],
        "description": event["description"]
    })
    url = f"{base_url}?{query_params}"
    response = requests.post(url)

    if response.status_code == 200:
        print(f"Event {event['event_type']} sent successfully")
    else:
        print(f"Failed to send event {event['event_type']}. Status code: {response.status_code}")


def run_simulation(file_path):
    events = read_events_data(file_path)

    for event in events:
        generate_event(event)
        time.sleep(1) # Simulate time between events


if __name__=="__main__":
    run_simulation('events_data.txt')

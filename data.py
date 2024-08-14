import requests
from urllib.parse import urlencode
base_url = "http://localhost:8000/send/"


events = [
    {"event_type": "feeling_ill", "priority": "medium", "description": "Guest has stomach ache after eating 5 pieces of cake"},
    {"event_type": "person_fell", "priority": "high", "description": "Guest slipped on the dance floor"},
    {"event_type": "broken_glass", "priority": "low", "description": "Broken glass found near table 5"},
    {"event_type": "dirty_table", "priority": "low", "description": "From the broken glass"},
    {"event_type": "brawl", "priority": "low", "description": "It's a fight!"},
    {"event_type": "missing_rings", "priority": "low", "description": "Rings are just a symbol"},
    {"event_type": "missing_bride", "priority": "low", "description": "We kinda need her"},
    {"event_type": "missing_groom", "priority": "low", "description": "Hungover?"},
    {"event_type": "injured_kid", "priority": "low", "description": "Cut himself on the broken glass"},
    {"event_type": "not_on_list", "priority": "low", "description": "GET OUT"},
    {"event_type": "bad_food", "priority": "low", "description": "Musta been the shrimp"},
    {"event_type": "music_too_loud", "priority": "low", "description": "Turn that music down!"},
    {"event_type": "music_too_low", "priority": "low", "description": "Turn up!"},
]

for event in events:
    topic = event["event_type"]
    query_params = urlencode({"message": event["description"]})
    url = f"{base_url}{topic}?{query_params}"
    # payload = {"message": event["description"]}
    # message = {"message": event["description"]}
    response = requests.post(url)
    if response.status_code == 200:
        print(f"Successfully sent event: {event['event_type']}")  
    else: 
        print(f"Failed to send event: {event['event_type']}, Status Code: {response.status_code}")  
    try: 
        print(f"Failed to send event: {event['event_type']}, Status Code: {response.status_code}")
    except ValueError:
        print(f"Failed to send event: {event['event_type']}, Status Code: {response.status_code}")
    print(response.json())


# import asyncio
# import aiohttp

# # Define the list of events

# events = [
#     {"event_type": "feeling_ill", "priority": "medium", "description": "Guest has stomach ache after eating 5 pieces of cake"},
#     {"event_type": "person_fell", "priority": "high", "description": "Guest slipped on the dance floor"},
#     {"event_type": "broken_glass", "priority": "low", "description": "Broken glass found near table 5"},
#     {"event_type": "dirty_table", "priority": "low", "description": "From the broken glass"},
#     {"event_type": "brawl", "priority": "low", "description": "It's a fight!"},
#     {"event_type": "missing_rings", "priority": "low", "description": "Rings are just a symbol"},
#     {"event_type": "missing_bride", "priority": "low", "description": "We kinda need her"},
#     {"event_type": "missing_groom", "priority": "low", "description": "Hungover?"},
#     {"event_type": "injured_kid", "priority": "low", "description": "Cut himself on the broken glass"},
#     {"event_type": "not_on_list", "priority": "low", "description": "GET OUT"},
#     {"event_type": "bad_food", "priority": "low", "description": "Musta been the shrimp"},
#     {"event_type": "music_too_loud", "priority": "low", "description": "Turn that music down!"},
#     {"event_type": "music_too_low", "priority": "low", "description": "Turn up!"},
# ]


# async def send_event(event):
#     url = f"http://localhost:8000/send/{event['event_type']}"
#     payload = {"message": f"Priority: {event['priority']}, Description: {event['description']}"}
#     async with aiohttp.ClientSession() as session:
#         async with session.post(url, json=payload) as response:
#             if response.status == 200:
#                 print(f"Successfully sent event: {event['event_type']}")
#             else:
#                 print(f"Failed to send event: {event['event_type']}")

# async def main():
#     await asyncio.gather(*[send_event(event) for event in events])


# if __name__ == "__main__":
#     asyncio.run(main())
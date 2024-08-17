import asyncio
from aiokafka import AIOKafkaProducer
import json
import argparse

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "wedding_events"

async def produce_event(producer, event):
    event_data = json.dumps(event).encode("utf-8")
    await producer.send_and_wait(TOPIC, event_data)
    print(f"Produced event: {event['event_type']}")

async def run_simulation(file_path):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start()
    try:
        events = read_events_data(file_path)

        for event in events:
            await produce_event(producer, event)
            await asyncio.sleep(1) # simulate time between events
    finally:
        await producer.stop()

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

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Run Kafka Simulation with a data file")
    parser.add_argument('file_path', type=str, help="Path to the events data file")
    args = parser.parse_args()
    asyncio.run(run_simulation(args.file_path))
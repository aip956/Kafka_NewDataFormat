from fastapi import FastAPI, Response, BackgroundTasks, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors, AIOKafkaClient
import asyncio
import logging
import os
from datetime import datetime
# import random
import json
from pydantic import BaseModel, ValidationError
from typing import List



app = FastAPI()

# Initialize logger
# logger = logging.getLogger("uvicorn.error")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Kafka configuration
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")


stress_level = 0
events = []

# Define Event class
class Event(BaseModel):
    event_id: int
    event_type: str
    priority: str
    description: str
    timestamp: str

loop = asyncio.get_event_loop()
# producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

# Timeframes for priorities in secs
PRIORITY_TIMEFRAMES = {
    "High": 5,
    "Medium": 10,
    "Low": 15
}

# New topics for v2 of assignment
topics = ["accident", "bad_food", "brawl", "broken_glass", "broken_itens", "bride", "dirty_floor", "dirty_table", "feeling_ill", "groom", "injured_kid", "missing_bride", "missing_groom", "missing_rings", "music_too_loud", "music_too_low", "music", "not_on_list", "person_fell"]


SECURITY_TOPICS = ["brawl", "not_on_list", "accident", "person_fell", "injured_kid"]
CLEAN_UP_TOPICS = ["dirty_table", "broken_glass", "broken_itens", "dirty_floor"]
CATERING_TOPICS = ["bad_food", "music", "music_too_loud", "music_too_low", "feeling_ill"]
OFFICIANT_TOPICS = ["missing_rings", "missing_bride", "missing_groom", "bride", "groom"]
WAITERS_TOPICS = [ "broken_glass", "person_fell", "injured_kid", "feeling_ill", "broken_itens", "accident", "bad_food"]



    
@app.on_event("startup")
async def on_startup():
    global AIOKafkaProducer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    try:
        await producer.start() # Start the Kafka producer
        logger.info("Kafka producer started")
        asyncio.create_task(consume_events())
        logger.info("Kafka consumer task created")
    except errors.KafkaConnectionError as e:
        logger.error(f"Kafka connection error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await producer.stop() # Stop the Kafka producer
    logger.info("Kafka producer stopped")



# Define Worker class
class Worker:
    def __init__(self, team, routine):
        self.team = team 
        self.routine = routine
        self.status = "Idle"
        self.last_active_time = datetime.now()

    async def handle_event(self, event):
        self.status = "Working"
        # Simulate event handling time based on routine and priority
        await asyncio.sleep(3) # 3 secs to handle event
        self.status = "Idle"
        self.last_active_time = datetime.now()
        logger.info(f"Event {event.event_id} handled by {self.team}")

        # work_time = PRIORITY_TIMEFRAMES[event.priority]
        # start_time = datetime.now()
    async def simulate_routine(self):
        while True:
            if self.routine == "Standard":
                await asyncio.sleep(20)
                self.status = "Idle"
                await asyncio.sleep(5)
            elif self.routine == "Intermittent":
                await asyncio.sleep(5) # Simulate short working time
                self.status = "Idle"
                await asyncio.sleep(5) # Idle time
            elif self.routine == "Concentrated":
                await asyncio.sleep(60)
                self.status = "Idle"
                await asyncio.sleep(60)
            self.status = "Working"


# Define Team class
class Team:
    def __init__(self, name, routine):
        self.name = name
        self.routine = routine
        self.workers = [Worker(name, routine) for _ in range(5)] # Assuming 5 workers / team

    async def assign_event(self, event):
        for worker in self.workers:
            if worker.status == "Idle":
                await worker.handle_event(event)
                return True
        return False
    
# Initialize teams
teams = {
    "Security": Team("Security", "Standard"),
    "Clean_Up": Team("Clean_Up", "Intermittent"),
    "Catering": Team("Catering", "Concentrated"), 
    "Officiant": Team("Officiant", "Concentrated"),
    "Waiters": Team("Waiters", "Standard")
}

# Event type to team mapping
event_team_mapping = {
        "accident": ["Security", "Waiters"],
        "bad_food": ["Catering", "Waiters"], 
        "brawl": ["Secruity"],
        "broken_glass": ["Clean_Up", "Waiters"], 
        "broken_itens": ["Clean_Up", "Waiters"], 
        "bride": ["Officiant"], 
        "dirty_floor": ["Clean_Up"], 
        "dirty_table": ["Clean_Up"], 
        "feeling_ill": ["Catering"], 
        "groom": ["Officiant"], 
        "injured_kid": ["Security", "Waiters"], 
        "missing_bride": ["Officiant"], 
        "missing_groom": ["Officiant"], 
        "missing_rings": ["Officiant"], 
        "music_too_loud": ["Catering"], 
        "music_too_low": ["Catering"], 
        "music": ["Catering"], 
        "not_on_list": ["Security"], 
        "person_fell": ["Security", "Waiters"]
    }

# For getting events by team
# consumed_messages = []
security_messages = []
clean_up_messages = []
catering_messages = []
officiant_messages = []
waiters_messages = []


@app.post("/events")
async def receive_event(event: Event):
    valid_event_types = event_team_mapping.keys()
    if event.event_type not in valid_event_types:
        raise HTTPException(status_code=400, detail="Invalid event type")
    # events.append(event)
    logger.info(f"Received event: {event.event_id}")
    # await dispatch_event(event)
    await produce_event_to_kafka(event)
    return{"status": "Event received"}

async def produce_event_to_kafka(event):
    try:
        await producer.send_and_wait("events_topic", json.dumps(event.dict()).encode('utf-8'))
        logger.info(f"Produced event: {event.event_id} to Kafka")
    except errors.KafkaConnectionError as e:
        logger.error(f"Kafka connection error: {e}")

# Marry Me Organizer to dispatch events
async def dispatch_event(event):
    global stress_level
    try:
        event_type = event.event_type
        event_id = event.event_id
        teams = event_team_mapping.get(event_type)
        if teams:
            event_handled = False
            start_time = datetime.now()
            for team_name in teams:
                team = await get_team_for_event(team_name)
                logger.info(f"Dispatched {event_type} event {event_id} with team {team_name}")
                if team:
                    logger.info(f"Dispatched {event_type} event {event_id} by team {team_name}.")
                    # Store event in event type list
                    if team_name == "Security":
                        security_messages.append(event)
                    elif team_name == "Clean_Up":
                        clean_up_messages.append(event)
                    elif team_name == "Catering":
                        catering_messages.append(event)
                    elif team_name == "Officiant":
                        officiant_messages.append(event)
                    elif team_name == "Waiters":
                        waiters_messages.append(event)

                    if await team.assign_event(event):
                        event_handled = True
                        break
                    else:
                        logger.warning(f"Team {team_name} unable to handle event type {event_type} eventID {event_id}")
            if not event_handled:
                elapsed_time = (datetime.now() - start_time).seconds
                if elapsed_time > PRIORITY_TIMEFRAMES[event.priority]:
                    stress_level += 1
                    logger.warning(f"Dispatched {event_type} event {event_id} not handled by team {team_name}. Stress level increased to {stress_level}")
        else:
            stress_level += 1
            logger.warning(f"200Unknown team or event type: {event_type}. Stress level increased to {stress_level}")
    except Exception as e:
        stress_level += 1
        logger.warning (f"Event {event_id} could not be handled in time. Stress level increased to {stress_level}")
        logger.error(f"Error handling event {event_id}: {e}")
        
async def get_team_for_event(event_type):
    return event_team_mapping.get(event_type, [None])[0]

# Kafka consumer function
async def consume_events():
    consumer = AIOKafkaConsumer(
        'wedding_events',
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="event_group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event_data = json.loads(msg.value.decode("utf-8"))
            if 'event_id' not in event_data or 'timestamp' not in event_data:
                logger.error(f"229Missing field: {event_data}")
                continue
            try:
                event = Event(**event_data)
                events.append(event)
                await dispatch_event(event)
                logger.info(f"230Consumed and processed event: {event.event_type}")
            except ValidationError as e:
                logger.error(f"Validation error for event data: {event_data} - {e.errors()}")
            except Exception as e:
                logger.error(f"Validation error for event data: {event_data} - {e}")
    except errors.KafkaError as e:
        logger.error(f"Kafka error: {e}")
    finally:
        await consumer.stop()

async def handle_event(event):
    global stress_level
    try:
        event_type = event.get("event_type")
        event_id = event.get("event_id")
        if event_type == "wedding":
            team = await get_team_for_event(event_type)
            # Perform actions with the team
            logger.info(f"Handled wedding event {event_id} with team {team}")
        else:
            logger.warning(f"210 type: {event_type}")
    except Exception as e:
        stress_level += 1
        logger.warning(f"Event {event_id} could not be handled in tiem. Stress level increased to {stress_level}")
        logger.error(f"Error handling event {event_id}: {e}")

@app.get("/")
async def read_root():
    return {"Hello": "world"}

@app.get("/stress_level")
def get_stress_level():
    logger.info(f"Returning stress level: {stress_level}")
    return {"stress_level": stress_level}

@app.get("/events", response_model=List[Event])
async def get_events():
    logger.info(f"Returning events: {events}")
    # return {"events": events}
    return events

# For the teams' messages
@app.get("/security_messages")
def get_security_messages():
    logger.info(f"Returning security messages: {security_messages}")
    return {"security messages": security_messages}

@app.get("/clean_up_messages")
def get_clean_up_messages():
    logger.info(f"Returning clean_up messages: {clean_up_messages}")
    return {"clean_up messages": clean_up_messages}

@app.get("/catering_messages")
def get_catering_messages():
    logger.info(f"Returning catering messages: {catering_messages}")
    return {"catering messages": catering_messages}

@app.get("/officiant_messages")
def get_officiant_messages():
    logger.info(f"Returning officiant messages: {officiant_messages}")
    return {"officiant messages": officiant_messages}

@app.get("/waiters_messages")
def get_waiters_messages():
    logger.info(f"Returning waiter messages: {waiters_messages}")
    return {"waiters messages": waiters_messages}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)






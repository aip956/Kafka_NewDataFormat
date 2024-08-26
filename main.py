from fastapi import FastAPI, Response, BackgroundTasks, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors, AIOKafkaClient
import asyncio
import logging
import os
from datetime import datetime, timedelta
# import random
import json
from pydantic import BaseModel, ValidationError



app = FastAPI()

# Initialize logger
# logger = logging.getLogger("uvicorn.error")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Global vars
events = []
stress_level = 0
producer = None
consumer = None

# Kafka configuration
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = "wedding_events"

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




# Initialize global vars for consuper and producer


# Timeframes for worker routines in secs
# ROUTINE_TIMEFRAMES = {
#     "Standard": (20, 5),
#     "Intermittent": (5, 5),
#     "Concentrated": (60, 60)
# }

# Define Event class
class Event(BaseModel):
    event_id: int
    event_type: str
    priority: str
    description: str
    timestamp: datetime

# Define Worker class
class Worker:
    def __init__(self, team, routine):
        self.team = team 
        self.routine = routine
        self.status = "Idle"

    async def handle_event(self, event):
        self.status = "Working"
        # Simulate event handling time based on routine and priority
        work_time = PRIORITY_TIMEFRAMES[event.priority]
        if self.routine == "Standard":
            await asyncio.sleep(work_time)
        elif self.routine == "Intermittent":
            await asyncio.sleep(work_time * 1.5)
        elif self.routine == "Concentrated":
            await asyncio.sleep(work_time * 0.5)
        self.status = "Idle"
        logger.info(f"Event {event.event_id} handled by {self.team}")

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

async def get_team_for_event(event_type):
    return event_team_mapping.get(event_type, [None])[0]


@app.post("/events")
async def receive_event(event: Event):
    valid_event_types = event_team_mapping.keys()
    if event.event_type not in valid_event_types:
        raise HTTPException(status_code=400, detail="Invalid event type")
    events.append(event)
    logger.info(f"Received event: {event.event_id}")
    await dispatch_event(event)
    return{"status": "Event received"}

# Marry Me Organizer to dispatch events
async def dispatch_event(event):
    global stress_level
    try:
        event_type = event.event_type
        event_id = event.event_id
        teams = event_team_mapping.get(event_type)
        if teams:
            for team_name in teams:
                team = await get_team_for_event(team_name)
                logger.info(f"Handled {event_type} event {event_id} with team {team_name}")
        else:
            logger.warning(f"161Unknown event type: {event_type}")
    except Exception as e:
        stress_level += 1
        logger.warning (f"Event {event_id} could not be handled in time. Stress level increased to {stress_level}")
        logger.error(f"Error handling event {event_id}: {e}")
        

    
@app.on_event("startup")
async def on_startup():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start() # Start the Kafka producer
    asyncio.create_task(consume_events()) #Start consuming Kafka events

@app.on_event("shutdown")
async def on_shutdown():
    if producer:
        await producer.stop() # Stop the Kafka producer
    if consumer:
        await consumer.stop() # Stop the Kafka consumer
    logger.info("Kafka producer stopped")



@app.get("/")
async def read_root():
    return {"Hello": "world"}

@app.get("/stress_level")
def get_stress_level():
    logger.info(f"Returning stress level: {stress_level}")
    return {"stress_level": stress_level}

@app.get("/events")
def get_events():
    logger.info(f"Returning events: {events}")
    return {"events": events}

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

# Kafka consumer function
async def consume_events():
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="wedding_group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event_data = json.loads(msg.value.decode("utf-8"))
            if 'event_id' not in event_data or 'timestamp' not in event_data:
                logger.error(f"Missing field: {event_data}")
                continue
            try:
                event = Event(**event_data)
                await dispatch_event(event)
                logger.info(f"230Consumed and processed event: {event.event_type}")
            except ValidationError as e:
                logger.error(f"Validation error for event data: {event_data} - {e.errors()}")
            except Exception as e:
                logger.error(f"Validation error for event data: {event_data} - {e}")

    finally:
        await consumer.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)




# loop = asyncio.get_event_loop()
# producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

# consumed_messages = []
# security_messages = []
# clean_up_messages = []
# catering_messages = []
# officiant_messages = []
# waiters_messages = []

# @app.on_event("startup")
# async def on_startup():
#     try:
#         await producer.start()
#         logger.info("Kafka producer started")
#         asyncio.create_task(consume_all())
#         asyncio.create_task(consume_security())
#         asyncio.create_task(consume_clean_up())
#         asyncio.create_task(consume_catering())
#         asyncio.create_task(consume_officiant())
#         asyncio.create_task(consume_waiters())
#         logger.info("Kafka consumer tasks created")
#     except errors.KafkaConnectionError as e:
#         logger.error(f"Kafka connection error: {e}")



# @app.post("/send/{topic}")
# async def produce(topic: str, message: str):
#     try:
#         await producer.send_and_wait(topic, message.encode('utf-8'))
#         logger.info(f"Produced message: {message} to topic: {topic}")
#         return {"message": "Message sent successfully"}
#     except errors.KafkaConnectionError as e:
#         logger.error(f"Kafka connection error: {e}")
#         return {"message": "Failed to send message"}

# async def consume_all():
#     consumer = AIOKafkaConsumer(
#         *topics, # Unpack all topic names
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id= "AllConsumers",
#         session_timeout_ms = 10000, # 6 seconds
#         heartbeat_interval_ms = 3000, # 2 seconds
#     )
    # await consumer.start()
    # try:
    #     async for msg in consumer:
    #         message = msg.value.decode('utf-8')
    #         topic = msg.topic
    #         consumed_messages.append({"topic": topic, "message": message})
    #         logger.info(f"Consumed_all message: {message} from topic: {topic}")
    # except Exception as e:
    #     logger.error(f"Error occurred: {e}")
    # finally:
    #     await consumer.stop()

# async def consume_security():
#     consumer = AIOKafkaConsumer(
#         *SECURITY_TOPICS,
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id="Security")
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             message = msg.value.decode('utf-8')
#             topic = msg.topic
#             security_messages.append({"topic": topic, "message": message})
#             logger.info(f"Security message: {message} from topic: {topic}")
#     except Exception as e:
#         logger.error(f"Error occurred: {e}")
#     finally:
#         await consumer.stop()

# async def consume_clean_up():
#     consumer = AIOKafkaConsumer(
#         *CLEAN_UP_TOPICS,
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id="Clean_Up")
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             message = msg.value.decode('utf-8')
#             topic = msg.topic
#             clean_up_messages.append({"topic": topic, "message": message})
#             logger.info(f"Clean_Up message: {message} from topic: {topic}")
#     except Exception as e:
#         logger.error(f"Error occurred: {e}")
#     finally:
#         await consumer.stop()


# async def consume_catering():
#     consumer = AIOKafkaConsumer(
#         *CATERING_TOPICS, #Sub in KAFA_TOPIC later
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id="Catering")
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             message = msg.value.decode('utf-8')
#             topic = msg.topic
#             catering_messages.append({"topic": topic, "message": message})
#             logger.info(f"Catering message: {message} from topic: {topic}")
#     except Exception as e:
#         logger.error(f"Error occurred: {e}")
#     finally:
#         await consumer.stop()

# async def consume_officiant():
#     consumer = AIOKafkaConsumer(
#         *OFFICIANT_TOPICS,
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id="Officiant")
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             message = msg.value.decode('utf-8')
#             topic = msg.topic
#             officiant_messages.append({"topic": topic, "message": message})
#             logger.info(f"Officiant message: {message} from topic: {topic}")
#     except Exception as e:
#         logger.error(f"Error occurred: {e}")
#     finally:
#         await consumer.stop()

# async def consume_waiters():
#     consumer = AIOKafkaConsumer(
#         *WAITERS_TOPICS,
#         loop=loop,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id="Waiters")
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             message = msg.value.decode('utf-8')
#             topic = msg.topic
#             waiters_messages.append({"topic": topic, "message": message})
#             logger.info(f"Waiting message: {message} from topic: {topic}")
#     except Exception as e:
#         logger.error(f"Error occurred: {e}")
#     finally:
#         await consumer.stop()


# @app.get("/messages")
# def get_messages():
#     logger.info(f"Returning consumed messages: {consumed_messages}")
#     return {"All messages": consumed_messages}

# @app.get("/security_messages")
# def get_security_messages():
#     logger.info(f"Returning security messages: {security_messages}")
#     return {"security messages": security_messages}

# @app.get("/clean_up_messages")
# def get_clean_up_messages():
#     logger.info(f"Returning clean_up messages: {clean_up_messages}")
#     return {"clean_up messages": clean_up_messages}

# @app.get("/catering_messages")
# def get_catering_messages():
#     logger.info(f"Returning catering messages: {catering_messages}")
#     return {"catering messages": catering_messages}

# @app.get("/officiant_messages")
# def get_officiant_messages():
#     logger.info(f"Returning officiant messages: {officiant_messages}")
#     return {"officiant messages": officiant_messages}

# @app.get("/waiters_messages")
# def get_waiters_messages():
#     logger.info(f"Returning waiter messages: {waiters_messages}")
#     return {"waiters messages": waiters_messages}




# Kafka producer and consumer setup 



    # Event handler function
# async def handle_event(event):
#     global stress_level
#     start_time = datetime.now()
#     timeout = PRIORITY_TIMEFRAMES[event['priority']]

#     # Simulate processing
#     processing_time = random.randint(1, 20)
#     await asyncio.sleep(processing_time)

#     elapsed_time = (datetime.now() - start_time).seconds
#     if elapsed_time > timeout:
#         stress_level += 1
#         logger.info(f"Event {event['event_type']} not handled in time. Stress level increased")
#     else:
#         logger.info(f"Event {event['event_type']} handled in time")

#     # Log event
#     events.append({"event": event, "handled": elapsed_time <= timeout})


# # Kafka consumer function
# async def consume_events():
#     consumer = AIOKafkaConsumer(
#         TOPIC,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         group_id = "wedding_group"
#     )
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             event = json.loads(msg.value.decode("utf-8"))
#             await handle_event(event)
#             logger.info(f"Consumed and processed event: {event['event_type']}")
#     finally:
#         await consumer.stop()
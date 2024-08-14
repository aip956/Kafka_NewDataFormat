from fastapi import FastAPI, Response, BackgroundTasks, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors, AIOKafkaClient
import asyncio
import logging
import os


# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

# All the data topics
topics = ["person_fell", "broken_glass", "dirty_table", "brawl", "missing_rings", "missing_bride", "missing_groom", "feeling_ill", "injured_kid", "not_on_list", "bad_food", "music_too_loud", "music_too_low"]
SECURITY_TOPICS = ["brawl", "not_on_list", "person_fell", "injured_kid"]
CLEAN_UP_TOPICS = ["dirty_table", "broken_glass"]
CATERING_TOPICS = ["bad_food", "music_too_loud", "music_too_low", "feeling_ill"]
OFFICIANT_TOPICS = ["missing_rings", "missing_bride", "missing_groom"]
WAITERS_TOPICS = [ "broken_glass", "person_fell", "injured_kid", "feeling_ill"]


loop = asyncio.get_event_loop()
producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

consumed_messages = []
security_messages = []
clean_up_messages = []
catering_messages = []
officiant_messages = []
waiters_messages = []

@app.on_event("startup")
async def on_startup():
    try:
        await producer.start()
        logger.info("Kafka producer started")
        asyncio.create_task(consume_all())
        asyncio.create_task(consume_security())
        asyncio.create_task(consume_clean_up())
        asyncio.create_task(consume_catering())
        asyncio.create_task(consume_officiant())
        asyncio.create_task(consume_waiters())
        logger.info("Kafka consumer tasks created")
    except errors.KafkaConnectionError as e:
        logger.error(f"Kafka connection error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await producer.stop()
    logger.info("Kafka producer stopped")

@app.post("/send/{topic}")
async def produce(topic: str, message: str):
    try:
        await producer.send_and_wait(topic, message.encode('utf-8'))
        logger.info(f"Produced message: {message} to topic: {topic}")
        return {"message": "Message sent successfully"}
    except errors.KafkaConnectionError as e:
        logger.error(f"Kafka connection error: {e}")
        return {"message": "Failed to send message"}

async def consume_all():
    consumer = AIOKafkaConsumer(
        *topics, # Unpack all topic names
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id= "AllConsumers",
        session_timeout_ms = 10000, # 6 seconds
        heartbeat_interval_ms = 3000, # 2 seconds
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            consumed_messages.append({"topic": topic, "message": message})
            logger.info(f"Consumed_all message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()

async def consume_security():
    consumer = AIOKafkaConsumer(
        *SECURITY_TOPICS,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="Security")
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            security_messages.append({"topic": topic, "message": message})
            logger.info(f"Security message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()

async def consume_clean_up():
    consumer = AIOKafkaConsumer(
        *CLEAN_UP_TOPICS,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="Clean_Up")
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            clean_up_messages.append({"topic": topic, "message": message})
            logger.info(f"Clean_Up message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()


async def consume_catering():
    consumer = AIOKafkaConsumer(
        *CATERING_TOPICS, #Sub in KAFA_TOPIC later
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="Catering")
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            catering_messages.append({"topic": topic, "message": message})
            logger.info(f"Catering message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()

async def consume_officiant():
    consumer = AIOKafkaConsumer(
        *OFFICIANT_TOPICS,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="Officiant")
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            officiant_messages.append({"topic": topic, "message": message})
            logger.info(f"Officiant message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()

async def consume_waiters():
    consumer = AIOKafkaConsumer(
        *WAITERS_TOPICS,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="Waiters")
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value.decode('utf-8')
            topic = msg.topic
            waiters_messages.append({"topic": topic, "message": message})
            logger.info(f"Waiting message: {message} from topic: {topic}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await consumer.stop()


@app.get("/messages")
def get_messages():
    logger.info(f"Returning consumed messages: {consumed_messages}")
    return {"All messages": consumed_messages}

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
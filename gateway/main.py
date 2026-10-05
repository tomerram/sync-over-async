import os
import socket
import asyncio
import logging
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, KafkaConnectionError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [GATEWAY] %(message)s")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
NETWORK_ID = os.getenv("NETWORK_ID", "network-a")
CLIENT_URL = os.getenv("CLIENT_URL", "http://client-a:8000")

producer: AIOKafkaProducer = None
http_client: httpx.AsyncClient = None


async def wait_for_kafka_dns(host="kafka", port=9092, retries=15, delay=2):
    """Waits for Docker internal DNS to resolve the Kafka hostname."""
    for attempt in range(1, retries + 1):
        try:
            ip = socket.gethostbyname(host)
            logging.info(f"Resolved '{host}' to IP: {ip}")
            return
        except socket.gaierror as e:
            logging.warning(f"DNS lookup for '{host}' failed ({attempt}/{retries}): {e}")
            await asyncio.sleep(delay)
    raise RuntimeError(f"Could not resolve hostname '{host}' after {retries} attempts.")


async def setup_own_network_topic():
    """Idempotently provisions the network topic on boot."""
    await wait_for_kafka_dns()
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    while True:
        try:
            await admin.start()
            topic = NewTopic(name=NETWORK_ID, num_partitions=1, replication_factor=1)
            await admin.create_topics([topic])
            logging.info(f"Created topic '{NETWORK_ID}' for network.")
            await admin.close()
            break
        except TopicAlreadyExistsError:
            logging.info(f"Topic '{NETWORK_ID}' already exists.")
            await admin.close()
            break
        except (KafkaConnectionError, Exception) as e:
            logging.warning(f"Waiting for Kafka readiness: {e}")
            await asyncio.sleep(3)


async def consume_incoming_topic():
    """Consumes messages from NETWORK_ID topic and posts them to local Client."""
    consumer = AIOKafkaConsumer(
        NETWORK_ID,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{NETWORK_ID}-gateway-group",
        auto_offset_reset="earliest"
    )

    while True:
        try:
            await consumer.start()
            logging.info(f"Started consuming Kafka topic '{NETWORK_ID}'")
            break
        except Exception as e:
            logging.warning(f"Consumer startup waiting for Kafka: {e}")
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            payload = msg.value.decode("utf-8")
            logging.info(f"Received Kafka message on '{NETWORK_ID}': {payload}")

            try:
                res = await http_client.post(
                    f"{CLIENT_URL}/incoming",
                    content=payload,
                    headers={"Content-Type": "application/json"}
                )
                logging.info(f"Forwarded payload to Client at '{CLIENT_URL}/incoming' (Status: {res.status_code})")
            except Exception as e:
                logging.error(f"Failed to forward message to local Client: {e}")
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, http_client

    await setup_own_network_topic()

    http_client = httpx.AsyncClient()
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    consumer_task = asyncio.create_task(consume_incoming_topic())

    yield

    consumer_task.cancel()
    await producer.stop()
    await http_client.aclose()


app = FastAPI(title=f"Gateway Service ({NETWORK_ID})", lifespan=lifespan)


class OutboundPayload(BaseModel):
    target_network: str
    payload: str


@app.post("/outbound")
async def handle_outbound(data: OutboundPayload):
    """Produces message to target_network topic. Returns 400 if target topic does not exist."""
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await admin.start()

    try:
        existing_topics = await admin.list_topics()
    finally:
        await admin.close()

    if data.target_network not in existing_topics:
        logging.warning(f"Outbound rejected: Target topic '{data.target_network}' does not exist.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Target network topic '{data.target_network}' does not exist."
        )

    try:
        await producer.send_and_wait(data.target_network, data.payload.encode("utf-8"))
        logging.info(f"Successfully published message to topic '{data.target_network}'")
        return {
            "status": "success",
            "from_network": NETWORK_ID,
            "target_network": data.target_network
        }
    except Exception as e:
        logging.error(f"Failed to publish to Kafka: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish message: {str(e)}"
        )
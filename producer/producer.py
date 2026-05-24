import json
import time
import uuid
import random
import logging
from datetime import datetime

from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ==============================
# INITIALIZE COMPONENTS
# ==============================

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


EVENT_TYPES = [
    "page_view",
    "search",
    "add_to_cart",
    "purchase",
    "remove_from_cart",
    "login",
    "checkout"
]

DEVICES = [
    "mobile",
    "desktop",
    "tablet"
]

def generate_event():

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random.randint(100, 500),
        "category": random.choice([
            "electronics",
            "fashion",
            "books",
            "home",
            "sports"
        ]),
        "price": round(random.uniform(10, 5000), 2),
        "device": random.choice(DEVICES),
        "country": fake.country(),
        "city": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

    return event


def stream_events():

    logger.info("Starting Kafka producer stream...")

    while True:

        event = generate_event()

        producer.send(
            KAFKA_TOPIC,
            value=event
        )

        logger.info(f"Produced Event: {event}")

        time.sleep(2)


if __name__ == "__main__":

    try:
        stream_events()

    except KeyboardInterrupt:
        logger.info("Producer stopped manually.")

    finally:
        producer.close()
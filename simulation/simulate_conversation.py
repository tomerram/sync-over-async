import asyncio
import logging
import os
import sys
import httpx

# Configure logging to show clear step-by-step progress
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

CLIENT_A_URL = os.getenv("CLIENT_A_URL", "http://localhost:8001")
CLIENT_B_URL = os.getenv("CLIENT_B_URL", "http://localhost:8002")


async def check_services_ready(client: httpx.AsyncClient) -> bool:
    """Verifies that both Client services are up and reachable before starting."""
    for name, url in [("Client A", CLIENT_A_URL), ("Client B", CLIENT_B_URL)]:
        try:
            res = await client.get(f"{url}/docs")
            if res.status_code != 200:
                logging.error(f"{name} returned status code {res.status_code}")
                return False
        except Exception as e:
            logging.error(f"Cannot reach {name} at {url}: {e}")
            return False
    return True


async def run_simulation():
    async with httpx.AsyncClient(timeout=15.0) as http_client:
        logging.info("Checking service readiness...")
        if not await check_services_ready(http_client):
            logging.error("One or both clients are unreachable. Make sure 'docker compose up' is running.")
            return

        logging.info("Services are ready. Starting conversation simulation...\n")

        # ----------------------------------------------------------------------
        # TURN 1: Network A sends "hello" to Network B
        # ----------------------------------------------------------------------
        logging.info(">>> TURN 1: Network A -> Network B: 'hello'")
        payload_1 = {
            "target_network": "network-b",
            "payload": "hello"
        }

        response_1 = await http_client.post(f"{CLIENT_A_URL}/send", json=payload_1)
        if response_1.status_code == 200:
            logging.info(f"Client A dispatched message successfully. Response: {response_1.json()}")
        else:
            logging.error(f"Turn 1 failed (Status {response_1.status_code}): {response_1.text}")
            return

        # Give Kafka and Gateway B time to consume and deliver to Client B
        await asyncio.sleep(2)

        # ----------------------------------------------------------------------
        # TURN 2: Network B replies "hello your self, how are you" to Network A
        # ----------------------------------------------------------------------
        logging.info("\n>>> TURN 2: Network B -> Network A: 'hello your self, how are you'")
        payload_2 = {
            "target_network": "network-a",
            "payload": "hello your self, how are you"
        }

        response_2 = await http_client.post(f"{CLIENT_B_URL}/send", json=payload_2)
        if response_2.status_code == 200:
            logging.info(f"Client B dispatched reply successfully. Response: {response_2.json()}")
        else:
            logging.error(f"Turn 2 failed (Status {response_2.status_code}): {response_2.text}")
            return

        # Give Kafka and Gateway A time to consume and deliver to Client A
        await asyncio.sleep(2)

        # ----------------------------------------------------------------------
        # TURN 3: Network A answers "i am good" to Network B
        # ----------------------------------------------------------------------
        logging.info("\n>>> TURN 3: Network A -> Network B: 'i am good'")
        payload_3 = {
            "target_network": "network-b",
            "payload": "i am good"
        }

        response_3 = await http_client.post(f"{CLIENT_A_URL}/send", json=payload_3)
        if response_3.status_code == 200:
            logging.info(f"Client A dispatched answer successfully. Response: {response_3.json()}")
        else:
            logging.error(f"Turn 3 failed (Status {response_3.status_code}): {response_3.text}")
            return

        # Wait for final message delivery
        await asyncio.sleep(2)
        logging.info("\nConversation simulation completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_simulation())
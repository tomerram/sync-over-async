import os
import logging
import httpx
from fastapi import FastAPI, Request, HTTPException, Response, status
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [CLIENT] %(message)s")

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway-a:8000")
NETWORK_ID = os.getenv("NETWORK_ID", "network-a")

app = FastAPI(title=f"Client Service ({NETWORK_ID})")


class OutboundMessage(BaseModel):
    target_network: str
    payload: str


@app.post("/incoming")
async def receive_incoming(request: Request):
    """Endpoint called by the local Gateway to deliver Kafka messages."""
    body = await request.body()
    decoded_message = body.decode("utf-8")

    # Print to stdout and log
    print(
        f"\n==================== INCOMING MESSAGE ====================\n{decoded_message}\n==========================================================")
    logging.info(f"Successfully processed incoming message from Gateway: {decoded_message}")

    return {"status": "received", "network_id": NETWORK_ID}


@app.post("/send")
async def send_message(msg: OutboundMessage):
    """Endpoint called by external clients to dispatch a message to another network."""
    logging.info(f"Initiating message transfer to target network '{msg.target_network}'")

    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(
                f"{GATEWAY_URL}/outbound",
                json={"target_network": msg.target_network, "payload": msg.payload},
                timeout=10.0
            )

            if res.status_code == 400:
                logging.warning(f"Failed to send: Target network topic '{msg.target_network}' does not exist.")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=res.json().get("detail"))

            res.raise_for_status()
            logging.info(f"Message successfully routed via local Gateway to '{msg.target_network}'")
            return res.json()

        except httpx.RequestError as exc:
            logging.error(f"HTTP request to local Gateway failed: {exc}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Gateway unreachable")
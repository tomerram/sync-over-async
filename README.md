# Project Name: sync over async poc

**sync over async poc** is a network-isolated, asynchronous message-bridge proof-of-concept using **FastAPI** and **Apache Kafka (KRaft mode)**. 

Services reside within isolated Docker networks (`net-a` and `net-b`) and are completely prevented from communicating directly with each other over standard HTTP. All inter-network communication is routed through dedicated **Gateway** services acting as a control plane over a central Kafka bus (`kafka-bus`).

---

## Architecture Review

Two Python services that communicate through http. 
Client - General purpose service 
Gateway - Service for communication with external networks via Kafka
Each Network has its own topic, and external communication is made through messages on the write topic
Gateway listen to the network topic and if message arrives it passes it to the client via http
Gateway is crating the network topic upon starting if topic does not exists
Kafka serves as a message bus for communications between networks. each network has its own topic


## 🏗 System Architecture

```text
+-----------------------------------------------------------------------------------+
|                                HOST / EXTERNAL                                    |
|                                                                                   |
|                   +---------------------------------------+                       |
|                   |  simulate_conversation.py / curl API   |                       |
|                   +-------------------+-------------------+                       |
+---------------------------------------|-------------------------------------------+
                                        |
                   +--------------------+--------------------+
                   |                                         |
                   | HTTP POST :8001                         | HTTP POST :8002
                   v                                         v
+------------------------------------+     +------------------------------------+
| ISOLATED NETWORK A (net-a)         |     | ISOLATED NETWORK B (net-b)         |
|                                    |     |                                    |
|  +------------------------------+  |     |  +------------------------------+  |
|  |       Client A (FastAPI)     |  |     |  |       Client B (FastAPI)     |  |
|  |          Port: 8000          |  |     |  |          Port: 8000          |  |
|  +--------------+---------------+  |     |  +--------------+---------------+  |
|                 |                  |     |                 |                  |
|                 | Internal HTTP    |     |                 | Internal HTTP    |
|                 v                  |     |                 v                  |
|  +--------------+---------------+  |     |  +--------------+---------------+  |
|  |      Gateway A (FastAPI)     |  |     |  |      Gateway B (FastAPI)     |  |
|  |          Port: 8000          |  |     |  |          Port: 8000          |  |
|  +--------------+---------------+  |     |  +--------------+---------------+  |
+-----------------|------------------+     +-----------------|------------------+
                  |                                          |
                  | Produce / Consume                        | Produce / Consume
                  v                                          v
+-----------------------------------------------------------------------------------+
| CENTRAL TRANSPORT BUS (kafka-bus)                                                 |
|                                                                                   |
|                    +------------------------------------------+                   |
|                    |        Apache Kafka Broker (KRaft)       |                   |
|                    +--------------------+---------------------+                   |
|                                         |                                         |
|                     +-------------------+-------------------+                     |
|                     |                                       |                     |
|                     v                                       v                     |
|         +-----------------------+               +-----------------------+         |
|         |   Topic: network-a    |               |   Topic: network-b    |         |
|         +-----------------------+               +-----------------------+         |
+-----------------------------------------------------------------------------------+
```
### Project structure:
```text
.
├── .env                     # System-wide environment variables & ports
├── docker-compose.yml        # Multi-network infrastructure definition
├── simulate_conversation.py  # Multi-turn interaction test script
├── client/
│   ├── Dockerfile
│   ├── main.py              # Client application (FastAPI)
│   └── requirements.txt     # Client Python dependencies
└── gateway/
    ├── Dockerfile
    ├── main.py              # Gateway service (FastAPI + AIOKafka)
    └── requirements.txt     # Gateway Python dependencies
```

## API Endpoint Documentation

### Client
- Listen to POST requests on:
  - ```/incoming ``` - Prints the message always returns:
    - ``{"status": "received", "network_id": NETWORK_ID}`` 
  - ```` /send ````- Pass the message to the gateway service. return the status code from the gateway  
### Gateway
- Listen to POST requests on:
    - ```/outbound``` message structure:
    ``
    {target_network: str, payload: str}
  ``
      - Returns: 
        - If succuss:
          - ``{
                  "status": "success",
                  "from_network": NETWORK_ID,
                  "target_network": data.target_network
              }``
        - If kafka topic not exists:
                ``{
                        "status_code": 400,
                        detail=f"Target network topic '{data.target_network}' does not exist."
                    }``
        - If error from kafka:
                ``{
                        status_code=500,
                        detail=f"Failed to publish message: {str(e)}"
                }``


## 🚀 Getting Started & Lifecycle Management

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v20.10+ with Docker Compose v2+)
* Python 3.10+ (optional, required only for running `simulate_conversation.py` locally)

---

### 1. How to Start the System

#### Standard Startup (Foreground Mode)
Builds images (if needed), creates networks, and streams container logs directly to your terminal:

```bash
docker compose up --build

```
### 1. How to Stop the System
If running in foreground mode, press Ctrl + C 

Or execute from another terminal window:

```bash
docker compose stop
```

### Run an example of communication between 2 networks 

```bash
docker build -t conversation-simulator ./simulation
docker run --rm \                                   
  -e CLIENT_A_URL="http://host.docker.internal:8001" \
  -e CLIENT_B_URL="http://host.docker.internal:8002" \
  --add-host=host.docker.internal:host-gateway \
  conversation-simulator
```

## Scale & Resilience Discussion
Horizontal scaling requires configuring a sufficient number of partitions per topic, allowing Kafka to distribute the load among all active gateway instances within the same consumer group. In the event of a gateway failure, fault tolerance is handled by Kafka's internal offset management system. When an instance restarts or a partition is reassigned, the consumer reads the last committed offset specific to that partition and resumes processing, ensuring seamless recovery.
This pattern works effortlessly at scale if the communication is asynchronous—meaning the client sends a fire-and-forget message over HTTP, receives an immediate status response, and does not keep the connection open waiting for the remote network's answer. Under this async model, gateways can safely use standard consumer groups and dynamic topic subscriptions (subscribe()).
Assuming a synchronous pattern is required—where a client keeps its HTTP connection open waiting for a specific response from the destination network—the architecture must adapt as follows:
1. Initialization: When a specific gateway instance initializes, it does not join a dynamic consumer group. Instead, it assigns itself a static identifier (e.g., Gateway-Partition-2) and uses manual partition assignment (assign() instead of subscribe()) to explicitly lock onto a designated, exclusive partition of its own network's inbound reply topic.
2. Routing the Reply: When an incoming HTTP request arrives, the gateway generates a unique Correlation ID for that specific request and tracks the open HTTP connection context in a local, in-memory map. It then forwards the request to the target network's Kafka topic, attaching its own assigned partition number (or its unique gateway identifier) to the message metadata.
3. Processing and Return: The remote network processes the request and sends the response back. Crucially, the remote network uses the gateway's identifier as the Kafka partition key (or directly targets the requested partition number). This ensures that Kafka routes the reply directly to the exact partition that the originating gateway instance is exclusively listening to.
4. Resolution: The gateway instance polls Kafka, retrieves the reply from its assigned partition, matches the Correlation ID against its local in-memory map to find the waiting HTTP connection, and streams the response back to the client.

###  Slow Backend
Slow backend can cause many open requests on the gateway which will lead to out of memory eventually
The way to handle it:
• Bounded Memory Maps: Do not use an unrestricted map. Use a cache structure with a hard capacity limit or an eviction policy
• HTTP Circuit Breaking & Rate Limiting: Track the size of your in-memory map. If the number of active, waiting HTTP requests exceeds a safe threshold (e.g., 10,000 active requests per instance), the gateway should immediately reject incoming HTTP requests with an HTTP 429 Too Many Requests or HTTP 503 Service Unavailable. This shields your memory footprint.
• Aggressive HTTP Timeouts: Never let an HTTP request hang indefinitely. Set a strict timeout on your HTTP server (e.g., 15–30 seconds). If a response doesn't arrive via Kafka within that window, evict the Correlation ID from your memory map and return an HTTP 504 Gateway Timeout to the client.

### Broker Downtime
• For Outbound Requests (Publishing):
	- reduce block time from the default 60 seconds and throw error of service unavailable
• For Inbound Requests (Consuming):
	- Retry to connect until kafka resolve. no messages will arrive on this time

### Dead partition
Dead partition can cause a "zombie" gateway instance to be cut off from Kafka while still accepting HTTP traffic from the load balancer
- Adding kafka health check inside gateway service. If partition is dead service should be killed and not accept calls

## Automated Route Management as a Service

To register a new service it is just necessary to start a client and gateway services with the following docker-compose.yml
```
version: '3.8'

services:
  # --- NETWORK C CLIENT ---
  client-c:
    build: ./client
    container_name: client-c
    ports:
      - "8003:8000"
    environment:
      - NETWORK_ID=network-c
      - GATEWAY_URL=http://gateway-c:8000
    networks:
      - net-c

  # --- NETWORK C GATEWAY ---
  gateway-c:
    build: ./gateway
    container_name: gateway-c
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - NETWORK_ID=network-c
      - CLIENT_URL=http://client-c:8000
    depends_on:
      - client-c
    networks:
      - net-c
      - kafka-bus

networks:
  net-c:
    driver: bridge
  kafka-bus:
    external:
      name:  sync-over-async_kafka-bus  # This should be replaced with the network inside the docker
```





# Project Name: sync over async poc

**sync over async poc** is a network-isolated, asynchronous message-bridge proof-of-concept using **FastAPI** and **Apache Kafka (KRaft mode)**. 

Services reside within isolated Docker networks (`net-a` and `net-b`) and are completely prevented from communicating directly with each other over standard HTTP. All inter-network communication is routed through dedicated **Gateway** services acting as a control plane over a central Kafka bus (`kafka-bus`).

---

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
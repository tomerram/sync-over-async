FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir httpx

# Copy script into container
COPY simulate_conversation.py .

CMD ["python", "simulate_conversation.py"]
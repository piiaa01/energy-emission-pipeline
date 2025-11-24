FROM python:3.11-slim

#packages for confluent-kafka, psutil... 
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

#python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#code
COPY . .

#for imports
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["bash"]
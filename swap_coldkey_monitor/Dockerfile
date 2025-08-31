FROM python:3.12.3-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src

ENV PYTHONUNBUFFERED=1

CMD ["python3",  "src/main.py"]


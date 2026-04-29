# App Project

This folder contains:
- `frontend` (React + Vite dashboard)
- `backend` (FastAPI endpoints over PostgreSQL)
- `spark_streaming` (structured streaming jobs that update parquet + PostgreSQL)
- `firehose.ts` (enhanced firehose collector for bronze + streaming feeds)
- `getPosts_streaming.py` and `getPosts_streaming.ipynb` (enhanced getPosts pipeline)

## Database

1. Start PostgreSQL and create `bluesky_db`.
2. Run:
   - `psql "postgresql://backend_user:supersecretpassword@localhost:5432/bluesky_db" -f app/backend/create_tables.sql`

## Backend

```bash
cd app/backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Frontend

```bash
cd app/frontend
npm install
npm run dev
```

To configure backend URL:
- set `VITE_API_BASE_URL`, default is `http://localhost:8000`

## Streaming jobs

Run each in separate terminals:

```bash
python app/spark_streaming/vaderSentimentTimeSeries.py
python app/spark_streaming/ingestionMetricsTimeline.py
python app/spark_streaming/controversialTopicsTimeseries.py
python app/spark_streaming/redditCrossoverStats.py
python app/getPosts_streaming.py
```

Run TypeScript firehose collector:

```bash
cd app
npm install ws
npx ts-node firehose.ts
```

## Optional Kafka Streaming (Firehose + getPosts -> all Spark jobs)

Start Kafka:

```bash
cd app
docker compose -f docker-compose.kafka.yml up -d
```

Install Python Kafka client once (for `getPosts_streaming.py` publisher):

```bash
pip install kafka-python
```

Run firehose with Kafka publishing enabled:

```bash
cd app
KAFKA_ENABLED=true KAFKA_BROKERS=localhost:9092 KAFKA_FIREHOSE_TOPIC=bluesky.firehose.raw npx ts-node firehose.ts
```

Run getPosts with Kafka publishing enabled:

```bash
cd app
KAFKA_ENABLED=true KAFKA_BROKERS=localhost:9092 KAFKA_GETPOSTS_TOPIC=bluesky.getposts.raw python getPosts_streaming.py
```

Run all 4 Spark jobs from Kafka:

```bash
cd app/spark_streaming
USE_KAFKA_FIREHOSE_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_FIREHOSE_TOPIC=bluesky.firehose.raw python vaderSentimentTimeSeries.py
USE_KAFKA_SOURCES=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_FIREHOSE_TOPIC=bluesky.firehose.raw KAFKA_GETPOSTS_TOPIC=bluesky.getposts.raw python ingestionMetricsTimeline.py
USE_KAFKA_GETPOSTS_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_GETPOSTS_TOPIC=bluesky.getposts.raw python controversialTopicsTimeseries.py
USE_KAFKA_GETPOSTS_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_GETPOSTS_TOPIC=bluesky.getposts.raw python redditCrossoverStats.py
```


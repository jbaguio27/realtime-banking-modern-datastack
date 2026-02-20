import argparse
import json
import os
import time
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=ENV_PATH)

# Source topics produced by Debezium
TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

BATCH_SIZE = 50
MAX_WAIT_SECONDS = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Kafka CDC events and upload parquet batches to MinIO"
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=300,
        help="Maximum runtime before graceful shutdown and flush",
    )
    parser.add_argument(
        "--max-idle-cycles",
        type=int,
        default=3,
        help="Stop after N idle poll cycles with no messages",
    )
    return parser.parse_args()


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_GROUP", "minio-landing-group"),
        api_version=(0, 10, 2),
        security_protocol="PLAINTEXT",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
        # Prevent blocking forever when no new messages arrive.
        consumer_timeout_ms=1000,
    )


def create_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )


def flush_to_minio(
    topic: str,
    buffer: dict[str, list[dict]],
    last_flush_time: dict[str, float],
    s3_client,
    bucket: str,
) -> None:
    records = buffer[topic]
    if not records:
        return

    table_name = topic.split(".")[-1]
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%H%M%S%f")

    file_path = f"temp_{table_name}_{timestamp_str}.parquet"
    df.to_parquet(file_path, engine="fastparquet", index=False)

    s3_key = f"{table_name}/date={date_str}/{table_name}_{timestamp_str}.parquet"
    s3_client.upload_file(file_path, bucket, s3_key)

    os.remove(file_path)
    print(f"BATCH UPLOAD: {len(records)} {table_name} records -> s3://{bucket}/{s3_key}")

    buffer[topic] = []
    last_flush_time[topic] = time.time()


def flush_all_buffers(
    buffer: dict[str, list[dict]],
    last_flush_time: dict[str, float],
    s3_client,
    bucket: str,
) -> None:
    for topic_name in TOPICS:
        if buffer[topic_name]:
            flush_to_minio(topic_name, buffer, last_flush_time, s3_client, bucket)


def run() -> None:
    args = parse_args()
    consumer = create_consumer()
    s3_client = create_minio_client()
    bucket = os.getenv("MINIO_BUCKET")

    if not bucket:
        raise ValueError("MINIO_BUCKET is not set")

    buffer = {topic: [] for topic in TOPICS}
    last_flush_time = {topic: time.time() for topic in TOPICS}

    print(
        f"Monitoring Kafka. Batch size={BATCH_SIZE}, max_wait={MAX_WAIT_SECONDS}s, "
        f"max_runtime={args.max_runtime_seconds}s, max_idle_cycles={args.max_idle_cycles}"
    )

    try:
        start_time = time.time()
        idle_cycles = 0

        while True:
            got_any_message = False

            for message in consumer:
                got_any_message = True
                if message.value is None:
                    continue

                topic = message.topic
                payload = message.value.get("payload", {})
                record = payload.get("after") if payload else None
                if not record:
                    continue

                buffer[topic].append(record)
                if len(buffer[topic]) >= BATCH_SIZE:
                    flush_to_minio(topic, buffer, last_flush_time, s3_client, bucket)

            # Flush stale buffered records.
            for topic_name in TOPICS:
                is_stale = (time.time() - last_flush_time[topic_name]) > MAX_WAIT_SECONDS
                if buffer[topic_name] and is_stale:
                    print(
                        f"Timeout reached for {topic_name}. "
                        f"Flushing remaining {len(buffer[topic_name])} records."
                    )
                    flush_to_minio(topic_name, buffer, last_flush_time, s3_client, bucket)

            # Stop conditions for bounded execution.
            idle_cycles = 0 if got_any_message else idle_cycles + 1

            if idle_cycles >= args.max_idle_cycles:
                print("No new messages for max idle cycles. Stopping consumer run.")
                break

            if (time.time() - start_time) >= args.max_runtime_seconds:
                print("Reached max runtime. Stopping consumer run.")
                break

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        flush_all_buffers(buffer, last_flush_time, s3_client, bucket)
        consumer.close()


if __name__ == "__main__":
    run()

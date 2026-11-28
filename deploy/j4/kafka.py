"""Independent Kafka witness and exact-byte redelivery in the isolated probe pod."""

import argparse
import base64
import json
import sys
import threading
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from workers.recorded_probe import clock_sample

TOPIC = "durable-agent.tasks.v1"
GROUP = "runtime-workers-v1"


def observe(address: str) -> None:
    partition = TopicPartition(TOPIC, 0)
    config = {"bootstrap_servers": [address], "api_version": (3, 9, 0)}
    notices = KafkaConsumer(**config, enable_auto_commit=False, auto_offset_reset="earliest")
    group = KafkaConsumer(**config, group_id=GROUP, enable_auto_commit=False)
    producer = KafkaProducer(**config, acks="all", retries=0, max_block_ms=2000, linger_ms=0)
    notices.assign([partition])
    group.assign([partition])
    notices.seek_to_beginning(partition)
    # Resolve metadata and the group coordinator before the controller submits.
    if (
        producer.partitions_for(TOPIC) != {0}
        or notices.beginning_offsets([partition])[partition] != 0
    ):
        raise ValueError("J4 requires a fresh single-partition task topic")
    group.committed(partition)
    lock = threading.Lock()
    original = {}

    def record(kind: str, **row) -> None:
        clock = clock_sample()
        with lock:
            print(
                json.dumps({"kind": kind, "at_ns": clock.pop("monotonic_ns"), **clock, **row}),
                flush=True,
            )

    def encode(data: bytes) -> str:
        return base64.b64encode(data).decode("ascii")

    def republish() -> None:
        for line in sys.stdin:
            request = json.loads(line)
            ordinal = request["delivery"]
            try:
                raw = original[request["original_offset"]]
                stamp = time.monotonic_ns()
                ack = producer.send(
                    TOPIC, partition=0, key=raw.key, value=raw.value, headers=raw.headers
                ).get(timeout=3)
                record(
                    "republished",
                    delivery=ordinal,
                    original_offset=raw.offset,
                    offset=ack.offset,
                    partition=ack.partition,
                    send_ns=stamp,
                )
            except Exception as exc:
                record("publish_error", delivery=ordinal, error_type=type(exc).__name__)

    threading.Thread(target=republish, daemon=True).start()
    record("ready")
    while True:
        for batch in notices.poll(timeout_ms=60, max_records=32).values():
            for message in batch:
                headers = dict(message.headers)
                if len(headers) != len(message.headers):
                    raise ValueError("duplicate J4 broker headers")
                original[message.offset] = message
                record(
                    "notice",
                    offset=message.offset,
                    partition=message.partition,
                    event_id=headers["event-id"].decode(),
                    event_type=headers["event-type"].decode(),
                    schema_version=int(headers["schema-version"]),
                    key_b64=encode(message.key),
                    value_b64=encode(message.value),
                    headers=[[name, encode(value)] for name, value in message.headers],
                )
        record("offset", offset=group.committed(partition))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("broker")
    observe(parser.parse_args().broker)

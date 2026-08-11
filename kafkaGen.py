#!/usr/bin/env python3
"""
Kafka Gen script for VTOM: quick message check.

Positional parameters:
  $1  bootstrap_servers
  $2  topic
  $3  group_id
  $4  expected_text
  $5  commit_mode         optional (default: 0)
                          0 = no commit
                          1 = commit fetched batch
                          2 = commit up to first matching message
  $6  expected_ids        optional, comma-separated IDs
  $7  security_protocol   optional (default: PLAINTEXT)
  $8  sasl_mechanism      optional
  $9  username            optional
  $10 password            optional
  $11 ssl_cafile          optional

Exit codes:
  0 = matching message found
  1 = no matching message / error
"""

import logging
import sys
import time

from kafka import KafkaConsumer
from kafka.serializer import DefaultSerializer
from kafka.structs import OffsetAndMetadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
    force=True,
)

POLL_TIMEOUT_MS = 3000

COMMIT_MODE_LABELS = {
    0: "none",
    1: "full batch",
    2: "up to first match",
}


def parse_commit_mode(value):
    """Parse $5 commit mode: 0=none, 1=batch, 2=first match."""
    raw = str(value).strip().lower()
    if raw in {"", "0", "false", "no", "n"}:
        return 0
    if raw in {"1", "true", "yes", "y", "batch"}:
        return 1
    if raw in {"2", "first"}:
        return 2
    raise ValueError(
        f"Invalid $5 value '{value}'. Expected: 0 (none), 1 (batch), 2 (first match)"
    )


# === Argument validation ===
if len(sys.argv) < 5:
    logging.error(
        "Usage: python kafkaGen.py <bootstrap_servers> <topic> <group_id> "
        "<expected_text> [commit_mode] [expected_ids] [security_protocol] "
        "[sasl_mechanism] [username] [password] [ssl_cafile]"
    )
    sys.exit(1)

BOOTSTRAP_SERVERS = sys.argv[1]
TOPIC = sys.argv[2]
GROUP_ID = sys.argv[3]
EXPECTED_TEXT = sys.argv[4]

try:
    COMMIT_MODE = parse_commit_mode(sys.argv[5] if len(sys.argv) > 5 else "0")
except ValueError as exc:
    logging.error(str(exc))
    sys.exit(1)

EXPECTED_IDS_RAW = sys.argv[6] if len(sys.argv) > 6 else ""
SECURITY_PROTOCOL = sys.argv[7] if len(sys.argv) > 7 and sys.argv[7] else "PLAINTEXT"
SASL_MECHANISM = sys.argv[8] if len(sys.argv) > 8 else ""
USERNAME = sys.argv[9] if len(sys.argv) > 9 else ""
PASSWORD = sys.argv[10] if len(sys.argv) > 10 else ""
SSL_CAFILE = sys.argv[11] if len(sys.argv) > 11 else ""

EXPECTED_IDS = [item.strip() for item in EXPECTED_IDS_RAW.split(",") if item.strip()]


def build_consumer():
    """Build a configured KafkaConsumer."""
    config = {
        "bootstrap_servers": [
            item.strip() for item in BOOTSTRAP_SERVERS.split(",") if item.strip()
        ],
        "group_id": GROUP_ID,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "request_timeout_ms": 10000,
        "session_timeout_ms": 10000,
        "security_protocol": SECURITY_PROTOCOL or "PLAINTEXT",
        "value_deserializer": DefaultSerializer(),
    }

    if SASL_MECHANISM:
        config["sasl_mechanism"] = SASL_MECHANISM
    if USERNAME:
        config["sasl_plain_username"] = USERNAME
    if PASSWORD:
        config["sasl_plain_password"] = PASSWORD
    if SSL_CAFILE:
        config["ssl_cafile"] = SSL_CAFILE

    return KafkaConsumer(TOPIC, **config)


def wait_for_assignment(consumer, timeout_sec=10):
    """Wait until Kafka assigns partitions to the consumer."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        # Required to trigger rebalance; may consume messages that we rewind later
        consumer.poll(timeout_ms=200)
        if consumer.assignment():
            return True
    return False


def reset_positions(consumer):
    """
    Seek to the group's committed offset, or to the beginning if none exists.
    Prevents losing messages consumed during assignment wait.
    """
    for tp in sorted(consumer.assignment(), key=lambda item: item.partition):
        committed = consumer.committed(tp)
        beginning = consumer.beginning_offsets([tp])[tp]
        end = consumer.end_offsets([tp])[tp]

        if committed is None:
            consumer.seek_to_beginning(tp)
            position = consumer.position(tp)
            logging.info(
                f"Partition {tp.partition}: no commit for this group_id -> "
                f"reading from beginning "
                f"(begin={beginning} end={end} position={position})"
            )
        else:
            consumer.seek(tp, committed)
            position = consumer.position(tp)
            logging.info(
                f"Partition {tp.partition}: commit={committed} "
                f"begin={beginning} end={end} position={position} "
                f"remaining={max(0, end - position)}"
            )


def message_is_valid(payload):
    """Return True when payload matches expected text/IDs."""
    if EXPECTED_TEXT not in payload:
        return False
    if not EXPECTED_IDS:
        return True
    return any(expected_id in payload for expected_id in EXPECTED_IDS)


def do_commit(consumer, topic_partition, message):
    """Apply commit according to mode 0 / 1 / 2."""
    if COMMIT_MODE == 0:
        logging.info("Commit not requested (message will remain readable)")
        return

    if COMMIT_MODE == 1:
        # Commit current consumer positions for the fetched batch
        consumer.commit()
        logging.info("Commit done (full batch)")
        return

    # Mode 2: commit only up to the matching message
    next_offset = message.offset + 1
    consumer.commit({
        topic_partition: OffsetAndMetadata(next_offset, "")
    })
    logging.info(
        "Commit done up to first match "
        f"(partition={topic_partition.partition}, next_offset={next_offset})"
    )


# === Main execution ===
consumer = None
try:
    logging.info("Starting Kafka Gen check")
    logging.info(f"Servers           : {BOOTSTRAP_SERVERS}")
    logging.info(f"Topic             : {TOPIC}")
    logging.info(f"Group ID          : {GROUP_ID}")
    logging.info(f"Expected text     : {EXPECTED_TEXT}")
    logging.info(
        f"Expected IDs      : {EXPECTED_IDS if EXPECTED_IDS else '(none)'}"
    )
    logging.info(
        f"Commit mode       : {COMMIT_MODE} ({COMMIT_MODE_LABELS[COMMIT_MODE]})"
    )
    logging.info(f"Security protocol : {SECURITY_PROTOCOL}")

    logging.info("Connecting to Kafka broker...")
    consumer = build_consumer()
    logging.info("Successfully connected to Kafka")

    logging.info("Waiting for partition assignment...")
    if not wait_for_assignment(consumer):
        logging.error("No partition assigned (timeout)")
        sys.exit(1)

    assigned = sorted(consumer.assignment(), key=lambda tp: tp.partition)
    logging.info(
        "Assigned partitions: "
        + ", ".join(f"{tp.topic}:{tp.partition}" for tp in assigned)
    )

    reset_positions(consumer)

    logging.info(f"Reading messages (poll timeout={POLL_TIMEOUT_MS} ms)...")
    records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=50)

    if not records:
        logging.info("No message available for this consumer/group")
        logging.info("Result: KO (code 1)")
        sys.exit(1)

    total = sum(len(messages) for messages in records.values())
    logging.info(f"{total} message(s) read")

    for topic_partition, messages in records.items():
        for message in messages:
            payload = "" if message.value is None else str(message.value)
            logging.info(
                f"Message offset={message.offset} "
                f"partition={topic_partition.partition}"
            )

            if message_is_valid(payload):
                logging.info("Matching message found")
                do_commit(consumer, topic_partition, message)
                logging.info("Result: OK (code 0)")
                sys.exit(0)

            logging.info("Message ignored: does not match criteria")

    logging.info("Messages were read but none matched")
    logging.info("Result: KO (code 1)")
    sys.exit(1)

except Exception as exc:
    logging.error(f"Error during execution: {exc}")
    sys.exit(1)

finally:
    if consumer is not None:
        logging.info("Closing Kafka consumer")
        consumer.close()

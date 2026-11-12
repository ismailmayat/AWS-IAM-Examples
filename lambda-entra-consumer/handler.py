"""
Confluent Cloud consumer Lambda (Entra ID OAuth via Lambda event source mapping)
================================================================================

This function contains NO Kafka client and NO credentials. The Lambda event
source mapping (ESM) does all the Kafka work:

  1. Reads the Entra ID client credentials from Secrets Manager.
  2. Requests an access token from Entra ID (client_credentials grant).
  3. Connects to Confluent Cloud with SASL_SSL / OAUTHBEARER, sending the
     logicalCluster and identityPoolId SASL extensions.
  4. Polls the topic and invokes this handler with batches of records.
  5. Commits offsets for the consumer group after the handler returns.

If this handler raises, the ESM retries the entire batch, so processing must
be idempotent (the same record can be delivered more than once).

Event shape (abridged):
{
  "eventSource": "SelfManagedKafka",
  "bootstrapServers": "pkc-xxxxx.region.provider.confluent.cloud:9092",
  "records": {
    "my_topic-0": [
      {"topic": "my_topic", "partition": 0, "offset": 15,
       "timestamp": 1545084650987, "timestampType": "CREATE_TIME",
       "key": "<base64>", "value": "<base64>",
       "headers": [{"headerKey": [104, 105]}]}
    ]
  }
}
"""

import base64
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def _b64_to_str(data):
    """Decode a base64 field from the ESM payload to text (None-safe)."""
    if data is None:
        return None
    raw = base64.b64decode(data)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        # Non-text payloads (e.g. Avro/Protobuf wire format) - keep as base64.
        return data


def _decode_headers(headers):
    """ESM headers are a list of {name: [byte, byte, ...]} dicts."""
    decoded = {}
    for header in headers or []:
        for name, byte_values in header.items():
            decoded[name] = bytes(byte_values).decode("utf-8", errors="replace")
    return decoded


def _parse_value(text):
    """Return parsed JSON if the value is JSON, otherwise the raw text."""
    if text is None:
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text


def process_record(record):
    """
    Business logic for a single record. Replace this with your own processing.

    Keep it idempotent: on failure the whole batch is redelivered.
    """
    key = _b64_to_str(record.get("key"))
    value = _parse_value(_b64_to_str(record.get("value")))
    headers = _decode_headers(record.get("headers"))

    logger.info(
        json.dumps(
            {
                "topic": record["topic"],
                "partition": record["partition"],
                "offset": record["offset"],
                "timestamp": record.get("timestamp"),
                "key": key,
                "value": value,
                "headers": headers,
            },
            default=str,
        )
    )


def lambda_handler(event, context):
    records_by_partition = event.get("records", {})
    total = 0

    for topic_partition, records in records_by_partition.items():
        logger.info("Processing %d record(s) from %s", len(records), topic_partition)
        for record in records:
            process_record(record)
            total += 1

    logger.info("Batch complete: %d record(s) processed", total)
    return {"processed": total}

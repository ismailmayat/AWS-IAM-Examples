#!/usr/bin/env python3
"""
Confluent Cloud Kafka Producer using AWS IAM Authentication
===========================================================

This producer authenticates to Confluent Cloud using AWS IAM credentials
via EKS Pod Identity. No API keys or secrets needed!

Key Features:
- Uses AWS STS GetWebIdentityToken for OAuth authentication
- Runs on Amazon EKS with Pod Identity
- Produces sample messages to demonstrate connectivity
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import boto3
from confluent_kafka import Producer


def get_aws_iam_token(client_id, aws_region='us-west-2'):
    """
    Request AWS IAM token using STS GetWebIdentityToken.

    This function is called by the Kafka client when it needs an OAuth token.
    The EKS Pod Identity provides the AWS credentials automatically.

    Args:
        client_id: The Confluent Cloud OAuth client ID (your AWS account ID)
        aws_region: AWS region for STS endpoint

    Returns:
        tuple: (token_string, expiry_seconds)
    """
    print(f"[{datetime.now(timezone.utc).isoformat()}] Requesting AWS IAM token...")

    sts_client = boto3.client('sts', region_name=aws_region)

    # Try different token durations (some AWS accounts have limits)
    for duration in [3600, 1800, 900]:  # 1hr, 30min, 15min
        try:
            response = sts_client.get_web_identity_token(
                Audience=[client_id],
                SigningAlgorithm='ES384',
                DurationSeconds=duration
            )

            token = response['WebIdentityToken']
            print(f"[{datetime.now(timezone.utc).isoformat()}] ✅ Got token (expires in {duration}s)")

            return token, duration

        except sts_client.exceptions.SessionDurationEscalationException:
            print(f"[{datetime.now(timezone.utc).isoformat()}] ⚠️  {duration}s duration not allowed, trying shorter...")
            continue
        except Exception as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] ❌ Error getting token: {e}")
            raise

    raise Exception("Could not get AWS IAM token with any duration")


def oauth_cb(oauth_config):
    """
    OAuth callback function required by confluent-kafka.

    This is called automatically when the Kafka client needs a new token.
    Returns a 4-tuple: (token, expiry_timestamp, principal, extensions_dict)
    """
    # oauth_config is a dict with cluster and account info
    client_id = oauth_config['aws_account_id']
    logical_cluster = oauth_config['logical_cluster']

    token, expiry = get_aws_iam_token(client_id)

    # Return token with SASL extensions as a dictionary
    # Extensions must be passed in the callback return value, not in config
    # The logicalCluster extension can be omitted for supported networking and cluster types.
    # See https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/overview.html
    return token, time.time() + expiry, "", {"logicalCluster": logical_cluster}


def create_producer_config():
    """
    Create Kafka producer configuration with AWS IAM authentication.

    Configuration comes from environment variables (set via ConfigMap in Kubernetes).
    """
    # Read configuration from environment
    bootstrap_servers = os.environ['BOOTSTRAP_SERVERS']
    logical_cluster = os.environ['LOGICAL_CLUSTER']
    aws_account_id = os.environ['AWS_ACCOUNT_ID']

    print(f"[{datetime.now(timezone.utc).isoformat()}] Initializing producer...")
    print(f"  Bootstrap: {bootstrap_servers}")
    print(f"  Cluster: {logical_cluster}")
    print(f"  AWS Account: {aws_account_id}")

    # Create config dict for oauth callback
    oauth_config = {
        'logical_cluster': logical_cluster,
        'aws_account_id': aws_account_id,
    }

    config = {
        # Kafka cluster
        'bootstrap.servers': bootstrap_servers,

        # Security: SASL_SSL with OAUTHBEARER
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',

        # OAuth configuration
        # Note: oauth_cb provides custom tokens, so token endpoint URL is not used
        'oauth_cb': lambda cfg: oauth_cb(oauth_config),

        # Producer settings
        'client.id': 'aws-iam-producer',
        'acks': 'all',
    }

    return config


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ❌ Message delivery failed: {err}")
    else:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ✅ Message delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def main():
    """Main producer loop - sends sample messages to demonstrate AWS IAM auth."""

    print("=" * 70)
    print("Confluent Cloud Producer with AWS IAM Authentication")
    print("=" * 70)
    print()

    # Get configuration
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'test_topic')
    message_count = int(os.environ.get('MESSAGE_COUNT', '100'))

    # Create producer
    config = create_producer_config()
    producer = Producer(config)

    print()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting to produce {message_count} messages to '{kafka_topic}'")
    print()

    # Produce messages
    for i in range(message_count):
        # Create sample message
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'message_number': i + 1,
            'value': random.randint(1, 100),
            'source': 'aws-iam-producer'
        }

        # Produce to Kafka
        producer.produce(
            kafka_topic,
            key=f'key-{i}',
            value=json.dumps(message),
            callback=delivery_callback
        )

        # Flush periodically to trigger delivery callbacks
        if (i + 1) % 10 == 0:
            producer.poll(0)

        # Small delay to avoid overwhelming the broker
        time.sleep(0.1)

    # Wait for all messages to be delivered
    print()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Flushing remaining messages...")
    producer.flush()

    print()
    print("=" * 70)
    print("✅ SUCCESS! All messages produced using AWS IAM authentication")
    print("=" * 70)


if __name__ == '__main__':
    main()

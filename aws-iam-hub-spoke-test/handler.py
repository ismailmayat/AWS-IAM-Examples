"""
Hub-and-spoke AWS IAM OAuth test consumer (see aws-iam-oauth-hub-spoke.md)
=============================================================================

Runtime role (Lambda execution role, "spoke")
    -- sts:AssumeRole -->  hub role /kafka/<env>-<workload>
    -- sts:GetWebIdentityToken (hub role creds) -->  JWT, sub = hub role ARN
    -- SASL_SSL / OAUTHBEARER + logicalCluster + identityPoolId -->  Confluent Cloud

The hop is done by the AWS SDK credential chain from the shipped profile
file (aws-config, credential_source = Environment). The function never
calls AssumeRole itself.

Invoke with an optional event:
  {"max_messages": 10, "max_seconds": 20, "duration_seconds": 900}
Setting duration_seconds above the hub role's cap (900) is a negative test:
GetWebIdentityToken must be denied.
"""

import base64
import json
import os
import time

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException

REGION = os.environ["AWS_REGION"]
HUB_PROFILE = os.environ["KAFKA_HUB_PROFILE"]
AUDIENCE = os.environ["TOKEN_AUDIENCE"]
DEFAULT_DURATION = int(os.environ.get("TOKEN_DURATION_SECONDS", "900"))


def _log(**fields):
    print(json.dumps(fields, default=str))


def _claims(jwt):
    """Decode the JWT payload for logging. The token itself is never logged."""
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def _whoami(session, label):
    try:
        arn = session.client("sts", region_name=REGION).get_caller_identity()["Arn"]
    except Exception as e:  # noqa: BLE001 - diagnostic only
        arn = f"ERROR: {e}"
    _log(event="caller_identity", via=label, arn=arn)
    return arn


# Diagnostic: does plain AWS_PROFILE (the design doc's "no code change"
# assumption) give hub-role credentials in Lambda, or do the Lambda
# environment credentials win?
_whoami(boto3.Session(), "default chain (AWS_PROFILE env var only)")

# Explicit profile: botocore then skips the environment-variable provider and
# uses the profile's assume-role (credential_source = Environment) provider.
HUB_SESSION = boto3.Session(profile_name=HUB_PROFILE)
_whoami(HUB_SESSION, f"explicit profile '{HUB_PROFILE}'")
HUB_STS = HUB_SESSION.client("sts", region_name=REGION)  # regional endpoint


def mint_token(duration_seconds):
    resp = HUB_STS.get_web_identity_token(
        Audience=[AUDIENCE],
        SigningAlgorithm="RS256",
        DurationSeconds=duration_seconds,
    )
    token = resp["WebIdentityToken"]
    _log(event="token_minted", duration_seconds=duration_seconds, claims=_claims(token))
    return token


def build_consumer(duration_seconds):
    extensions = {
        "logicalCluster": os.environ["LOGICAL_CLUSTER"],
        "identityPoolId": os.environ["IDENTITY_POOL_ID"],
    }

    def oauth_cb(_oauthbearer_config):
        token = mint_token(duration_seconds)
        return token, time.time() + duration_seconds, "", extensions

    return Consumer(
        {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": oauth_cb,
            "group.id": os.environ["CONSUMER_GROUP_ID"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "client.id": "hub-spoke-test-consumer",
        }
    )


def lambda_handler(event, context):
    event = event or {}
    max_messages = int(event.get("max_messages", 10))
    max_seconds = float(event.get("max_seconds", 20))
    duration = int(event.get("duration_seconds", DEFAULT_DURATION))

    # Mint once up front so STS errors (AccessDenied on AssumeRole or
    # GetWebIdentityToken) surface clearly instead of inside librdkafka.
    try:
        mint_token(duration)
    except Exception as e:  # noqa: BLE001
        _log(event="token_error", error=str(e))
        return {"ok": False, "stage": "sts", "error": str(e)}

    consumer = build_consumer(duration)
    consumer.subscribe([os.environ["TOPIC"]])
    consumed, deadline = [], time.time() + max_seconds
    try:
        while len(consumed) < max_messages and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            record = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8", "replace") if msg.key() else None,
                "value": msg.value().decode("utf-8", "replace")[:500] if msg.value() else None,
            }
            _log(event="record", **record)
            consumed.append(record)
        if consumed:
            consumer.commit(asynchronous=False)
    except KafkaException as e:
        _log(event="kafka_error", error=str(e))
        return {"ok": False, "stage": "kafka", "error": str(e), "consumed": len(consumed)}
    finally:
        consumer.close()

    return {"ok": True, "consumed": len(consumed)}

"""
ESM variant of the hub-and-spoke test (expected to FAIL authentication).

The Lambda native Kafka trigger (event source mapping) with
IAM_OAUTHBEARER_AUTH mints its token as THIS function's execution role.
It cannot assume the hub role first, so the token's `sub` is the spoke
execution role, which the hub identity pool's filter does not accept.

Two ways this function runs:
  * Invoked by the ESM with Kafka records: only happens if authentication
    succeeded, which it should not. Records are logged.
  * Invoked directly (any non-Kafka event): diagnostic mode. Mints a token
    the same way the ESM does (execution role, same audience) and logs the
    decoded claims, so you can compare `sub` with the hub pool filter.
"""

import base64
import json
import os

import boto3

AUDIENCE = os.environ["TOKEN_AUDIENCE"]
HUB_ROLE_ARN = os.environ["HUB_ROLE_ARN"]


def _claims(jwt):
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def _diagnose():
    sts = boto3.client("sts", region_name=os.environ["AWS_REGION"])
    caller = sts.get_caller_identity()["Arn"]
    try:
        token = sts.get_web_identity_token(
            Audience=[AUDIENCE], SigningAlgorithm="RS256", DurationSeconds=900
        )["WebIdentityToken"]
    except Exception as e:  # noqa: BLE001
        result = {"caller": caller, "token_error": str(e)}
        print(json.dumps(result))
        return result

    claims = _claims(token)
    result = {
        "caller": caller,
        "token_sub": claims.get("sub"),
        "token_iss": claims.get("iss"),
        "token_aud": claims.get("aud"),
        "hub_pool_expects_sub": HUB_ROLE_ARN,
        "matches_hub_pool": claims.get("sub") == HUB_ROLE_ARN,
    }
    print(json.dumps({"event": "esm_equivalent_token", **result, "claims": claims}, default=str))
    return result


def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("eventSource") == "SelfManagedKafka":
        count = 0
        for tp, records in event.get("records", {}).items():
            for r in records:
                value = base64.b64decode(r["value"]).decode("utf-8", "replace") if r.get("value") else None
                print(json.dumps({"event": "record", "tp": tp, "offset": r["offset"], "value": value}))
                count += 1
        print(json.dumps({"event": "UNEXPECTED_SUCCESS", "records": count}))
        return {"processed": count}

    return _diagnose()

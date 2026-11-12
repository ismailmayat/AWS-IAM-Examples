# Hub-and-spoke AWS IAM OAuth: single-account test

A test of the design in [aws-iam-oauth-hub-spoke.md](aws-iam-oauth-hub-spoke.md) using **one** AWS account. That account plays both roles:

- **Hub:** the account with outbound identity federation enabled. It holds the hub role `role/kafka/{env}-{workload}`.
- **Spoke:** the same account. It holds the Lambda execution role, which acts as the runtime role.

```mermaid
sequenceDiagram
    participant Fn as Lambda (runtime role)
    participant STS as AWS STS (regional endpoint)
    participant CC as Confluent Cloud

    Fn->>STS: AssumeRole hub role (SDK, profile kafka-hub,<br/>credential_source = Environment)
    STS-->>Fn: hub role credentials
    Fn->>STS: GetWebIdentityToken<br/>aud=confluent-cloud-kafka, 900 s
    STS-->>Fn: JWT (iss = hub issuer, sub = hub role ARN)
    Fn->>CC: SASL_SSL / OAUTHBEARER<br/>JWT + logicalCluster + identityPoolId
    Note over CC: verify against hub JWKS<br/>→ pool filter on exact sub + aud<br/>→ DeveloperRead on topic and group
    CC-->>Fn: records
```

**The test Lambda runs its own Kafka client (self-polling) only to exercise the hub hop.** It is not a recommended pattern for Lambda consumers, which should use the native Kafka trigger (ESM). The ESM can't assume a hub role. See [Lambda consumers](aws-iam-oauth-hub-spoke.md#lambda-consumers-the-native-kafka-trigger-esm-is-the-recommended-way-and-it-cant-use-the-hub) in the design doc.

**What this tests:** the role hop, token minting with the audience and duration caps, the `sub` format, the identity pool filter, RBAC, and the CloudTrail events.

**What it can't test:** the hop between two different accounts, and the organization-wide guardrail policy (SCP), which needs the AWS Organizations management account.

## Files

| Path | Purpose |
| --- | --- |
| `aws-iam-oauth-hub-spoke.md` | The design this folder tests. |
| `handler.py` | Test consumer. confluent-kafka with an `oauth_cb` that mints tokens using the hub role. It logs the decoded claims, never the token itself. |
| `aws-config.tftpl` | The AWS config profile file shipped in the function. Its `role_arn` is the hub role and `credential_source = Environment`. |
| `build-layer.sh`, `requirements.txt` | Build the Lambda layer containing confluent-kafka and boto3 (Python 3.13, x86_64). |
| `terraform/hub.tf` | Reads the federation issuer, and creates the hub role with the trust and permission policies from the design doc plus its required tags. |
| `terraform/spoke.tf` | The runtime role (allowed only to assume the hub role), the Lambda layer and the function. |
| `terraform/confluent.tf` | An AWS IAM identity provider for the hub issuer, an identity pool filtered on the exact hub role ARN, and `DeveloperRead` role bindings. |
| `terraform/tags.tf` | The `tags` variable, applied to every AWS resource through `default_tags`. |

## Prerequisites

- **Terraform** 1.5 or later, **Python 3** with `pip`, and `zip` (for `build-layer.sh`).
- **AWS account:**
  - Credentials that can create IAM roles and policies, Lambda functions and layers, and CloudWatch log groups.
  - **Outbound identity federation enabled.** Terraform only reads the issuer; it does not turn the feature on. Check with `aws iam get-outbound-web-identity-federation-info`, and enable it once with `aws iam enable-outbound-web-identity-federation` (or IAM console → Account settings).
  - **Membership of an AWS Organization.** The hub role's trust policy uses `aws:PrincipalOrgID`, which Terraform reads with `organizations:DescribeOrganization`.
  - No SCP that denies `sts:GetWebIdentityToken` in this account.
- **Confluent Cloud:**
  - An existing Kafka cluster with a public endpoint, and the topic already created.
  - A free OAuth identity provider slot (5 per organization by default); this test creates one AWS IAM provider.
  - A Cloud API key with OrganizationAdmin, or rights to create identity providers, identity pools and role bindings.

## Run

```bash
# AWS credentials for the target account, e.g. `aws sso login --profile <profile>`
export CONFLUENT_CLOUD_API_KEY=... CONFLUENT_CLOUD_API_SECRET=...

./build-layer.sh
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in
terraform init
terraform apply
```

## Test cases

Run these from the `terraform/` folder.

```bash
FN=$(terraform output -raw lambda_function_name)
invoke() {
  out=$(mktemp)
  aws lambda invoke --function-name "$FN" --cli-binary-format raw-in-base64-out \
    --payload "${1:-{\}}" "$out" >/dev/null && cat "$out"; echo
  rm -f "$out"
}
logs() { aws logs tail "/aws/lambda/$FN" --since 5m --format short "$@"; }
```

| # | Test | Command | Expected result |
| --- | --- | --- | --- |
| 1 | Happy path | `invoke '{"max_messages":5}'` | `{"ok": true, "consumed": N}`. The logs show `token_minted` with `sub` = the hub role ARN, including `/kafka/`. |
| 2 | Duration cap | `invoke '{"duration_seconds":3600}'` | `stage: sts`, `AccessDenied` on GetWebIdentityToken |
| 3 | Trust pinning | Remove the runtime role from the hub trust policy, then run test 1 | `stage: sts`, `AccessDenied` on AssumeRole |
| 4 | Pool filter | Change the pool filter's `sub`, then run test 1 | `stage: kafka`, SASL authentication failed |
| 5 | Audit | CloudTrail event history, filtering on `AssumeRole` and `GetWebIdentityToken` | Both events appear, with the hub role session |

Things to read in the logs:

- **`caller_identity` lines:** these show how the hop behaves in Lambda. The line logged `via=default chain (AWS_PROFILE env var only)` shows whether `AWS_PROFILE` on its own gives hub-role credentials. Lambda also sets its own credentials as environment variables, and the SDK may use those first. The line logged `via=explicit profile` shows what an explicit `boto3.Session(profile_name=...)` returns. The handler always mints tokens through the explicit profile.
- **`token_minted.claims["https://sts.amazonaws.com/"]`:** check whether the hub role's tags (`source-account`, `workload`, `environment`, `owner`) appear in the token as principal tags. If your `tags` variable already has one of these keys in a different case (for example `Owner`), the hub role uses that spelling, because IAM tag keys are case-insensitive.

## Clean up

```bash
terraform destroy
```

`destroy` doesn't turn off outbound identity federation, because Terraform never enabled it. Disable it manually if this account shouldn't issue tokens after the test.

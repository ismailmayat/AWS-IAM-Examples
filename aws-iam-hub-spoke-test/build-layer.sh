#!/usr/bin/env bash
# Build the Lambda layer (confluent-kafka + boto3) for python3.13 / x86_64.
# Run this before `terraform apply`. Output: terraform/.build/layer.zip
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD="$HERE/terraform/.build"
rm -rf "$BUILD/layer" "$BUILD/layer.zip"
mkdir -p "$BUILD/layer/python"

python3 -m pip install \
  --quiet \
  --target "$BUILD/layer/python" \
  --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 \
  --implementation cp --python-version 3.13 \
  --only-binary=:all: \
  -r "$HERE/requirements.txt"

(cd "$BUILD/layer" && zip -qr ../layer.zip python)
echo "Built $BUILD/layer.zip ($(du -h "$BUILD/layer.zip" | cut -f1))"

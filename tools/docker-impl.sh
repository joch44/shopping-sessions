#!/usr/bin/env bash
# Gate adapter: run the image ${IMG:-kit/m1} like the scorer would, on DATA_DIR -> OUTPUT_DIR.
# Used as IMPL for the osu repo's `make check` targets.
set -e
S=$(mktemp -d)
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL \
  --security-opt no-new-privileges --memory 24g --user "$(id -u):$(id -g)" -e NCPU="${NCPU:-8}" \
  -v "$(realpath "$DATA_DIR")":/data:ro -v "$(realpath "$OUTPUT_DIR")":/output -v "$S":/scratch -e SCRATCH_DIR=/scratch \
  "${IMG:-kit/m1}"
rc=$?; rm -rf "${S:?}"; exit $rc

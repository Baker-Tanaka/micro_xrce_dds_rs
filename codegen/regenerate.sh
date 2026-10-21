#!/usr/bin/env bash
set -euo pipefail
codegen="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$codegen/../../.." && pwd)"
if [[ ! -d "$repo/msgs" ]]; then
  echo "Repository msgs/ not found; use the Python CLI with explicit paths." >&2
  exit 1
fi
if [[ -f /opt/ros/jazzy/setup.bash ]]; then
  # ROS setup scripts do not guarantee compatibility with nounset.
  set +u
  source /opt/ros/jazzy/setup.bash
  set -u
fi
export PYTHONPATH="$codegen${PYTHONPATH:+:$PYTHONPATH}"
exec /usr/bin/python3 -m rosidl_generator_microxrcedds.cli \
  --messages "$repo/msgs" --output "$repo/generated" \
  --runtime "$codegen/.." "$@"

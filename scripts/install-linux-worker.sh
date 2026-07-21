#!/usr/bin/env bash
set -euo pipefail

image=""
coordinator_url=""
worker_token=""
run_window=""
abort_outside_window=0
isolate_conversion=0
gpu=0

usage() {
  cat <<'EOF'
Usage: install-linux-worker.sh --coordinator-url URL --token bfw_TOKEN [options]

Options:
  --image IMAGE                 Override the CPU or CUDA image selected by the installer
  --run-window HH:MM-HH:MM     Restrict conversion work to a local-time window
  --abort-outside-window       Abort and requeue conversion when the window closes
  --isolate-conversion         Isolate marker conversion in a child process
  --gpu                        Pass the host NVIDIA GPU through to the container
  -h, --help                   Show this help
EOF
}

while (($#)); do
  case "$1" in
    --coordinator-url) coordinator_url=${2-}; shift 2 ;;
    --token) worker_token=${2-}; shift 2 ;;
    --image) image=${2-}; shift 2 ;;
    --run-window) run_window=${2-}; shift 2 ;;
    --abort-outside-window) abort_outside_window=1; shift ;;
    --isolate-conversion) isolate_conversion=1; shift ;;
    --gpu) gpu=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ! $coordinator_url =~ ^https://[^[:space:]]+$ ]]; then
  printf '%s\n' 'A valid HTTPS --coordinator-url is required.' >&2
  exit 2
fi
if [[ ! $worker_token =~ ^bfw_[A-Za-z0-9_-]+$ ]]; then
  printf '%s\n' 'A UI-issued --token beginning with bfw_ is required.' >&2
  exit 2
fi
if [[ -n $run_window && ! $run_window =~ ^([01][0-9]|2[0-3]):[0-5][0-9]-([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
  printf '%s\n' '--run-window must use HH:MM-HH:MM.' >&2
  exit 2
fi
if [[ $abort_outside_window == 1 && -z $run_window ]]; then
  printf '%s\n' '--abort-outside-window requires --run-window.' >&2
  exit 2
fi

if [[ -z $image ]]; then
  if [[ $gpu == 1 ]]; then
    image="ghcr.io/tionis/blobforge:latest-cuda"
  else
    image="ghcr.io/tionis/blobforge:latest"
  fi
fi

if command -v podman >/dev/null 2>&1; then
  engine=$(command -v podman)
  engine_name=podman
elif command -v docker >/dev/null 2>&1; then
  engine=$(command -v docker)
  engine_name=docker
else
  printf '%s\n' 'Install Podman or Docker before running this installer.' >&2
  exit 1
fi
if ! command -v systemctl >/dev/null 2>&1 || ! systemctl --user show-environment >/dev/null 2>&1; then
  printf '%s\n' 'A running systemd user manager is required for service installation.' >&2
  exit 1
fi

config_dir=${XDG_CONFIG_HOME:-"$HOME/.config"}/blobforge
unit_dir=${XDG_CONFIG_HOME:-"$HOME/.config"}/systemd/user
cache_dir=${XDG_CACHE_HOME:-"$HOME/.cache"}/blobforge
mkdir -p "$config_dir" "$unit_dir" "$cache_dir"
chmod 700 "$config_dir"

umask 077
{
  printf 'BLOBFORGE_COORDINATOR_URL=%s\n' "$coordinator_url"
  printf 'BLOBFORGE_COORDINATOR_TOKEN=%s\n' "$worker_token"
} > "$config_dir/worker.env"

worker_args=(worker)
if [[ -n $run_window ]]; then worker_args+=(--run-window "$run_window"); fi
if [[ $abort_outside_window == 1 ]]; then worker_args+=(--abort-outside-window); fi
if [[ $isolate_conversion == 1 ]]; then worker_args+=(--isolate-conversion); fi

engine_args=(run --rm --name blobforge-worker --env-file "$config_dir/worker.env" -v "$cache_dir:/var/cache/blobforge")
if [[ $gpu == 1 && $engine_name == docker ]]; then engine_args+=(--gpus=all); fi
if [[ $gpu == 1 && $engine_name == podman ]]; then engine_args+=(--device=nvidia.com/gpu=all); fi
engine_args+=("$image" "${worker_args[@]}")

quote_args() {
  local first=1 value
  for value in "$@"; do
    if [[ $first == 0 ]]; then printf ' '; fi
    printf '%q' "$value"
    first=0
  done
}

exec_start=$(quote_args "$engine" "${engine_args[@]}")
exec_stop=$(quote_args "$engine" stop --time 120 blobforge-worker)
exec_remove=$(quote_args "$engine" rm --force blobforge-worker)

cat > "$unit_dir/blobforge-worker.service" <<EOF
[Unit]
Description=BlobForge PDF conversion worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=-$exec_remove
ExecStart=$exec_start
ExecStop=-$exec_stop
Restart=on-failure
RestartSec=15
TimeoutStopSec=150

[Install]
WantedBy=default.target
EOF

"$engine" pull "$image"
systemctl --user daemon-reload
systemctl --user enable --now blobforge-worker.service

printf 'BlobForge worker installed with %s.\n' "$engine_name"
printf 'Status: systemctl --user status blobforge-worker\n'
printf 'Logs:   journalctl --user -u blobforge-worker -f\n'
printf 'Config: %s\n' "$config_dir/worker.env"

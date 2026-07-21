# Linux Worker Setup

BlobForge publishes CPU and CUDA worker containers to
`ghcr.io/tionis/blobforge`. A Linux
worker therefore does not need a repository clone, Python environment, S3
credentials, or a local source checkout. It needs:

- Podman or Docker.
- A systemd user session for the managed-service installer.
- One worker enrollment token created in the BlobForge management console.
- Outbound HTTPS access to the coordinator, object store, container registry,
  and model registry on the first conversion.

## Managed user service

Create a worker in the management console. The one-time credential panel shows
the installation command. The equivalent manual flow is:

```bash
curl -fsSLO https://raw.githubusercontent.com/tionis/BlobForge/main/scripts/install-linux-worker.sh
chmod +x install-linux-worker.sh
./install-linux-worker.sh \
  --coordinator-url https://blobforge.example \
  --token bfw_REPLACE_ME
```

The installer:

1. Selects Podman first, then Docker.
2. Pulls the multi-architecture CPU image at
   `ghcr.io/tionis/blobforge:latest`.
3. Stores the coordinator URL and token in
   `~/.config/blobforge/worker.env` with mode `0600`.
4. Persists downloaded Marker/Hugging Face/Torch data under
   `~/.cache/blobforge`.
5. Installs and starts `blobforge-worker.service` in the systemd user manager.

The worker token is bound to this enrollment. Do not reuse the same token on a
second computer.

### Scheduling and isolation

Pass worker runtime options through the installer:

```bash
./install-linux-worker.sh \
  --coordinator-url https://blobforge.example \
  --token bfw_REPLACE_ME \
  --run-window 22:00-06:00 \
  --abort-outside-window
```

`--abort-outside-window` automatically enables isolated conversion. Use
`--isolate-conversion` separately when native conversion crash containment is
wanted without a run window.

For an NVIDIA host whose container runtime is already configured, add `--gpu`.
The installer selects the amd64-only `ghcr.io/tionis/blobforge:latest-cuda`
image. Docker receives `--gpus=all`; Podman receives
`--device=nvidia.com/gpu=all`. GPU runtime installation remains a host concern.
Use `--image` only to override the automatically selected CPU or CUDA image.

### Service operations

```bash
systemctl --user status blobforge-worker
journalctl --user -u blobforge-worker -f
systemctl --user restart blobforge-worker
systemctl --user stop blobforge-worker
```

User services normally stop when the user has no login session. To keep a
dedicated worker account running across logout and reboot, an administrator can
enable lingering once:

```bash
sudo loginctl enable-linger "$USER"
```

To upgrade, rerun the installer or pull the image and restart the service:

```bash
podman pull ghcr.io/tionis/blobforge:latest
systemctl --user restart blobforge-worker
```

Use `docker pull` instead on Docker hosts. CUDA workers should pull
`ghcr.io/tionis/blobforge:latest-cuda`. Pin `--image` to a SHA tag when a worker
should not follow a moving tag automatically.

## Manual container run

On Linux systems without a systemd user manager, create a private environment
file and run the same image under the host's preferred supervisor:

```bash
install -d -m 700 ~/.config/blobforge ~/.cache/blobforge
printf '%s\n' \
  'BLOBFORGE_COORDINATOR_URL=https://blobforge.example' \
  'BLOBFORGE_COORDINATOR_TOKEN=bfw_REPLACE_ME' \
  > ~/.config/blobforge/worker.env
chmod 600 ~/.config/blobforge/worker.env

podman run --rm --name blobforge-worker \
  --env-file ~/.config/blobforge/worker.env \
  -v ~/.cache/blobforge:/var/cache/blobforge \
  ghcr.io/tionis/blobforge:latest worker
```

## Native uv installation

Containers are the supported distribution boundary because Marker, Torch, OCR,
and native libraries vary by Linux distribution. A native install is still
available when the host environment is intentionally managed:

```bash
uv tool install "blobforge[convert,metrics] @ git+https://github.com/tionis/BlobForge.git"
blobforge worker --coordinator-url https://blobforge.example --token bfw_REPLACE_ME
```

Install Ghostscript, Tesseract OCR, OpenGL/GLib, and the desired CUDA stack with
the distribution's package manager first. The container path avoids that host
dependency setup.

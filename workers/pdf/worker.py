"""
BlobForge PDF Worker

A simple worker that claims PDF conversion jobs from the BlobForge server,
downloads source PDFs, converts them to markdown using marker, and uploads
the results.
"""

import hashlib
import logging
import os
import platform
import shutil
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Config:
    """Worker configuration from environment variables."""

    def __init__(self):
        self.server_url = os.environ.get("BLOBFORGE_SERVER_URL", "http://localhost:8080")
        self.worker_type = "pdf-convert"
        self.poll_interval = int(os.environ.get("BLOBFORGE_POLL_INTERVAL", "5"))
        self.heartbeat_interval = int(os.environ.get("BLOBFORGE_HEARTBEAT_INTERVAL", "30"))
        self.max_batch_pages = int(os.environ.get("BLOBFORGE_MAX_BATCH_PAGES", "4"))
        self.worker_id = self._generate_worker_id()

    def _generate_worker_id(self) -> str:
        """Generate a stable worker ID based on machine fingerprint."""
        # Try to read machine ID
        machine_id = ""
        for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"]:
            try:
                with open(path) as f:
                    machine_id = f.read().strip()
                    break
            except FileNotFoundError:
                continue

        # Combine with hostname
        hostname = platform.node()
        fingerprint = f"{hostname}:{machine_id}"
        return hashlib.sha256(fingerprint.encode()).hexdigest()[:16]


class BlobForgeClient:
    """HTTP client for interacting with BlobForge server."""

    def __init__(self, config: Config):
        self.config = config
        self.client = httpx.Client(base_url=config.server_url, timeout=30)

    def register(self) -> bool:
        """Register this worker with the server."""
        try:
            resp = self.client.post(
                "/api/workers/register",
                json={
                    "id": self.config.worker_id,
                    "type": self.config.worker_type,
                    "metadata": {
                        "hostname": platform.node(),
                        "platform": platform.platform(),
                    },
                },
            )
            resp.raise_for_status()
            logger.info(f"Registered worker {self.config.worker_id}")
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to register: {e}")
            return False

    def unregister(self):
        """Unregister this worker from the server."""
        try:
            resp = self.client.post(f"/api/workers/{self.config.worker_id}/unregister")
            resp.raise_for_status()
            logger.info("Unregistered worker")
        except httpx.HTTPError as e:
            logger.error(f"Failed to unregister: {e}")

    def heartbeat(self, job_id: Optional[int] = None):
        """Send heartbeat to server."""
        try:
            resp = self.client.post(
                f"/api/workers/{self.config.worker_id}/heartbeat",
                json={"job_id": job_id},
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"Heartbeat failed: {e}")

    def claim_job(self) -> Optional[dict]:
        """Try to claim a job from the server."""
        try:
            resp = self.client.post(
                "/api/jobs/claim",
                json={
                    "worker_id": self.config.worker_id,
                    "job_type": self.config.worker_type,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data if data.get("job") else None
        except httpx.HTTPError as e:
            logger.error(f"Failed to claim job: {e}")
            return None

    def complete_job(self, job_id: int, output_key: str, result: dict):
        """Mark a job as complete."""
        try:
            resp = self.client.post(
                f"/api/jobs/{job_id}/complete",
                json={
                    "worker_id": self.config.worker_id,
                    "output_key": output_key,
                    "result": result,
                },
            )
            resp.raise_for_status()
            logger.info(f"Completed job {job_id}")
        except httpx.HTTPError as e:
            logger.error(f"Failed to complete job {job_id}: {e}")
            raise

    def fail_job(self, job_id: int, error: str):
        """Mark a job as failed."""
        try:
            resp = self.client.post(
                f"/api/jobs/{job_id}/fail",
                json={
                    "worker_id": self.config.worker_id,
                    "error": error,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("will_retry"):
                logger.info(f"Job {job_id} failed, will retry")
            else:
                logger.warning(f"Job {job_id} failed permanently (dead)")
        except httpx.HTTPError as e:
            logger.error(f"Failed to report job failure for {job_id}: {e}")

    def close(self):
        """Close the HTTP client."""
        self.client.close()


def download_file(url: str, dest: Path) -> bool:
    """Download a file from a presigned URL."""
    try:
        with httpx.stream("GET", url, timeout=300) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False


def upload_file(url: str, source: Path) -> bool:
    """Upload a file to a presigned URL."""
    try:
        with open(source, "rb") as f:
            # Use httpx directly without client to avoid base_url
            resp = httpx.put(
                url,
                content=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300,
            )
            resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False


def convert_pdf(input_path: Path, output_dir: Path, max_pages: int = 4) -> Optional[Path]:
    """Convert a PDF to markdown using marker."""
    try:
        from marker.converters.pdf import PdfConverter
        from marker.models import create_model_dict
        from marker.config.parser import ConfigParser
        from marker.output import save_output

        # Initialize converter
        config_parser = ConfigParser({"output_format": "markdown"})
        converter = PdfConverter(
            config=config_parser.generate_config_dict(),
            artifact_dict=create_model_dict(),
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer(),
        )

        # Convert
        rendered = converter(str(input_path))

        # Save output
        output_path = output_dir / "output.md"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rendered.markdown)

        return output_path

    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return None


def process_job(client: BlobForgeClient, job_data: dict) -> bool:
    """Process a single job."""
    job = job_data["job"]
    job_id = job["id"]
    source_url = job_data["source_url"]
    upload_url = job_data["upload_url"]
    output_key = job_data["output_key"]

    logger.info(f"Processing job {job_id}: {job.get('source_path', 'unknown')}")

    # Create temp directory for this job
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        input_path = tmpdir / "input.pdf"
        output_dir = tmpdir / "output"
        output_dir.mkdir()

        # Download source PDF
        logger.info(f"Downloading source for job {job_id}")
        if not download_file(source_url, input_path):
            client.fail_job(job_id, "Failed to download source file")
            return False

        # Convert PDF
        logger.info(f"Converting PDF for job {job_id}")
        output_path = convert_pdf(input_path, output_dir)
        if not output_path:
            client.fail_job(job_id, "PDF conversion failed")
            return False

        # Upload result
        logger.info(f"Uploading result for job {job_id}")
        if not upload_file(upload_url, output_path):
            client.fail_job(job_id, "Failed to upload result")
            return False

        # Mark complete
        result = {
            "output_size": output_path.stat().st_size,
            "input_size": input_path.stat().st_size,
        }
        client.complete_job(job_id, output_key, result)
        return True


def main():
    """Main worker loop."""
    config = Config()
    client = BlobForgeClient(config)

    # Track shutdown
    shutdown = False

    def signal_handler(signum, frame):
        nonlocal shutdown
        logger.info("Received shutdown signal")
        shutdown = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Register worker
    if not client.register():
        logger.error("Failed to register worker, exiting")
        sys.exit(1)

    last_heartbeat = time.time()
    current_job_id = None

    try:
        while not shutdown:
            # Send heartbeat if needed
            if time.time() - last_heartbeat >= config.heartbeat_interval:
                client.heartbeat(current_job_id)
                last_heartbeat = time.time()

            # Try to claim a job
            job_data = client.claim_job()

            if job_data:
                current_job_id = job_data["job"]["id"]
                try:
                    process_job(client, job_data)
                except Exception as e:
                    logger.exception(f"Error processing job {current_job_id}")
                    client.fail_job(current_job_id, str(e))
                finally:
                    current_job_id = None
            else:
                # No jobs available, wait before polling again
                time.sleep(config.poll_interval)

    finally:
        client.unregister()
        client.close()


if __name__ == "__main__":
    main()

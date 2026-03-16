"""
CINEOS Media Runtime — Render Manager

Creates and tracks render jobs through their lifecycle:
    queued -> rendering -> completed | failed | cancelled

Implements the RenderOrchestrationContract TypeScript interfaces.
Thread-safe, in-memory, pure stdlib.
"""

from __future__ import annotations

import hashlib
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Render job data
# ---------------------------------------------------------------------------

class RenderJob:
    """In-memory representation of a render job matching the TS RenderJob interface."""

    def __init__(
        self,
        request: dict[str, Any],
    ) -> None:
        self.job_id: str = str(uuid.uuid4())
        self.request: dict[str, Any] = request
        self.status: str = "queued"
        self.progress_percent: int = 0
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.error_message: Optional[str] = None
        self.output_ref: Optional[dict[str, Any]] = None
        self.retry_count: int = 0
        self.max_retries: int = 3
        self.created_at: str = datetime.now(timezone.utc).isoformat()

    def start(self) -> None:
        """Transition to rendering state."""
        self.status = "rendering"
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.progress_percent = 0

    def complete(self, output_ref: dict[str, Any]) -> None:
        """Transition to completed state with output reference."""
        self.status = "completed"
        self.progress_percent = 100
        self.completed_at = datetime.now(timezone.utc).isoformat()
        self.output_ref = output_ref

    def fail(self, error_message: str) -> None:
        """Transition to failed state."""
        self.status = "failed"
        self.completed_at = datetime.now(timezone.utc).isoformat()
        self.error_message = error_message
        self.retry_count += 1

    def cancel(self) -> None:
        """Transition to cancelled state."""
        self.status = "cancelled"
        self.completed_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to match the TS RenderJob interface."""
        return {
            "job_id": self.job_id,
            "request": self.request,
            "status": self.status,
            "progress_percent": self.progress_percent,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
            "output_ref": self.output_ref,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }

    def to_failure_dict(self) -> dict[str, Any]:
        """Serialize to match the TS RenderFailure interface."""
        return {
            "job_id": self.job_id,
            "error_code": "RENDER_FAILED",
            "error_message": self.error_message or "Unknown error",
            "failed_at": self.completed_at or datetime.now(timezone.utc).isoformat(),
            "recoverable": self.retry_count < self.max_retries,
        }


# ---------------------------------------------------------------------------
# Render Manager
# ---------------------------------------------------------------------------

class RenderManager:
    """
    Manages the full render job lifecycle.

    Since no FFmpeg is available, jobs are created as queued and then
    immediately simulated through rendering -> completed with
    deterministic output hashes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, RenderJob] = {}
        self._total_completed: int = 0
        self._total_failed: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, request_body: dict[str, Any]) -> dict[str, Any]:
        """
        Submit a render request.  Creates a RenderJob, simulates processing,
        and returns the completed job dict matching the TS contract.

        Expected request_body keys (from RenderRequest TS interface):
            source_content_hash, source_filename, output_format,
            render_profile, parameters, requested_by
        """
        now = datetime.now(timezone.utc).isoformat()
        request_id = str(uuid.uuid4())

        # Build the RenderRequest record
        render_request: dict[str, Any] = {
            "request_id": request_id,
            "source_content_hash": request_body.get("source_content_hash", ""),
            "source_filename": request_body.get("source_filename", "unknown"),
            "output_format": request_body.get("output_format", "mp4"),
            "render_profile": request_body.get("render_profile", "default"),
            "parameters": request_body.get("parameters", {}),
            "requested_at": now,
            "requested_by": request_body.get("requested_by", "cineos-api"),
        }

        job = RenderJob(request=render_request)

        with self._lock:
            self._jobs[job.job_id] = job

        # Simulate immediate processing (no real FFmpeg)
        self._simulate_render(job)

        return job.to_dict()

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        """Retrieve a render job by ID."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return job.to_dict()

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a render job. Returns True if cancelled, False if not found or already terminal."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status in ("completed", "failed", "cancelled"):
                return False
            job.cancel()
            return True

    def get_queue_status(self) -> dict[str, Any]:
        """Return the RenderQueueStatus matching the TS contract."""
        with self._lock:
            all_jobs = list(self._jobs.values())

            queued = [j for j in all_jobs if j.status == "queued"]
            active = [j for j in all_jobs if j.status == "rendering"]
            completed = [
                j for j in all_jobs if j.status == "completed"
            ]
            failed = [j for j in all_jobs if j.status == "failed"]

            # Recent = last 20
            completed_recent = sorted(
                completed,
                key=lambda j: j.completed_at or "",
                reverse=True,
            )[:20]
            failed_recent = sorted(
                failed,
                key=lambda j: j.completed_at or "",
                reverse=True,
            )[:20]

            return {
                "queued_count": len(queued),
                "active_jobs": [j.to_dict() for j in active],
                "completed_recent": [j.to_dict() for j in completed_recent],
                "failed_recent": [j.to_failure_dict() for j in failed_recent],
                "total_completed": self._total_completed,
                "total_failed": self._total_failed,
            }

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    def _simulate_render(self, job: RenderJob) -> None:
        """
        Simulate a render by transitioning queued -> rendering -> completed.
        Produces a deterministic output hash from the source hash + profile.
        """
        job.start()

        source_hash = job.request.get("source_content_hash", "")
        output_format = job.request.get("output_format", "mp4")
        profile = job.request.get("render_profile", "default")
        source_filename = job.request.get("source_filename", "unknown")

        # Deterministic output hash: SHA-256 of source_hash + profile + format
        seed = f"{source_hash}:{profile}:{output_format}:{job.job_id}"
        output_hash = hashlib.sha256(seed.encode("utf-8")).hexdigest()

        # Simulated output size (deterministic from hash)
        output_size = int(output_hash[:8], 16) % (500 * 1024 * 1024) + 1024

        # Derive output path
        base_name = source_filename.rsplit(".", 1)[0] if "." in source_filename else source_filename
        output_path = f"/renders/{base_name}_{profile}.{output_format}"

        output_ref: dict[str, Any] = {
            "output_content_hash": output_hash,
            "output_path": output_path,
            "output_format": output_format,
            "output_size_bytes": output_size,
            "derived_from_hash": source_hash,
        }

        job.complete(output_ref)

        with self._lock:
            self._total_completed += 1

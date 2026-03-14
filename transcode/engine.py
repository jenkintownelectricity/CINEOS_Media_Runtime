"""
CINEOS Media Runtime — Transcode Engine

FFmpeg-based transcode engine with deterministic output naming,
retry-safe (idempotent) execution, tenant scoping, and full provenance.
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

from transcode.events import TranscodeEventEmitter
from transcode.models import (
    DerivativeResult,
    DerivativeSpec,
    MediaType,
    TranscodeJob,
    TranscodeStatus,
)
from transcode.profiles import TranscodeProfile, get_profile
from transcode.provenance import ProvenanceCapture, ProvenanceRecord

logger = logging.getLogger(__name__)


def _file_hash(path: str, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
    """Compute the content hash of a file."""
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def _ensure_dir(path: str) -> None:
    """Create directory tree if it does not exist."""
    os.makedirs(path, exist_ok=True)


class TranscodeEngine:
    """
    Orchestrates FFmpeg-based media transcoding.

    Key invariants:
      - Output filenames are deterministic: {source_hash}_{profile_id}.{ext}
      - If the output already exists, the transcode is skipped (idempotent).
      - Every successful transcode produces a ProvenanceRecord.
      - All operations are tenant-scoped via output_base_dir/{tenant_id}/.
    """

    def __init__(
        self,
        output_base_dir: str,
        ffmpeg_path: str = "ffmpeg",
        ffprobe_path: str = "ffprobe",
        event_emitter: Optional[TranscodeEventEmitter] = None,
    ) -> None:
        self.output_base_dir = output_base_dir
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        self.events = event_emitter or TranscodeEventEmitter()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transcode(
        self,
        source_path: str,
        source_hash: str,
        profile: TranscodeProfile | str,
        tenant_id: str,
    ) -> DerivativeResult:
        """
        Generate a single derivative from a source file.

        If the output file already exists, returns a SKIPPED result
        (idempotent / retry-safe).
        """
        if isinstance(profile, str):
            profile = get_profile(profile)

        output_dir = self._tenant_dir(tenant_id, profile.category.value)
        _ensure_dir(output_dir)
        output_filename = f"{source_hash}_{profile.transform_id}.{profile.output_format}"
        output_path = os.path.join(output_dir, output_filename)

        spec = DerivativeSpec(
            source_hash=source_hash,
            transform_id=profile.transform_id,
            media_type=(
                MediaType.AUDIO
                if profile.category.value.startswith("audio")
                else MediaType.VIDEO
            ),
            output_format=profile.output_format,
            params=dict(profile.params),
            tenant_id=tenant_id,
        )

        # Idempotency: skip if output exists
        if os.path.isfile(output_path):
            logger.info("Derivative already exists, skipping: %s", output_path)
            return DerivativeResult(
                spec=spec,
                status=TranscodeStatus.SKIPPED,
                output_path=output_path,
                output_size_bytes=os.path.getsize(output_path),
                output_hash=_file_hash(output_path),
            )

        # Emit started event
        self.events.transcode_started(
            tenant_id=tenant_id,
            job_id="",
            source_hash=source_hash,
            source_path=source_path,
            profile_id=profile.profile_id,
        )

        # Build FFmpeg command
        cmd = self._build_ffmpeg_cmd(source_path, output_path, profile)
        logger.info("Running: %s", " ".join(cmd))

        t0 = time.monotonic()
        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            duration = time.monotonic() - t0
            error_msg = exc.stderr or str(exc)
            logger.error("Transcode failed: %s", error_msg)
            self.events.transcode_failed(
                tenant_id=tenant_id,
                job_id="",
                source_hash=source_hash,
                profile_id=profile.profile_id,
                error=error_msg,
            )
            return DerivativeResult(
                spec=spec,
                status=TranscodeStatus.FAILED,
                duration_seconds=duration,
                error_message=error_msg,
            )
        except FileNotFoundError:
            error_msg = f"ffmpeg not found at '{self.ffmpeg_path}'"
            logger.error(error_msg)
            self.events.transcode_failed(
                tenant_id=tenant_id,
                job_id="",
                source_hash=source_hash,
                profile_id=profile.profile_id,
                error=error_msg,
            )
            return DerivativeResult(
                spec=spec,
                status=TranscodeStatus.FAILED,
                error_message=error_msg,
            )

        duration = time.monotonic() - t0
        output_hash = _file_hash(output_path)
        output_size = os.path.getsize(output_path)

        # Emit completed event
        self.events.transcode_completed(
            tenant_id=tenant_id,
            job_id="",
            source_hash=source_hash,
            profile_id=profile.profile_id,
            output_path=output_path,
            output_hash=output_hash,
            duration_seconds=duration,
            output_size_bytes=output_size,
        )

        return DerivativeResult(
            spec=spec,
            status=TranscodeStatus.COMPLETED,
            output_path=output_path,
            output_size_bytes=output_size,
            output_hash=output_hash,
            duration_seconds=duration,
        )

    def run_job(self, job: TranscodeJob) -> TranscodeJob:
        """
        Execute all derivatives in a TranscodeJob.
        Populates results and updates job status.
        """
        job.mark_started()

        all_ok = True
        for spec in job.derivatives:
            profile = get_profile(spec.transform_id)
            result = self.transcode(
                source_path=job.source_path,
                source_hash=job.source_hash,
                profile=profile,
                tenant_id=job.tenant_id,
            )
            job.add_result(result)
            if not result.succeeded and result.status != TranscodeStatus.SKIPPED:
                all_ok = False

        if all_ok:
            job.mark_completed()
        else:
            job.mark_failed()

        return job

    def build_provenance(
        self,
        source_path: str,
        source_hash: str,
        profile: TranscodeProfile | str,
        result: DerivativeResult,
        tenant_id: str,
    ) -> ProvenanceRecord:
        """Build a W3C PROV provenance record for a completed derivative."""
        if isinstance(profile, str):
            profile = get_profile(profile)

        capture = ProvenanceCapture(tenant_id=tenant_id)
        capture.set_source(content_hash=source_hash, path=source_path)
        capture.start_activity(profile_id=profile.profile_id, params=dict(profile.params))
        capture.finish_activity()
        capture.set_output(
            output_hash=result.output_hash or "",
            output_path=result.output_path or "",
            transform_id=profile.transform_id,
        )
        return capture.build()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _tenant_dir(self, tenant_id: str, sub: str = "") -> str:
        parts = [self.output_base_dir, tenant_id]
        if sub:
            parts.append(sub)
        return os.path.join(*parts)

    def _build_ffmpeg_cmd(
        self,
        source_path: str,
        output_path: str,
        profile: TranscodeProfile,
    ) -> list[str]:
        cmd = [
            self.ffmpeg_path,
            "-y",          # overwrite (safe — we already checked idempotency)
            "-i", source_path,
        ]
        cmd.extend(profile.to_ffmpeg_args())
        cmd.append(output_path)
        return cmd

    def probe(self, path: str) -> dict[str, Any]:
        """Run ffprobe and return parsed JSON output."""
        cmd = [
            self.ffprobe_path,
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        import json
        return json.loads(result.stdout)

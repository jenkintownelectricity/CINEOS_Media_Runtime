"""
CINEOS Media Runtime -- Proxy / Thumbnail / Waveform Generator

Generates visual and audio derivatives from source media files using
FFmpeg.  All output filenames are deterministic: they are derived from
the SHA-256 hash of the source content, guaranteeing idempotent
regeneration and content-addressed storage.

Classes:
    ProxyGenerator  -- high-level API for derivative generation.
    DerivativeMeta  -- metadata record returned for each generated file.
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

# Default thumbnail widths when the caller does not specify sizes.
DEFAULT_THUMBNAIL_SIZES: list[int] = [160, 320, 640]

# Contact-sheet defaults.
DEFAULT_CONTACT_COLUMNS: int = 4
DEFAULT_CONTACT_ROWS: int = 4
DEFAULT_TILE_WIDTH: int = 320

# Waveform defaults.
DEFAULT_WAVEFORM_WIDTH: int = 1920
DEFAULT_WAVEFORM_HEIGHT: int = 200
DEFAULT_WAVEFORM_COLOR: str = "0x00ff00"
DEFAULT_WAVEFORM_BG: str = "0x000000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _file_hash(path: str, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
    """Compute the hex digest of a file."""
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def _source_hash(source_path: str) -> str:
    """Return the SHA-256 hex digest of *source_path*."""
    return _file_hash(source_path)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# DerivativeMeta -- returned for every generated file
# ---------------------------------------------------------------------------

@dataclass
class DerivativeMeta:
    """Metadata about a single generated derivative file."""

    source_hash: str
    derivative_type: str
    output_path: str
    output_hash: str
    width: Optional[int] = None
    height: Optional[int] = None
    size_bytes: int = 0
    duration_seconds: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ProxyGenerator
# ---------------------------------------------------------------------------

class ProxyGenerator:
    """
    Generates proxy, thumbnail, contact-sheet, and waveform derivatives
    from source media files.

    Key invariants
    --------------
    * Output filenames are deterministic:
        {source_hash}_{derivative_label}.{ext}
    * If the output already exists on disk the generation step is skipped
      (idempotent).
    * Every public method returns a list of ``DerivativeMeta`` records
      describing the files that were produced (or already existed).

    Parameters
    ----------
    output_dir
        Root directory where derivatives are written.
    ffmpeg_path
        Path to the ``ffmpeg`` binary.
    ffprobe_path
        Path to the ``ffprobe`` binary.
    """

    def __init__(
        self,
        output_dir: str,
        ffmpeg_path: str = "ffmpeg",
        ffprobe_path: str = "ffprobe",
    ) -> None:
        self.output_dir = output_dir
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_thumbnail(
        self,
        source: str,
        sizes: Sequence[int] | None = None,
        *,
        timestamp: str = "00:00:01",
        quality: int = 85,
    ) -> list[DerivativeMeta]:
        """
        Extract a single frame from *source* and scale it to each
        requested width.

        Parameters
        ----------
        source
            Path to the source media file.
        sizes
            Iterable of target widths in pixels.  Heights are computed
            automatically to preserve the aspect ratio.
        timestamp
            Seek position for the extracted frame (HH:MM:SS or seconds).
        quality
            JPEG quality (2-31 for FFmpeg ``-q:v``; mapped to 2-5 range
            internally, with 2 = best).

        Returns
        -------
        list[DerivativeMeta]
        """
        if sizes is None:
            sizes = DEFAULT_THUMBNAIL_SIZES

        src_hash = _source_hash(source)
        thumb_dir = os.path.join(self.output_dir, "thumbnails")
        _ensure_dir(thumb_dir)

        results: list[DerivativeMeta] = []
        for width in sizes:
            label = f"thumb_{width}"
            out_name = f"{src_hash}_{label}.jpg"
            out_path = os.path.join(thumb_dir, out_name)

            if os.path.isfile(out_path):
                logger.info("Thumbnail already exists, skipping: %s", out_path)
                results.append(self._meta_for_existing(src_hash, "thumbnail", out_path, width=width))
                continue

            # FFmpeg: extract frame, scale, write JPEG
            q_val = max(2, min(5, 7 - (quality // 20)))  # rough mapping
            cmd = [
                self.ffmpeg_path,
                "-y",
                "-ss", str(timestamp),
                "-i", source,
                "-frames:v", "1",
                "-vf", f"scale={width}:-1",
                "-q:v", str(q_val),
                out_path,
            ]

            meta = self._run_and_collect(
                cmd=cmd,
                source_hash=src_hash,
                derivative_type="thumbnail",
                output_path=out_path,
                width=width,
            )
            results.append(meta)

        return results

    def generate_contact_sheet(
        self,
        source: str,
        *,
        columns: int = DEFAULT_CONTACT_COLUMNS,
        rows: int = DEFAULT_CONTACT_ROWS,
        tile_width: int = DEFAULT_TILE_WIDTH,
        quality: int = 85,
    ) -> list[DerivativeMeta]:
        """
        Generate a contact-sheet image (grid of evenly-spaced frames)
        from a video source.

        Returns a single-element list containing the ``DerivativeMeta``
        for the contact sheet.
        """
        src_hash = _source_hash(source)
        cs_dir = os.path.join(self.output_dir, "contact_sheets")
        _ensure_dir(cs_dir)

        label = "contact_sheet"
        out_name = f"{src_hash}_{label}.jpg"
        out_path = os.path.join(cs_dir, out_name)

        if os.path.isfile(out_path):
            logger.info("Contact sheet already exists, skipping: %s", out_path)
            return [self._meta_for_existing(src_hash, label, out_path)]

        total_frames = columns * rows
        duration = self._probe_duration(source)
        if duration <= 0:
            duration = 10.0  # fallback for very short / unknown-duration files

        interval = duration / (total_frames + 1)
        tile_height = -1  # auto

        # Build a complex filtergraph:
        #   select every N-th second, scale tiles, tile into a grid
        select_expr = "+".join(
            [f"gt(t,{interval * (i + 1):.3f})" for i in range(total_frames)]
        )
        vf = (
            f"select='{select_expr}',"
            f"scale={tile_width}:{tile_height},"
            f"tile={columns}x{rows}"
        )

        q_val = max(2, min(5, 7 - (quality // 20)))
        cmd = [
            self.ffmpeg_path,
            "-y",
            "-i", source,
            "-frames:v", "1",
            "-vf", vf,
            "-q:v", str(q_val),
            out_path,
        ]

        meta = self._run_and_collect(
            cmd=cmd,
            source_hash=src_hash,
            derivative_type="contact_sheet",
            output_path=out_path,
            extra={"columns": columns, "rows": rows, "tile_width": tile_width},
        )
        return [meta]

    def generate_waveform(
        self,
        source: str,
        *,
        width: int = DEFAULT_WAVEFORM_WIDTH,
        height: int = DEFAULT_WAVEFORM_HEIGHT,
        color: str = DEFAULT_WAVEFORM_COLOR,
        background: str = DEFAULT_WAVEFORM_BG,
    ) -> list[DerivativeMeta]:
        """
        Render an audio waveform visualization as a PNG image.

        Returns a single-element list containing the ``DerivativeMeta``
        for the waveform image.
        """
        src_hash = _source_hash(source)
        wf_dir = os.path.join(self.output_dir, "waveforms")
        _ensure_dir(wf_dir)

        label = "waveform"
        out_name = f"{src_hash}_{label}.png"
        out_path = os.path.join(wf_dir, out_name)

        if os.path.isfile(out_path):
            logger.info("Waveform already exists, skipping: %s", out_path)
            return [self._meta_for_existing(src_hash, label, out_path, width=width, height=height)]

        filter_complex = (
            f"aformat=channel_layouts=mono,"
            f"showwavespic=s={width}x{height}:colors={color}"
        )
        cmd = [
            self.ffmpeg_path,
            "-y",
            "-i", source,
            "-filter_complex", filter_complex,
            "-frames:v", "1",
            out_path,
        ]

        meta = self._run_and_collect(
            cmd=cmd,
            source_hash=src_hash,
            derivative_type="waveform",
            output_path=out_path,
            width=width,
            height=height,
        )
        return [meta]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _probe_duration(self, path: str) -> float:
        """Return duration in seconds via ffprobe, or 0.0 on failure."""
        try:
            result = subprocess.run(
                [
                    self.ffprobe_path,
                    "-v", "quiet",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    path,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return float(result.stdout.strip())
        except Exception:
            logger.warning("Could not probe duration for %s", path)
            return 0.0

    def _run_and_collect(
        self,
        cmd: list[str],
        source_hash: str,
        derivative_type: str,
        output_path: str,
        width: Optional[int] = None,
        height: Optional[int] = None,
        extra: dict[str, Any] | None = None,
    ) -> DerivativeMeta:
        """Run an FFmpeg command and return a ``DerivativeMeta`` record."""
        logger.info("Running: %s", " ".join(cmd))
        t0 = time.monotonic()

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            duration = time.monotonic() - t0
            error_msg = str(exc)
            logger.error("Derivative generation failed (%s): %s", derivative_type, error_msg)
            return DerivativeMeta(
                source_hash=source_hash,
                derivative_type=derivative_type,
                output_path=output_path,
                output_hash="",
                width=width,
                height=height,
                duration_seconds=duration,
                extra={"error": error_msg, **(extra or {})},
            )

        duration = time.monotonic() - t0
        out_hash = _file_hash(output_path) if os.path.isfile(output_path) else ""
        size = os.path.getsize(output_path) if os.path.isfile(output_path) else 0

        return DerivativeMeta(
            source_hash=source_hash,
            derivative_type=derivative_type,
            output_path=output_path,
            output_hash=out_hash,
            width=width,
            height=height,
            size_bytes=size,
            duration_seconds=duration,
            extra=extra or {},
        )

    def _meta_for_existing(
        self,
        source_hash: str,
        derivative_type: str,
        output_path: str,
        width: Optional[int] = None,
        height: Optional[int] = None,
    ) -> DerivativeMeta:
        """Build a ``DerivativeMeta`` for an already-existing file."""
        return DerivativeMeta(
            source_hash=source_hash,
            derivative_type=derivative_type,
            output_path=output_path,
            output_hash=_file_hash(output_path),
            width=width,
            height=height,
            size_bytes=os.path.getsize(output_path),
        )

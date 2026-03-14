"""
CINEOS Media Runtime — Transcode Profiles

Deterministic transcode profiles that define output parameters for each
derivative type. Each profile produces a stable transform_id and parameter
set so that derivative naming is fully reproducible.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ProfileCategory(str, Enum):
    VIDEO_PROXY = "video_proxy"
    AUDIO_PROXY = "audio_proxy"
    THUMBNAIL = "thumbnail"
    CONTACT_SHEET = "contact_sheet"
    WAVEFORM = "waveform"


@dataclass(frozen=True)
class TranscodeProfile:
    """
    Immutable transcode profile. Frozen to guarantee determinism —
    once created, parameters cannot be mutated.
    """

    profile_id: str
    category: ProfileCategory
    output_format: str
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    @property
    def transform_id(self) -> str:
        """
        Deterministic transform identifier derived from profile_id.
        Used in derivative filenames: {source_hash}_{transform_id}.{ext}
        """
        return self.profile_id

    @property
    def param_fingerprint(self) -> str:
        """
        Hash of the full parameter set. If someone changes profile params
        but keeps the same profile_id, the fingerprint will differ —
        useful for cache invalidation.
        """
        canonical = json.dumps(
            {"profile_id": self.profile_id, "params": self.params},
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def to_ffmpeg_args(self) -> list[str]:
        """Convert profile parameters into FFmpeg CLI arguments."""
        args: list[str] = []

        # Video encoding
        if vcodec := self.params.get("video_codec"):
            args.extend(["-c:v", vcodec])
        if preset := self.params.get("preset"):
            args.extend(["-preset", preset])
        if crf := self.params.get("crf"):
            args.extend(["-crf", str(crf)])
        if video_bitrate := self.params.get("video_bitrate"):
            args.extend(["-b:v", video_bitrate])
        if width := self.params.get("width"):
            height = self.params.get("height", -2)
            args.extend(["-vf", f"scale={width}:{height}"])
        if fps := self.params.get("fps"):
            args.extend(["-r", str(fps)])
        if pix_fmt := self.params.get("pix_fmt"):
            args.extend(["-pix_fmt", pix_fmt])

        # Audio encoding
        if acodec := self.params.get("audio_codec"):
            args.extend(["-c:a", acodec])
        if audio_bitrate := self.params.get("audio_bitrate"):
            args.extend(["-b:a", audio_bitrate])
        if sample_rate := self.params.get("sample_rate"):
            args.extend(["-ar", str(sample_rate)])
        if channels := self.params.get("channels"):
            args.extend(["-ac", str(channels)])

        # Container flags
        if movflags := self.params.get("movflags"):
            args.extend(["-movflags", movflags])

        return args


# ---------------------------------------------------------------------------
# Built-in profiles
# ---------------------------------------------------------------------------

PROXY_360P = TranscodeProfile(
    profile_id="proxy_360p",
    category=ProfileCategory.VIDEO_PROXY,
    output_format="mp4",
    description="Low-resolution video proxy for offline/mobile review",
    params={
        "width": 640,
        "height": 360,
        "video_codec": "libx264",
        "preset": "fast",
        "crf": 28,
        "pix_fmt": "yuv420p",
        "audio_codec": "aac",
        "audio_bitrate": "96k",
        "sample_rate": 44100,
        "channels": 2,
        "movflags": "+faststart",
        "fps": 24,
    },
)

PROXY_720P = TranscodeProfile(
    profile_id="proxy_720p",
    category=ProfileCategory.VIDEO_PROXY,
    output_format="mp4",
    description="Standard video proxy for editorial review",
    params={
        "width": 1280,
        "height": 720,
        "video_codec": "libx264",
        "preset": "fast",
        "crf": 23,
        "pix_fmt": "yuv420p",
        "audio_codec": "aac",
        "audio_bitrate": "128k",
        "sample_rate": 48000,
        "channels": 2,
        "movflags": "+faststart",
        "fps": 24,
    },
)

PROXY_1080P = TranscodeProfile(
    profile_id="proxy_1080p",
    category=ProfileCategory.VIDEO_PROXY,
    output_format="mp4",
    description="High-resolution video proxy for QC and client review",
    params={
        "width": 1920,
        "height": 1080,
        "video_codec": "libx264",
        "preset": "medium",
        "crf": 20,
        "pix_fmt": "yuv420p",
        "audio_codec": "aac",
        "audio_bitrate": "192k",
        "sample_rate": 48000,
        "channels": 2,
        "movflags": "+faststart",
        "fps": 24,
    },
)

AUDIO_AAC = TranscodeProfile(
    profile_id="audio_aac",
    category=ProfileCategory.AUDIO_PROXY,
    output_format="m4a",
    description="AAC audio proxy for web playback",
    params={
        "audio_codec": "aac",
        "audio_bitrate": "192k",
        "sample_rate": 48000,
        "channels": 2,
    },
)

AUDIO_WAV = TranscodeProfile(
    profile_id="audio_wav",
    category=ProfileCategory.AUDIO_PROXY,
    output_format="wav",
    description="Uncompressed WAV audio for editorial",
    params={
        "audio_codec": "pcm_s16le",
        "sample_rate": 48000,
        "channels": 2,
    },
)

THUMBNAIL_SMALL = TranscodeProfile(
    profile_id="thumb_sm",
    category=ProfileCategory.THUMBNAIL,
    output_format="jpg",
    description="Small thumbnail (160px wide)",
    params={
        "width": 160,
        "height": -1,
        "quality": 80,
    },
)

THUMBNAIL_MEDIUM = TranscodeProfile(
    profile_id="thumb_md",
    category=ProfileCategory.THUMBNAIL,
    output_format="jpg",
    description="Medium thumbnail (320px wide)",
    params={
        "width": 320,
        "height": -1,
        "quality": 85,
    },
)

THUMBNAIL_LARGE = TranscodeProfile(
    profile_id="thumb_lg",
    category=ProfileCategory.THUMBNAIL,
    output_format="jpg",
    description="Large thumbnail (640px wide)",
    params={
        "width": 640,
        "height": -1,
        "quality": 90,
    },
)

CONTACT_SHEET = TranscodeProfile(
    profile_id="contact_sheet",
    category=ProfileCategory.CONTACT_SHEET,
    output_format="jpg",
    description="Contact sheet grid (4x4 frames)",
    params={
        "columns": 4,
        "rows": 4,
        "tile_width": 320,
        "quality": 85,
    },
)

WAVEFORM = TranscodeProfile(
    profile_id="waveform",
    category=ProfileCategory.WAVEFORM,
    output_format="png",
    description="Audio waveform visualization",
    params={
        "width": 1920,
        "height": 200,
        "color": "0x00ff00",
        "background": "0x000000",
    },
)

# ---------------------------------------------------------------------------
# Profile registry
# ---------------------------------------------------------------------------

_PROFILE_REGISTRY: dict[str, TranscodeProfile] = {
    p.profile_id: p
    for p in [
        PROXY_360P,
        PROXY_720P,
        PROXY_1080P,
        AUDIO_AAC,
        AUDIO_WAV,
        THUMBNAIL_SMALL,
        THUMBNAIL_MEDIUM,
        THUMBNAIL_LARGE,
        CONTACT_SHEET,
        WAVEFORM,
    ]
}


def get_profile(profile_id: str) -> TranscodeProfile:
    """Look up a profile by ID. Raises KeyError if unknown."""
    if profile_id not in _PROFILE_REGISTRY:
        raise KeyError(
            f"Unknown profile '{profile_id}'. "
            f"Available: {sorted(_PROFILE_REGISTRY.keys())}"
        )
    return _PROFILE_REGISTRY[profile_id]


def list_profiles(
    category: Optional[ProfileCategory] = None,
) -> list[TranscodeProfile]:
    """List all registered profiles, optionally filtered by category."""
    profiles = list(_PROFILE_REGISTRY.values())
    if category is not None:
        profiles = [p for p in profiles if p.category == category]
    return sorted(profiles, key=lambda p: p.profile_id)


def register_profile(profile: TranscodeProfile) -> None:
    """Register a custom profile at runtime."""
    _PROFILE_REGISTRY[profile.profile_id] = profile

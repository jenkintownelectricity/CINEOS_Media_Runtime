"""
CINEOS Media Runtime — Transcode Module

Media derivative generation (proxies, transcodes) for cinema workflows.
Deterministic naming, tenant-scoped, retry-safe execution with full provenance.
"""

from transcode.models import (
    DerivativeSpec,
    DerivativeResult,
    TranscodeJob,
    TranscodeStatus,
    MediaType,
)
from transcode.profiles import TranscodeProfile, get_profile, list_profiles
from transcode.engine import TranscodeEngine
from transcode.provenance import ProvenanceRecord, ProvenanceCapture
from transcode.events import TranscodeEventEmitter

__all__ = [
    "DerivativeSpec",
    "DerivativeResult",
    "TranscodeJob",
    "TranscodeStatus",
    "MediaType",
    "TranscodeProfile",
    "get_profile",
    "list_profiles",
    "TranscodeEngine",
    "ProvenanceRecord",
    "ProvenanceCapture",
    "TranscodeEventEmitter",
]

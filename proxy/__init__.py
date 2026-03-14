"""
CINEOS Media Runtime -- Proxy & Derivative Generation Package.

Generates visual and audio derivatives from source media:
  - Video proxy files (low-res review copies)
  - Thumbnails at multiple sizes
  - Contact sheets (grid of representative frames)
  - Audio waveform visualizations

All outputs use deterministic, content-addressed naming derived from
the source file hash, ensuring idempotent regeneration.
"""

from proxy.generator import (
    DerivativeMeta,
    ProxyGenerator,
)

__all__ = [
    "DerivativeMeta",
    "ProxyGenerator",
]

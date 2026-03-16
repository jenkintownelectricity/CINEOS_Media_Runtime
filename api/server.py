"""
CINEOS Media Runtime — HTTP API Server

stdlib-only HTTP server (http.server + json) that bridges the Python
media backend to the TypeScript UI layer.

Endpoints
---------
GET  /api/health                    Health check
POST /api/ingest                    Ingest a media file descriptor
GET  /api/ingest/queue              Ingest queue state
GET  /api/media                     List all registered media
GET  /api/media/<content_id>        Get single media record
GET  /api/media/<content_id>/lineage  Media lineage chain
POST /api/proxy/generate            Trigger proxy generation
GET  /api/cache                     Cache state
POST /api/render/create             Create a render job
GET  /api/render/<contract_id>      Get render status

Default port: 9400
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs

from api.media_cache import MediaCache
from api.render_manager import RenderManager


# ---------------------------------------------------------------------------
# In-memory media registry (content-addressed by SHA-256)
# ---------------------------------------------------------------------------

class MediaRegistry:
    """
    In-memory content-addressed media store.

    Each entry is keyed by its SHA-256 content hash.  Since no real files
    exist on disk, hashes are computed deterministically from
    filename + size + registration timestamp.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, dict[str, Any]] = {}
        # lineage links per content_hash
        self._lineage: dict[str, list[dict[str, Any]]] = {}

    def register(
        self,
        filename: str,
        file_size: int,
        mime_type: str,
    ) -> dict[str, Any]:
        """Register a new media asset and return the ContentIdentityRecord."""
        now = datetime.now(timezone.utc).isoformat()

        # SHA-256 content hash: hash(filename + size + timestamp)
        seed = f"{filename}:{file_size}:{now}"
        content_hash = hashlib.sha256(seed.encode("utf-8")).hexdigest()

        asset_id = f"asset-{content_hash[:16]}"
        media_id = content_hash  # runtime identity = content hash

        record: dict[str, Any] = {
            "content_hash": content_hash,
            "asset_id": asset_id,
            "media_id": media_id,
            "identity_strategy": "sha256-content-addressed",
            "filename": filename,
            "file_size": file_size,
            "mime_type": mime_type,
            "registered_at": now,
        }

        with self._lock:
            self._records[content_hash] = record
            self._lineage[content_hash] = []

        return record

    def get(self, content_hash: str) -> Optional[dict[str, Any]]:
        with self._lock:
            return self._records.get(content_hash)

    def list_all(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._records.values())

    def add_lineage_link(
        self,
        source_hash: str,
        derived_hash: str,
        link_type: str,
        transform_id: str,
        agent: str = "cineos-media-runtime",
    ) -> dict[str, Any]:
        """Add a lineage link from source to derived asset."""
        link: dict[str, Any] = {
            "link_id": str(uuid.uuid4()),
            "source_content_hash": source_hash,
            "derived_content_hash": derived_hash,
            "link_type": link_type,
            "transform_id": transform_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "agent": agent,
        }
        with self._lock:
            if source_hash not in self._lineage:
                self._lineage[source_hash] = []
            self._lineage[source_hash].append(link)
        return link

    def get_lineage(self, content_hash: str) -> Optional[dict[str, Any]]:
        """Return the LineageChain for an asset."""
        with self._lock:
            record = self._records.get(content_hash)
            if record is None:
                return None

            derivatives = list(self._lineage.get(content_hash, []))

        return {
            "root_content_hash": content_hash,
            "root_filename": record.get("filename", ""),
            "derivatives": derivatives,
            "total_derivatives": len(derivatives),
            "chain_complete": True,
            "missing_links": [],
        }


# ---------------------------------------------------------------------------
# Ingest queue (in-memory)
# ---------------------------------------------------------------------------

class IngestQueue:
    """Tracks ingest jobs and their status."""

    MAX_JOBS = 10_000

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: list[dict[str, Any]] = []

    def add_job(
        self,
        filename: str,
        content_hash: str,
        status: str = "completed",
    ) -> dict[str, Any]:
        job: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "filename": filename,
            "contentHash": content_hash,
            "status": status,
            "progress": 100 if status == "completed" else 0,
            "startedAt": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._jobs.append(job)
            # Prevent unbounded growth — trim oldest completed jobs
            if len(self._jobs) > self.MAX_JOBS:
                self._jobs = self._jobs[-self.MAX_JOBS:]
        return job

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            pending = [j for j in self._jobs if j["status"] == "pending"]
            active = [j for j in self._jobs if j["status"] == "active"]
            completed = [j for j in self._jobs if j["status"] == "completed"]
            failed = [j for j in self._jobs if j["status"] == "failed"]

            return {
                "pendingCount": len(pending),
                "activeJobs": active,
                "completedRecent": completed[-20:],
                "failedRecent": failed[-20:],
            }


# ---------------------------------------------------------------------------
# Route patterns
# ---------------------------------------------------------------------------

# Compiled patterns for URL routing
_ROUTES: list[tuple[str, re.Pattern[str], str]] = [
    ("GET",  re.compile(r"^/api/health$"),                          "health"),
    ("POST", re.compile(r"^/api/ingest$"),                          "ingest"),
    ("GET",  re.compile(r"^/api/ingest/queue$"),                    "ingest_queue"),
    ("GET",  re.compile(r"^/api/media$"),                           "media_list"),
    ("GET",  re.compile(r"^/api/media/([a-f0-9]{64})$"),            "media_get"),
    ("GET",  re.compile(r"^/api/media/([a-f0-9]{64})/lineage$"),    "media_lineage"),
    ("POST", re.compile(r"^/api/proxy/generate$"),                  "proxy_generate"),
    ("GET",  re.compile(r"^/api/cache$"),                           "cache_state"),
    ("POST", re.compile(r"^/api/render/create$"),                   "render_create"),
    ("GET",  re.compile(r"^/api/render/([a-f0-9-]{36})$"),          "render_get"),
]


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class CINEOSRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the CINEOS Media Runtime API."""

    # Class-level shared state (set by server factory)
    registry: MediaRegistry
    ingest_queue: IngestQueue
    media_cache: MediaCache
    render_manager: RenderManager

    # Suppress default stderr logging
    def log_message(self, format: str, *args: Any) -> None:
        pass  # silent; override for debug if needed

    # ------------------------------------------------------------------
    # CORS + JSON helpers
    # ------------------------------------------------------------------

    def _set_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def _send_json(self, status: int, body: Any) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (ValueError, TypeError):
            return {}
        if length <= 0:
            return {}
        # Cap request body at 10 MB to prevent memory exhaustion
        if length > 10 * 1024 * 1024:
            raise ValueError("Request body too large")
        raw = self.rfile.read(length)
        return json.loads(raw)

    def _send_not_found(self, message: str = "Not found") -> None:
        self._send_json(404, {"error": message})

    def _send_bad_request(self, message: str = "Bad request") -> None:
        self._send_json(400, {"error": message})

    # ------------------------------------------------------------------
    # OPTIONS (CORS preflight)
    # ------------------------------------------------------------------

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def _route(self, method: str) -> None:
        path = urlparse(self.path).path

        for route_method, pattern, handler_name in _ROUTES:
            if route_method != method:
                continue
            match = pattern.match(path)
            if match:
                handler = getattr(self, f"_handle_{handler_name}", None)
                if handler:
                    handler(*match.groups())
                    return

        self._send_not_found(f"No route for {method} {path}")

    def do_GET(self) -> None:
        try:
            self._route("GET")
        except Exception:
            self._send_json(500, {"error": "Internal server error"})

    def do_POST(self) -> None:
        try:
            self._route("POST")
        except Exception:
            self._send_json(500, {"error": "Internal server error"})

    # ------------------------------------------------------------------
    # Handler implementations
    # ------------------------------------------------------------------

    def _handle_health(self) -> None:
        self._send_json(200, {
            "status": "ok",
            "service": "cineos-media-runtime",
        })

    def _handle_ingest(self) -> None:
        try:
            body = self._read_json_body()
        except (json.JSONDecodeError, ValueError, OverflowError):
            self._send_bad_request("Invalid or oversized request body")
            return

        filename = body.get("filename")
        file_size = body.get("file_size")
        mime_type = body.get("mime_type")

        if not filename or file_size is None or not mime_type:
            self._send_bad_request(
                "Required fields: filename, file_size, mime_type"
            )
            return

        try:
            file_size_int = int(file_size)
        except (ValueError, TypeError):
            self._send_bad_request("file_size must be a valid integer")
            return

        if file_size_int < 0:
            self._send_bad_request("file_size must be non-negative")
            return

        # Register the media asset
        record = self.registry.register(
            filename=filename,
            file_size=file_size_int,
            mime_type=mime_type,
        )

        # Add to ingest queue as completed
        self.ingest_queue.add_job(
            filename=filename,
            content_hash=record["content_hash"],
            status="completed",
        )

        # Cache the asset
        self.media_cache.put(
            content_hash=record["content_hash"],
            has_original=True,
            has_proxy=False,
            size_bytes=file_size_int,
        )

        self._send_json(201, record)

    def _handle_ingest_queue(self) -> None:
        self._send_json(200, self.ingest_queue.get_status())

    def _handle_media_list(self) -> None:
        records = self.registry.list_all()
        self._send_json(200, records)

    def _handle_media_get(self, content_id: str) -> None:
        record = self.registry.get(content_id)
        if record is None:
            self._send_not_found(f"Media not found: {content_id}")
            return
        self._send_json(200, record)

    def _handle_media_lineage(self, content_id: str) -> None:
        lineage = self.registry.get_lineage(content_id)
        if lineage is None:
            self._send_not_found(f"Media not found: {content_id}")
            return
        self._send_json(200, lineage)

    def _handle_proxy_generate(self) -> None:
        try:
            body = self._read_json_body()
        except (json.JSONDecodeError, ValueError, OverflowError):
            self._send_bad_request("Invalid or oversized request body")
            return

        content_id = body.get("content_id")
        if not content_id:
            self._send_bad_request("Required field: content_id")
            return

        record = self.registry.get(content_id)
        if record is None:
            self._send_not_found(f"Media not found: {content_id}")
            return

        # Simulate proxy generation (no FFmpeg available)
        proxy_seed = f"proxy:{content_id}:{record.get('filename', '')}:960x540"
        proxy_hash = hashlib.sha256(proxy_seed.encode("utf-8")).hexdigest()
        proxy_size = int(proxy_hash[:8], 16) % (100 * 1024 * 1024) + 1024

        proxy_record: dict[str, Any] = {
            "source_content_hash": content_id,
            "proxy_content_hash": proxy_hash,
            "proxy_resolution": "960x540",
            "proxy_format": "mp4",
            "proxy_codec": "h264",
            "proxy_size_bytes": proxy_size,
            "proxy_path": f"/proxies/{content_id[:16]}_proxy_960x540.mp4",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
        }

        # Update cache to reflect proxy availability
        self.media_cache.put(
            content_hash=content_id,
            has_original=True,
            has_proxy=True,
            proxy_resolution="960x540",
            size_bytes=record.get("file_size", 0),
        )

        # Add lineage link
        self.registry.add_lineage_link(
            source_hash=content_id,
            derived_hash=proxy_hash,
            link_type="source_to_proxy",
            transform_id="proxy_h264_960x540",
            agent="cineos-proxy-generator",
        )

        self._send_json(201, proxy_record)

    def _handle_cache_state(self) -> None:
        self._send_json(200, self.media_cache.get_full_state())

    def _handle_render_create(self) -> None:
        try:
            body = self._read_json_body()
        except (json.JSONDecodeError, ValueError, OverflowError):
            self._send_bad_request("Invalid or oversized request body")
            return

        source_media_id = body.get("source_media_id")
        job_type = body.get("job_type")

        if not source_media_id:
            self._send_bad_request("Required field: source_media_id")
            return

        if not job_type:
            self._send_bad_request("Required field: job_type")
            return

        # Map to the RenderManager submit contract
        render_request: dict[str, Any] = {
            "source_content_hash": source_media_id,
            "source_filename": body.get("source_filename", "unknown"),
            "output_format": body.get("output_format", "mp4"),
            "render_profile": body.get("render_profile", "default"),
            "parameters": body.get("parameters", {}),
            "requested_by": body.get("requested_by", "cineos-api"),
        }

        job = self.render_manager.submit(render_request)

        # Add lineage link for the render output
        if job.get("output_ref"):
            self.registry.add_lineage_link(
                source_hash=source_media_id,
                derived_hash=job["output_ref"]["output_content_hash"],
                link_type="source_to_render_output",
                transform_id=body.get("render_profile", "default"),
                agent="cineos-render-engine",
            )

        self._send_json(201, job)

    def _handle_render_get(self, contract_id: str) -> None:
        job = self.render_manager.get_job(contract_id)
        if job is None:
            self._send_not_found(f"Render job not found: {contract_id}")
            return
        self._send_json(200, job)


# ---------------------------------------------------------------------------
# Server factory and entry point
# ---------------------------------------------------------------------------

def create_server(
    host: str = "0.0.0.0",
    port: int = 9400,
) -> HTTPServer:
    """
    Create and return a configured CINEOS API HTTPServer instance.

    Shared state (registry, queue, cache, render manager) is attached
    to the handler class so all requests share the same in-memory state.
    """
    # Initialize shared state
    CINEOSRequestHandler.registry = MediaRegistry()
    CINEOSRequestHandler.ingest_queue = IngestQueue()
    CINEOSRequestHandler.media_cache = MediaCache()
    CINEOSRequestHandler.render_manager = RenderManager()

    server = HTTPServer((host, port), CINEOSRequestHandler)
    return server


def serve() -> None:
    """Run the CINEOS Media Runtime API server."""
    import sys

    port = 9400
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"Invalid port: {sys.argv[1]}", file=sys.stderr)
            sys.exit(1)

    server = create_server(port=port)
    print(f"CINEOS Media Runtime API server listening on http://0.0.0.0:{port}")
    print("Endpoints:")
    print("  GET  /api/health")
    print("  POST /api/ingest")
    print("  GET  /api/ingest/queue")
    print("  GET  /api/media")
    print("  GET  /api/media/<content_id>")
    print("  GET  /api/media/<content_id>/lineage")
    print("  POST /api/proxy/generate")
    print("  GET  /api/cache")
    print("  POST /api/render/create")
    print("  GET  /api/render/<contract_id>")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    serve()

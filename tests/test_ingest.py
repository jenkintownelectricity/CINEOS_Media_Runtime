"""
CINEOS Media Runtime -- Media Ingest Tests

Covers:
  - Upload URL generation (pre-signed URLs)
  - Checksum computation (SHA-256, content-addressed ID)
  - Media registration with duplicate detection
  - Idempotent ingest workflow (retry safety)
  - CloudEvents emission at lifecycle boundaries
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import tempfile
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Helpers -- resolve imports relative to project root
# ---------------------------------------------------------------------------
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest.checksum import (
    ChecksumMismatchError,
    compute_sha256,
    compute_sha256_bytes,
    require_checksum,
    validate_checksum,
)
from ingest.events import (
    CloudEvent,
    EventType,
    InMemoryEventEmitter,
)
from ingest.models import (
    IngestRecord,
    IngestStatus,
    MediaObject,
    MediaType,
    RegistrationResult,
    UploadRequest,
    UploadURL,
)
from ingest.register import InMemoryMediaDatabase, register_media
from ingest.upload import LocalStorageBackend, initiate_upload
from ingest.workflow import (
    DefaultDerivativeJobCreator,
    IngestWorkflow,
    InMemoryIdempotencyStore,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CONTENT = b"CINEOS sample media content for testing"
SAMPLE_SHA256 = hashlib.sha256(SAMPLE_CONTENT).hexdigest()
TENANT_ID = "tenant-test-001"


def _upload_request(**overrides: Any) -> UploadRequest:
    defaults: dict[str, Any] = {
        "tenant_id": TENANT_ID,
        "filename": "rushes_001.mov",
        "content_type": "video/quicktime",
        "media_type": MediaType.VIDEO,
        "size_bytes": len(SAMPLE_CONTENT),
    }
    defaults.update(overrides)
    return UploadRequest(**defaults)


# ===================================================================
# 1. Upload URL generation
# ===================================================================


class TestUploadURLGeneration:
    """Pre-signed upload URL initiation."""

    def test_upload_url_contains_bucket_and_key(self) -> None:
        backend = LocalStorageBackend()
        request = _upload_request()

        url_obj: UploadURL = asyncio.get_event_loop().run_until_complete(
            initiate_upload(request, backend)
        )

        assert url_obj.tenant_id == TENANT_ID
        assert url_obj.storage_bucket == "cineos-media"
        assert TENANT_ID in url_obj.storage_key
        assert url_obj.url.startswith("https://")
        assert url_obj.method == "PUT"

    def test_upload_url_has_expiry(self) -> None:
        backend = LocalStorageBackend()
        request = _upload_request()

        url_obj = asyncio.get_event_loop().run_until_complete(
            initiate_upload(request, backend, expires_in_seconds=600)
        )

        assert url_obj.expires_at is not None
        assert "expires=600" in url_obj.url

    def test_upload_url_headers_include_content_type(self) -> None:
        backend = LocalStorageBackend()
        request = _upload_request(content_type="video/mp4")

        url_obj = asyncio.get_event_loop().run_until_complete(
            initiate_upload(request, backend)
        )

        assert url_obj.headers.get("Content-Type") == "video/mp4"


# ===================================================================
# 2. Checksum computation
# ===================================================================


class TestChecksumComputation:
    """SHA-256 checksum computation and validation."""

    def test_sha256_from_bytes(self) -> None:
        digest = compute_sha256(SAMPLE_CONTENT)
        assert digest == SAMPLE_SHA256
        assert len(digest) == 64

    def test_sha256_from_stream(self) -> None:
        stream = io.BytesIO(SAMPLE_CONTENT)
        digest = compute_sha256(stream)
        assert digest == SAMPLE_SHA256

    def test_sha256_from_file(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(SAMPLE_CONTENT)
            tmp_path = tmp.name
        try:
            digest = compute_sha256(tmp_path)
            assert digest == SAMPLE_SHA256
        finally:
            os.unlink(tmp_path)

    def test_sha256_bytes_convenience(self) -> None:
        assert compute_sha256_bytes(SAMPLE_CONTENT) == SAMPLE_SHA256

    def test_validate_checksum_success(self) -> None:
        assert validate_checksum(SAMPLE_CONTENT, SAMPLE_SHA256) is True

    def test_validate_checksum_failure(self) -> None:
        assert validate_checksum(SAMPLE_CONTENT, "0" * 64) is False

    def test_require_checksum_raises_on_mismatch(self) -> None:
        with pytest.raises(ChecksumMismatchError) as exc_info:
            require_checksum(SAMPLE_CONTENT, "0" * 64)
        assert exc_info.value.expected == "0" * 64
        assert exc_info.value.actual == SAMPLE_SHA256

    def test_require_checksum_returns_digest_on_match(self) -> None:
        result = require_checksum(SAMPLE_CONTENT, SAMPLE_SHA256)
        assert result == SAMPLE_SHA256

    def test_deterministic_across_calls(self) -> None:
        """Same content must always produce the same hash."""
        assert compute_sha256(SAMPLE_CONTENT) == compute_sha256(SAMPLE_CONTENT)


# ===================================================================
# 3. Media registration with content-addressed ID
# ===================================================================


class TestMediaRegistration:
    """Registration creates a content-addressed MediaObject."""

    def test_register_creates_media_with_content_hash_id(self) -> None:
        db = InMemoryMediaDatabase()
        emitter = InMemoryEventEmitter()

        result: RegistrationResult = asyncio.get_event_loop().run_until_complete(
            register_media(
                tenant_id=TENANT_ID,
                source=SAMPLE_CONTENT,
                filename="rushes.mov",
                content_type="video/quicktime",
                media_type=MediaType.VIDEO,
                size_bytes=len(SAMPLE_CONTENT),
                storage_bucket="cineos-media",
                storage_key="tenants/t1/uploads/u1/rushes.mov",
                database=db,
                emitter=emitter,
            )
        )

        assert result.media_id == SAMPLE_SHA256
        assert result.is_duplicate is False
        assert result.media_object.media_id == SAMPLE_SHA256
        assert result.media_object.checksum_sha256 == SAMPLE_SHA256
        assert result.media_object.tenant_id == TENANT_ID

    def test_duplicate_registration_returns_existing(self) -> None:
        db = InMemoryMediaDatabase()
        emitter = InMemoryEventEmitter()

        common_kwargs: dict[str, Any] = dict(
            tenant_id=TENANT_ID,
            source=SAMPLE_CONTENT,
            filename="rushes.mov",
            content_type="video/quicktime",
            media_type=MediaType.VIDEO,
            size_bytes=len(SAMPLE_CONTENT),
            storage_bucket="cineos-media",
            storage_key="tenants/t1/uploads/u1/rushes.mov",
            database=db,
            emitter=emitter,
        )

        loop = asyncio.get_event_loop()

        first = loop.run_until_complete(register_media(**common_kwargs))
        second = loop.run_until_complete(register_media(**common_kwargs))

        assert first.is_duplicate is False
        assert second.is_duplicate is True
        assert second.media_id == first.media_id
        # Only one insert should have occurred
        assert db.count == 1

    def test_registration_emits_cloud_event(self) -> None:
        db = InMemoryMediaDatabase()
        emitter = InMemoryEventEmitter()

        asyncio.get_event_loop().run_until_complete(
            register_media(
                tenant_id=TENANT_ID,
                source=SAMPLE_CONTENT,
                filename="rushes.mov",
                content_type="video/quicktime",
                media_type=MediaType.VIDEO,
                size_bytes=len(SAMPLE_CONTENT),
                storage_bucket="cineos-media",
                storage_key="tenants/t1/uploads/u1/rushes.mov",
                database=db,
                emitter=emitter,
            )
        )

        events = emitter.get_events(EventType.REGISTERED)
        assert len(events) == 1
        evt = events[0]
        assert evt.tenant_id == TENANT_ID
        assert evt.data["media_id"] == SAMPLE_SHA256
        assert evt.data["is_duplicate"] is False

    def test_duplicate_does_not_re_emit(self) -> None:
        db = InMemoryMediaDatabase()
        emitter = InMemoryEventEmitter()

        common_kwargs: dict[str, Any] = dict(
            tenant_id=TENANT_ID,
            source=SAMPLE_CONTENT,
            filename="rushes.mov",
            content_type="video/quicktime",
            media_type=MediaType.VIDEO,
            size_bytes=len(SAMPLE_CONTENT),
            storage_bucket="cineos-media",
            storage_key="tenants/t1/uploads/u1/rushes.mov",
            database=db,
            emitter=emitter,
        )

        loop = asyncio.get_event_loop()
        loop.run_until_complete(register_media(**common_kwargs))
        loop.run_until_complete(register_media(**common_kwargs))

        # Only the first registration emits a media.registered event
        registered_events = emitter.get_events(EventType.REGISTERED)
        assert len(registered_events) == 1


# ===================================================================
# 4. Idempotent ingest workflow
# ===================================================================


class TestIdempotentIngestWorkflow:
    """End-to-end ingest with idempotency-key support."""

    def _build_workflow(self) -> tuple[IngestWorkflow, InMemoryEventEmitter]:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        return workflow, emitter

    def test_full_ingest_completes(self) -> None:
        workflow, emitter = self._build_workflow()
        request = _upload_request()

        record: IngestRecord = asyncio.get_event_loop().run_until_complete(
            workflow.run(request, SAMPLE_CONTENT)
        )

        assert record.status == IngestStatus.COMPLETED
        assert record.media_id == SAMPLE_SHA256
        assert record.tenant_id == TENANT_ID

    def test_idempotent_retry_returns_same_record(self) -> None:
        workflow, _ = self._build_workflow()
        request = _upload_request(idempotency_key="idem-key-1")
        loop = asyncio.get_event_loop()

        first = loop.run_until_complete(workflow.run(request, SAMPLE_CONTENT))
        second = loop.run_until_complete(workflow.run(request, SAMPLE_CONTENT))

        assert first.status == IngestStatus.COMPLETED
        assert second.status == IngestStatus.COMPLETED
        # The second call should return the stored record (same ingest_id)
        assert second.ingest_id == first.ingest_id

    def test_different_idempotency_keys_create_separate_records(self) -> None:
        workflow, _ = self._build_workflow()
        loop = asyncio.get_event_loop()

        r1 = loop.run_until_complete(
            workflow.run(_upload_request(idempotency_key="key-a"), SAMPLE_CONTENT)
        )
        r2 = loop.run_until_complete(
            workflow.run(_upload_request(idempotency_key="key-b"), SAMPLE_CONTENT)
        )

        # Different idempotency keys => different ingest IDs
        assert r1.ingest_id != r2.ingest_id
        # But same content => same media_id (content-addressed)
        # The second one will be a DUPLICATE at the registration level
        assert r2.status == IngestStatus.DUPLICATE
        assert r2.media_id == r1.media_id

    def test_workflow_without_idempotency_key(self) -> None:
        """No idempotency key => always runs fresh."""
        workflow, _ = self._build_workflow()
        request = _upload_request()  # no idempotency_key
        loop = asyncio.get_event_loop()

        r1 = loop.run_until_complete(workflow.run(request, SAMPLE_CONTENT))
        r2 = loop.run_until_complete(workflow.run(request, SAMPLE_CONTENT))

        assert r1.ingest_id != r2.ingest_id


# ===================================================================
# 5. CloudEvents emission
# ===================================================================


class TestCloudEventsEmission:
    """Verify correct CloudEvents are emitted during ingest."""

    def test_ingest_emits_upload_initiated_event(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        request = _upload_request()

        asyncio.get_event_loop().run_until_complete(
            workflow.run(request, SAMPLE_CONTENT)
        )

        upload_events = emitter.get_events(EventType.UPLOAD_INITIATED)
        assert len(upload_events) >= 1
        evt = upload_events[0]
        assert evt.tenant_id == TENANT_ID
        assert evt.data["filename"] == "rushes_001.mov"

    def test_ingest_emits_upload_completed_event(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )

        asyncio.get_event_loop().run_until_complete(
            workflow.run(_upload_request(), SAMPLE_CONTENT)
        )

        completed_events = emitter.get_events(EventType.UPLOAD_COMPLETED)
        assert len(completed_events) >= 1

    def test_ingest_emits_registered_event(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )

        asyncio.get_event_loop().run_until_complete(
            workflow.run(_upload_request(), SAMPLE_CONTENT)
        )

        reg_events = emitter.get_events(EventType.REGISTERED)
        assert len(reg_events) == 1
        assert reg_events[0].data["media_id"] == SAMPLE_SHA256

    def test_ingest_emits_completed_event(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )

        asyncio.get_event_loop().run_until_complete(
            workflow.run(_upload_request(), SAMPLE_CONTENT)
        )

        done_events = emitter.get_events(EventType.INGEST_COMPLETED)
        assert len(done_events) == 1
        assert done_events[0].data["media_id"] == SAMPLE_SHA256

    def test_cloud_event_has_specversion(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )

        asyncio.get_event_loop().run_until_complete(
            workflow.run(_upload_request(), SAMPLE_CONTENT)
        )

        for evt in emitter.events:
            assert evt.specversion == "1.0"

    def test_all_events_carry_tenant_id(self) -> None:
        emitter = InMemoryEventEmitter()
        workflow = IngestWorkflow(
            storage=LocalStorageBackend(),
            database=InMemoryMediaDatabase(),
            emitter=emitter,
            idempotency_store=InMemoryIdempotencyStore(),
        )

        asyncio.get_event_loop().run_until_complete(
            workflow.run(_upload_request(), SAMPLE_CONTENT)
        )

        for evt in emitter.events:
            assert evt.tenant_id == TENANT_ID

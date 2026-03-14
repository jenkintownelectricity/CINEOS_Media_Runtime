"""
CINEOS Media Runtime — Idempotent ingest workflow.

Orchestrates the full ingest pipeline:
  Upload -> Checksum -> Register -> Create derivative jobs -> Emit events

Features:
  - Idempotency key support (retry-safe)
  - Status tracking per stage
  - Automatic event emission at each lifecycle boundary
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, BinaryIO, Optional, Protocol, Union

from ingest.checksum import compute_sha256
from ingest.events import (
    CloudEvent,
    EventEmitter,
    EventType,
    InMemoryEventEmitter,
    ingest_completed,
    ingest_failed,
    upload_completed,
    upload_initiated,
)
from ingest.models import (
    DerivativeJob,
    IngestRecord,
    IngestStatus,
    MediaType,
    UploadRequest,
)
from ingest.register import MediaDatabase, register_media
from ingest.upload import StorageBackend, initiate_upload


# ---------------------------------------------------------------------------
# Idempotency store protocol
# ---------------------------------------------------------------------------

class IdempotencyStore(Protocol):
    """Protocol for storing / retrieving ingest records by idempotency key."""

    async def get_by_idempotency_key(
        self, tenant_id: str, key: str,
    ) -> Optional[IngestRecord]:
        ...

    async def get_by_ingest_id(
        self, tenant_id: str, ingest_id: str,
    ) -> Optional[IngestRecord]:
        ...

    async def save(self, record: IngestRecord) -> None:
        ...


class InMemoryIdempotencyStore:
    """Simple in-memory idempotency store for testing."""

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, str], IngestRecord] = {}
        self._by_id: dict[tuple[str, str], IngestRecord] = {}

    async def get_by_idempotency_key(
        self, tenant_id: str, key: str,
    ) -> Optional[IngestRecord]:
        return self._by_key.get((tenant_id, key))

    async def get_by_ingest_id(
        self, tenant_id: str, ingest_id: str,
    ) -> Optional[IngestRecord]:
        return self._by_id.get((tenant_id, ingest_id))

    async def save(self, record: IngestRecord) -> None:
        self._by_id[(record.tenant_id, record.ingest_id)] = record
        if record.idempotency_key:
            self._by_key[(record.tenant_id, record.idempotency_key)] = record


# ---------------------------------------------------------------------------
# Derivative job creator protocol
# ---------------------------------------------------------------------------

class DerivativeJobCreator(Protocol):
    """Protocol for creating derivative processing jobs."""

    async def create_jobs(
        self, tenant_id: str, media_id: str, media_type: MediaType,
    ) -> list[DerivativeJob]:
        ...


class DefaultDerivativeJobCreator:
    """
    Creates a standard set of derivative jobs based on media type.
    """

    DERIVATIVE_MAP: dict[MediaType, list[str]] = {
        MediaType.VIDEO: ["thumbnail", "proxy_h264", "transcode_h265"],
        MediaType.AUDIO: ["waveform", "transcode_aac"],
        MediaType.IMAGE: ["thumbnail", "web_optimized"],
        MediaType.SUBTITLE: [],
        MediaType.DCP: ["validation", "kdm_check"],
        MediaType.OTHER: [],
    }

    async def create_jobs(
        self, tenant_id: str, media_id: str, media_type: MediaType,
    ) -> list[DerivativeJob]:
        derivative_types = self.DERIVATIVE_MAP.get(media_type, [])
        now = datetime.datetime.now(datetime.timezone.utc)
        return [
            DerivativeJob(
                job_id=str(uuid.uuid4()),
                tenant_id=tenant_id,
                media_id=media_id,
                derivative_type=dt,
                created_at=now,
            )
            for dt in derivative_types
        ]


# ---------------------------------------------------------------------------
# Ingest workflow
# ---------------------------------------------------------------------------

class IngestWorkflow:
    """
    Orchestrates the full, idempotent media ingest pipeline.

    Usage::

        workflow = IngestWorkflow(
            storage=my_storage,
            database=my_db,
            emitter=my_emitter,
            idempotency_store=my_store,
        )
        result = await workflow.run(request, file_data)
    """

    def __init__(
        self,
        storage: StorageBackend,
        database: MediaDatabase,
        emitter: EventEmitter,
        idempotency_store: IdempotencyStore,
        derivative_creator: Optional[DerivativeJobCreator] = None,
        bucket: str = "cineos-media",
    ) -> None:
        self.storage = storage
        self.database = database
        self.emitter = emitter
        self.idempotency_store = idempotency_store
        self.derivative_creator = derivative_creator or DefaultDerivativeJobCreator()
        self.bucket = bucket

    async def run(
        self,
        request: UploadRequest,
        file_data: Union[bytes, BinaryIO],
    ) -> IngestRecord:
        """
        Execute the full ingest workflow.

        If an ``idempotency_key`` is set on *request* and a matching completed
        record exists, the previous result is returned immediately (retry safety).

        Parameters
        ----------
        request
            Upload/ingest request with tenant, filename, media type, etc.
        file_data
            Raw bytes or readable binary stream of the media content.

        Returns
        -------
        IngestRecord
            The final state of the ingest operation.
        """
        # ----- Idempotency check -----
        if request.idempotency_key:
            existing = await self.idempotency_store.get_by_idempotency_key(
                request.tenant_id, request.idempotency_key,
            )
            if existing is not None and existing.status in (
                IngestStatus.COMPLETED, IngestStatus.DUPLICATE,
            ):
                return existing

        ingest_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        record = IngestRecord(
            ingest_id=ingest_id,
            tenant_id=request.tenant_id,
            idempotency_key=request.idempotency_key,
            status=IngestStatus.UPLOAD_INITIATED,
            filename=request.filename,
            content_type=request.content_type,
            media_type=request.media_type,
            size_bytes=request.size_bytes,
            created_at=now,
            updated_at=now,
        )

        try:
            # 1. Initiate upload (generate signed URL)
            upload_url = await initiate_upload(
                request, self.storage, bucket=self.bucket,
            )
            record = record.model_copy(update={
                "upload_id": upload_url.upload_id,
                "status": IngestStatus.UPLOAD_INITIATED,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            await self.emitter.emit(upload_initiated(
                tenant_id=request.tenant_id,
                upload_id=upload_url.upload_id,
                filename=request.filename,
                content_type=request.content_type,
                size_bytes=request.size_bytes,
            ))

            # 2. Mark upload complete (in real system, this would be
            #    triggered by a callback/webhook from storage)
            record = record.model_copy(update={
                "status": IngestStatus.UPLOAD_COMPLETED,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            await self.emitter.emit(upload_completed(
                tenant_id=request.tenant_id,
                upload_id=upload_url.upload_id,
                storage_bucket=upload_url.storage_bucket,
                storage_key=upload_url.storage_key,
                size_bytes=request.size_bytes,
            ))

            # 3. Checksum + Register
            record = record.model_copy(update={
                "status": IngestStatus.REGISTERING,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            reg_result = await register_media(
                tenant_id=request.tenant_id,
                source=file_data,
                filename=request.filename,
                content_type=request.content_type,
                media_type=request.media_type,
                size_bytes=request.size_bytes,
                storage_bucket=upload_url.storage_bucket,
                storage_key=upload_url.storage_key,
                metadata=request.metadata,
                database=self.database,
                emitter=self.emitter,
            )

            media_id = reg_result.media_id

            if reg_result.is_duplicate:
                record = record.model_copy(update={
                    "media_id": media_id,
                    "status": IngestStatus.DUPLICATE,
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                })
                await self.idempotency_store.save(record)
                return record

            record = record.model_copy(update={
                "media_id": media_id,
                "status": IngestStatus.REGISTERED,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            # 4. Create derivative jobs
            record = record.model_copy(update={
                "status": IngestStatus.CREATING_DERIVATIVES,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            derivative_jobs = await self.derivative_creator.create_jobs(
                tenant_id=request.tenant_id,
                media_id=media_id,
                media_type=request.media_type,
            )

            # 5. Mark completed + emit final event
            record = record.model_copy(update={
                "status": IngestStatus.COMPLETED,
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            await self.emitter.emit(ingest_completed(
                tenant_id=request.tenant_id,
                ingest_id=ingest_id,
                media_id=media_id,
                derivative_job_ids=[j.job_id for j in derivative_jobs],
            ))

            return record

        except Exception as exc:
            record = record.model_copy(update={
                "status": IngestStatus.FAILED,
                "error_message": str(exc),
                "updated_at": datetime.datetime.now(datetime.timezone.utc),
            })
            await self.idempotency_store.save(record)

            await self.emitter.emit(ingest_failed(
                tenant_id=request.tenant_id,
                ingest_id=ingest_id,
                error=str(exc),
                stage=record.status.value,
            ))

            return record

"""
CINEOS Media Runtime -- Derivative Generation Tests

Tests covering:
  - Pydantic models (DerivativeSpec, DerivativeResult, TranscodeJob)
  - Transcode profiles (determinism, FFmpeg args, registry)
  - Provenance capture (W3C PROV document structure)
  - CloudEvents (emission, handler dispatch, schema)
  - TranscodeEngine (idempotency, job execution, provenance building)
  - ProxyGenerator (thumbnails, contact sheets, waveforms)
  - DerivativeMeta dataclass
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure the project root is on sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from transcode.models import (
    DerivativeResult,
    DerivativeSpec,
    MediaType,
    SourceAsset,
    TranscodeJob,
    TranscodeStatus,
)
from transcode.profiles import (
    AUDIO_AAC,
    AUDIO_WAV,
    CONTACT_SHEET,
    PROXY_360P,
    PROXY_720P,
    PROXY_1080P,
    THUMBNAIL_LARGE,
    THUMBNAIL_MEDIUM,
    THUMBNAIL_SMALL,
    WAVEFORM,
    ProfileCategory,
    TranscodeProfile,
    get_profile,
    list_profiles,
    register_profile,
)
from transcode.provenance import (
    ProvenanceCapture,
    ProvenanceRecord,
    ProvActivity,
    ProvAgent,
    ProvEntity,
)
from transcode.events import (
    PROXY_GENERATED,
    THUMBNAIL_GENERATED,
    TRANSCODE_COMPLETED,
    TRANSCODE_FAILED,
    TRANSCODE_STARTED,
    CloudEvent,
    TranscodeEventEmitter,
)
from transcode.engine import TranscodeEngine
from proxy.generator import DerivativeMeta, ProxyGenerator


# ======================================================================
# Fixtures
# ======================================================================

TENANT = "tenant-acme"
SOURCE_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"


@pytest.fixture
def tmp_output_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def emitter():
    return TranscodeEventEmitter()


@pytest.fixture
def engine(tmp_output_dir, emitter):
    return TranscodeEngine(
        output_base_dir=tmp_output_dir,
        event_emitter=emitter,
    )


@pytest.fixture
def source_file(tmp_output_dir):
    """Create a small dummy source file for hash-based tests."""
    src = os.path.join(tmp_output_dir, "source.mov")
    Path(src).write_bytes(b"fake source media content for testing")
    return src


@pytest.fixture
def proxy_gen(tmp_output_dir):
    return ProxyGenerator(
        output_dir=tmp_output_dir,
        ffmpeg_path="ffmpeg",
        ffprobe_path="ffprobe",
    )


# ======================================================================
# Model tests
# ======================================================================


class TestDerivativeSpec:
    def test_deterministic_output_filename(self):
        spec = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            params={"width": 1280},
            tenant_id=TENANT,
        )
        assert spec.output_filename == f"{SOURCE_HASH}_proxy_720p.mp4"

    def test_same_params_same_hash(self):
        params = {"width": 1280, "crf": 23, "codec": "libx264"}
        s1 = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            params=params,
            tenant_id=TENANT,
        )
        s2 = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            params=dict(reversed(list(params.items()))),
            tenant_id=TENANT,
        )
        assert s1.param_hash == s2.param_hash

    def test_source_hash_validation(self):
        with pytest.raises(Exception):
            DerivativeSpec(
                source_hash="short",
                transform_id="proxy_720p",
                media_type=MediaType.VIDEO,
                output_format="mp4",
                tenant_id=TENANT,
            )

    def test_output_filename_deterministic_across_calls(self):
        spec = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="thumb_md",
            media_type=MediaType.IMAGE,
            output_format="jpg",
            tenant_id=TENANT,
        )
        assert spec.output_filename == spec.output_filename


class TestDerivativeResult:
    def test_succeeded_flag(self):
        spec = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        ok = DerivativeResult(spec=spec, status=TranscodeStatus.COMPLETED)
        assert ok.succeeded is True

        fail = DerivativeResult(spec=spec, status=TranscodeStatus.FAILED)
        assert fail.succeeded is False

        skip = DerivativeResult(spec=spec, status=TranscodeStatus.SKIPPED)
        assert skip.succeeded is False


class TestTranscodeJob:
    def test_lifecycle(self):
        job = TranscodeJob(
            tenant_id=TENANT,
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
        )
        assert job.status == TranscodeStatus.PENDING
        job.mark_started()
        assert job.status == TranscodeStatus.STARTED
        job.mark_completed()
        assert job.status == TranscodeStatus.COMPLETED

    def test_retry_logic(self):
        job = TranscodeJob(
            tenant_id=TENANT,
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            max_retries=2,
        )
        job.mark_failed("error 1")
        assert job.retry_count == 1
        assert job.is_retryable is True

        job.mark_failed("error 2")
        assert job.retry_count == 2
        assert job.is_retryable is False

    def test_add_result(self):
        job = TranscodeJob(
            tenant_id=TENANT,
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
        )
        spec = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        result = DerivativeResult(spec=spec, status=TranscodeStatus.COMPLETED)
        job.add_result(result)
        assert len(job.results) == 1


class TestSourceAsset:
    def test_creation(self):
        asset = SourceAsset(
            path="/media/clip.mov",
            content_hash=SOURCE_HASH,
            media_type=MediaType.VIDEO,
            tenant_id=TENANT,
            width=3840,
            height=2160,
            duration_seconds=120.5,
        )
        assert asset.media_type == MediaType.VIDEO
        assert asset.width == 3840


# ======================================================================
# Profile tests
# ======================================================================


class TestTranscodeProfiles:
    def test_builtin_profiles_exist(self):
        for pid in [
            "proxy_360p", "proxy_720p", "proxy_1080p",
            "audio_aac", "audio_wav",
            "thumb_sm", "thumb_md", "thumb_lg",
            "contact_sheet", "waveform",
        ]:
            profile = get_profile(pid)
            assert profile.profile_id == pid

    def test_unknown_profile_raises(self):
        with pytest.raises(KeyError):
            get_profile("nonexistent_profile")

    def test_profile_is_frozen(self):
        with pytest.raises(AttributeError):
            PROXY_720P.profile_id = "changed"

    def test_transform_id_equals_profile_id(self):
        assert PROXY_720P.transform_id == "proxy_720p"

    def test_param_fingerprint_deterministic(self):
        fp1 = PROXY_720P.param_fingerprint
        fp2 = PROXY_720P.param_fingerprint
        assert fp1 == fp2
        assert len(fp1) == 16

    def test_different_profiles_different_fingerprints(self):
        assert PROXY_720P.param_fingerprint != PROXY_1080P.param_fingerprint

    def test_ffmpeg_args_video(self):
        args = PROXY_720P.to_ffmpeg_args()
        assert "-c:v" in args
        assert "libx264" in args
        assert "-vf" in args
        vf_idx = args.index("-vf")
        assert "scale=1280:" in args[vf_idx + 1]

    def test_ffmpeg_args_audio_only(self):
        args = AUDIO_AAC.to_ffmpeg_args()
        assert "-c:a" in args
        assert "aac" in args
        assert "-c:v" not in args

    def test_list_profiles_all(self):
        all_profiles = list_profiles()
        assert len(all_profiles) >= 10

    def test_list_profiles_by_category(self):
        video_profiles = list_profiles(category=ProfileCategory.VIDEO_PROXY)
        assert all(p.category == ProfileCategory.VIDEO_PROXY for p in video_profiles)
        assert len(video_profiles) >= 3

    def test_register_custom_profile(self):
        custom = TranscodeProfile(
            profile_id="custom_test_profile",
            category=ProfileCategory.VIDEO_PROXY,
            output_format="webm",
            params={"video_codec": "libvpx-vp9", "crf": 30},
        )
        register_profile(custom)
        assert get_profile("custom_test_profile") is custom


# ======================================================================
# Provenance tests
# ======================================================================


class TestProvenance:
    def test_source_entity(self):
        entity = ProvEntity.for_source(
            content_hash=SOURCE_HASH,
            path="/media/clip.mov",
            tenant_id=TENANT,
        )
        assert "cineos:source:" in entity.entity_id
        assert entity.attributes["cineos:contentHash"] == SOURCE_HASH

    def test_derivative_entity(self):
        entity = ProvEntity.for_derivative(
            output_hash="deadbeef1234",
            output_path="/out/proxy.mp4",
            transform_id="proxy_720p",
            tenant_id=TENANT,
        )
        assert "cineos:derivative:" in entity.entity_id
        assert entity.attributes["cineos:transformId"] == "proxy_720p"

    def test_activity_creation(self):
        activity = ProvActivity.for_transcode(
            profile_id="proxy_720p",
            params={"crf": 23},
            tenant_id=TENANT,
        )
        assert "cineos:transcode:" in activity.activity_id
        assert activity.started_at is not None

    def test_provenance_capture_builder(self):
        capture = ProvenanceCapture(tenant_id=TENANT)
        capture.set_source(content_hash=SOURCE_HASH, path="/media/clip.mov")
        capture.start_activity(profile_id="proxy_720p", params={"crf": 23})
        capture.finish_activity()
        capture.set_output(
            output_hash="outputhash123",
            output_path="/out/proxy.mp4",
            transform_id="proxy_720p",
        )
        record = capture.build()
        assert isinstance(record, ProvenanceRecord)
        assert record.tenant_id == TENANT

    def test_provenance_capture_requires_source(self):
        capture = ProvenanceCapture(tenant_id=TENANT)
        capture.start_activity(profile_id="proxy_720p", params={})
        capture.set_output(
            output_hash="x", output_path="/x", transform_id="proxy_720p"
        )
        with pytest.raises(ValueError, match="Source entity"):
            capture.build()

    def test_prov_document_structure(self):
        capture = ProvenanceCapture(tenant_id=TENANT)
        capture.set_source(content_hash=SOURCE_HASH, path="/media/clip.mov")
        capture.start_activity(profile_id="proxy_720p", params={"crf": 23})
        capture.finish_activity()
        capture.set_output(
            output_hash="outputhash123",
            output_path="/out/proxy.mp4",
            transform_id="proxy_720p",
        )
        record = capture.build()
        doc = record.to_prov_document()

        # W3C PROV-JSON required keys
        assert "entity" in doc
        assert "activity" in doc
        assert "agent" in doc
        assert "wasGeneratedBy" in doc
        assert "used" in doc
        assert "wasDerivedFrom" in doc
        assert "wasAssociatedWith" in doc
        assert "prefix" in doc
        assert doc["prefix"]["prov"] == "http://www.w3.org/ns/prov#"

    def test_prov_document_entities_count(self):
        capture = ProvenanceCapture(tenant_id=TENANT)
        capture.set_source(content_hash=SOURCE_HASH, path="/media/clip.mov")
        capture.start_activity(profile_id="proxy_720p", params={})
        capture.set_output(
            output_hash="outhash", output_path="/out.mp4", transform_id="proxy_720p"
        )
        doc = capture.build().to_prov_document()
        assert len(doc["entity"]) == 2


# ======================================================================
# CloudEvent tests
# ======================================================================


class TestCloudEvents:
    def test_event_construction(self):
        event = CloudEvent(
            type=TRANSCODE_STARTED,
            tenantid=TENANT,
            data={"source_hash": SOURCE_HASH},
        )
        assert event.specversion == "1.0"
        assert event.type == TRANSCODE_STARTED
        assert event.tenantid == TENANT
        assert event.datacontenttype == "application/json"

    def test_event_serialization(self):
        event = CloudEvent(
            type=TRANSCODE_COMPLETED,
            tenantid=TENANT,
            data={"output": "proxy.mp4"},
        )
        data = json.loads(event.to_json())
        assert data["type"] == TRANSCODE_COMPLETED
        assert data["specversion"] == "1.0"

    def test_emitter_handler_dispatch(self, emitter):
        received = []
        emitter.on(TRANSCODE_STARTED, lambda e: received.append(e))

        emitter.transcode_started(
            tenant_id=TENANT,
            job_id="job-1",
            source_hash=SOURCE_HASH,
            source_path="/media/clip.mov",
            profile_id="proxy_720p",
        )
        assert len(received) == 1
        assert received[0].type == TRANSCODE_STARTED

    def test_emitter_global_handler(self, emitter):
        received = []
        emitter.on_all(lambda e: received.append(e))

        emitter.transcode_started(
            tenant_id=TENANT, job_id="j1", source_hash=SOURCE_HASH,
            source_path="/clip.mov", profile_id="proxy_720p",
        )
        emitter.transcode_completed(
            tenant_id=TENANT, job_id="j1", source_hash=SOURCE_HASH,
            profile_id="proxy_720p", output_path="/out.mp4",
            output_hash="hash", duration_seconds=1.0, output_size_bytes=1000,
        )
        assert len(received) == 2

    def test_emitter_event_log(self, emitter):
        emitter.transcode_started(
            tenant_id=TENANT, job_id="j1", source_hash=SOURCE_HASH,
            source_path="/clip.mov", profile_id="proxy_720p",
        )
        assert len(emitter.event_log) == 1
        emitter.clear_log()
        assert len(emitter.event_log) == 0

    def test_emitter_transcode_failed(self, emitter):
        received = []
        emitter.on(TRANSCODE_FAILED, lambda e: received.append(e))

        emitter.transcode_failed(
            tenant_id=TENANT, job_id="j1", source_hash=SOURCE_HASH,
            profile_id="proxy_720p", error="codec not found", retry_count=1,
        )
        assert len(received) == 1
        assert received[0].data["error"] == "codec not found"
        assert received[0].data["retry_count"] == 1

    def test_emitter_proxy_generated(self, emitter):
        received = []
        emitter.on(PROXY_GENERATED, lambda e: received.append(e))

        emitter.proxy_generated(
            tenant_id=TENANT, source_hash=SOURCE_HASH,
            profile_id="proxy_720p", output_path="/out.mp4",
            output_hash="hash123",
        )
        assert len(received) == 1

    def test_emitter_thumbnail_generated(self, emitter):
        received = []
        emitter.on(THUMBNAIL_GENERATED, lambda e: received.append(e))

        emitter.thumbnail_generated(
            tenant_id=TENANT, source_hash=SOURCE_HASH,
            profile_id="thumb_md", output_path="/out.jpg",
            output_hash="hash456",
        )
        assert len(received) == 1

    def test_event_subject_is_deterministic(self, emitter):
        event = emitter.transcode_started(
            tenant_id=TENANT, job_id="j1", source_hash=SOURCE_HASH,
            source_path="/clip.mov", profile_id="proxy_720p",
        )
        assert event.subject == f"{SOURCE_HASH}_proxy_720p"

    def test_event_types_are_strings(self):
        assert isinstance(TRANSCODE_STARTED, str)
        assert TRANSCODE_STARTED.startswith("media.")


# ======================================================================
# Engine tests
# ======================================================================


class TestTranscodeEngine:
    def test_tenant_dir_structure(self, engine, tmp_output_dir):
        path = engine._tenant_dir(TENANT, "video_proxy")
        assert TENANT in path
        assert path.startswith(tmp_output_dir)

    @patch("transcode.engine.subprocess.run")
    @patch("transcode.engine._file_hash", return_value="fakehash123")
    def test_transcode_calls_ffmpeg(self, mock_hash, mock_run, engine, tmp_output_dir):
        output_dir = os.path.join(tmp_output_dir, TENANT, "video_proxy")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{SOURCE_HASH}_proxy_720p.mp4")

        def side_effect(cmd, **kw):
            Path(output_path).write_bytes(b"transcoded")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        result = engine.transcode(
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            profile="proxy_720p",
            tenant_id=TENANT,
        )
        mock_run.assert_called()
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "ffmpeg"
        assert "-i" in cmd
        assert "/media/source.mov" in cmd

    def test_idempotent_skip(self, engine, tmp_output_dir):
        """If output file already exists, transcode should return SKIPPED."""
        output_dir = os.path.join(tmp_output_dir, TENANT, "video_proxy")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{SOURCE_HASH}_proxy_720p.mp4")
        Path(output_path).write_bytes(b"existing proxy data")

        result = engine.transcode(
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            profile="proxy_720p",
            tenant_id=TENANT,
        )
        assert result.status == TranscodeStatus.SKIPPED
        assert result.output_path == output_path

    def test_ffmpeg_not_found(self, tmp_output_dir, emitter):
        engine = TranscodeEngine(
            output_base_dir=tmp_output_dir,
            ffmpeg_path="/nonexistent/ffmpeg",
            event_emitter=emitter,
        )
        result = engine.transcode(
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            profile="proxy_720p",
            tenant_id=TENANT,
        )
        assert result.status == TranscodeStatus.FAILED
        assert "not found" in result.error_message

    def test_build_provenance(self, engine):
        spec = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        result = DerivativeResult(
            spec=spec,
            status=TranscodeStatus.COMPLETED,
            output_path="/out/proxy.mp4",
            output_hash="outhash123",
        )
        prov = engine.build_provenance(
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            profile="proxy_720p",
            result=result,
            tenant_id=TENANT,
        )
        assert isinstance(prov, ProvenanceRecord)
        assert prov.tenant_id == TENANT
        doc = prov.to_prov_document()
        assert len(doc["entity"]) == 2

    @patch("transcode.engine.subprocess.run")
    @patch("transcode.engine._file_hash", return_value="fakehash")
    def test_run_job(self, mock_hash, mock_run, engine, tmp_output_dir):
        output_dir = os.path.join(tmp_output_dir, TENANT, "video_proxy")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{SOURCE_HASH}_proxy_720p.mp4")

        def side_effect(*a, **kw):
            Path(output_path).write_bytes(b"transcoded")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        job = TranscodeJob(
            tenant_id=TENANT,
            source_path="/media/source.mov",
            source_hash=SOURCE_HASH,
            derivatives=[
                DerivativeSpec(
                    source_hash=SOURCE_HASH,
                    transform_id="proxy_720p",
                    media_type=MediaType.VIDEO,
                    output_format="mp4",
                    tenant_id=TENANT,
                ),
            ],
        )

        engine.run_job(job)
        assert job.status == TranscodeStatus.COMPLETED
        assert len(job.results) == 1


# ======================================================================
# ProxyGenerator tests
# ======================================================================


class TestProxyGenerator:
    def test_thumbnail_idempotent(self, proxy_gen, tmp_output_dir, source_file):
        """If the thumbnail already exists, it should be returned without re-encoding."""
        from proxy.generator import _source_hash

        src_hash = _source_hash(source_file)
        thumb_dir = os.path.join(tmp_output_dir, "thumbnails")
        os.makedirs(thumb_dir, exist_ok=True)
        out_path = os.path.join(thumb_dir, f"{src_hash}_thumb_320.jpg")
        Path(out_path).write_bytes(b"existing thumbnail")

        results = proxy_gen.generate_thumbnail(source_file, sizes=[320])
        assert len(results) == 1
        assert results[0].derivative_type == "thumbnail"
        assert results[0].size_bytes > 0

    def test_contact_sheet_idempotent(self, proxy_gen, tmp_output_dir, source_file):
        from proxy.generator import _source_hash

        src_hash = _source_hash(source_file)
        cs_dir = os.path.join(tmp_output_dir, "contact_sheets")
        os.makedirs(cs_dir, exist_ok=True)
        out_path = os.path.join(cs_dir, f"{src_hash}_contact_sheet.jpg")
        Path(out_path).write_bytes(b"existing sheet")

        results = proxy_gen.generate_contact_sheet(source_file)
        assert len(results) == 1
        assert results[0].derivative_type == "contact_sheet"

    def test_waveform_idempotent(self, proxy_gen, tmp_output_dir, source_file):
        from proxy.generator import _source_hash

        src_hash = _source_hash(source_file)
        wf_dir = os.path.join(tmp_output_dir, "waveforms")
        os.makedirs(wf_dir, exist_ok=True)
        out_path = os.path.join(wf_dir, f"{src_hash}_waveform.png")
        Path(out_path).write_bytes(b"existing waveform")

        results = proxy_gen.generate_waveform(source_file)
        assert len(results) == 1
        assert results[0].derivative_type == "waveform"

    def test_thumbnail_ffmpeg_not_found(self, tmp_output_dir, source_file):
        gen = ProxyGenerator(
            output_dir=tmp_output_dir,
            ffmpeg_path="/nonexistent/ffmpeg",
        )
        results = gen.generate_thumbnail(source_file, sizes=[320])
        assert len(results) == 1
        assert results[0].output_hash == ""
        assert "error" in results[0].extra

    @patch("proxy.generator.subprocess.run")
    @patch("proxy.generator._file_hash", return_value="thumbhash")
    def test_generate_thumbnails_multiple_sizes(
        self, mock_hash, mock_run, proxy_gen, tmp_output_dir, source_file
    ):
        def side_effect(cmd, **kw):
            output_path = cmd[-1]
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            Path(output_path).write_bytes(b"thumb data")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        results = proxy_gen.generate_thumbnail(source_file, sizes=[160, 320, 640])
        assert len(results) == 3
        widths = {r.width for r in results}
        assert widths == {160, 320, 640}

    @patch("proxy.generator.subprocess.run")
    @patch("proxy.generator._file_hash", return_value="wavehash")
    def test_waveform_generation(
        self, mock_hash, mock_run, proxy_gen, tmp_output_dir, source_file
    ):
        def side_effect(cmd, **kw):
            output_path = cmd[-1]
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            Path(output_path).write_bytes(b"waveform data")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        results = proxy_gen.generate_waveform(source_file)
        assert len(results) == 1
        assert results[0].output_hash == "wavehash"

    @patch("proxy.generator.subprocess.run")
    @patch("proxy.generator._file_hash", return_value="sheethash")
    def test_contact_sheet_generation(
        self, mock_hash, mock_run, proxy_gen, tmp_output_dir, source_file
    ):
        with patch.object(proxy_gen, "_probe_duration", return_value=60.0):
            def side_effect(cmd, **kw):
                output_path = cmd[-1]
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                Path(output_path).write_bytes(b"sheet data")
                return MagicMock(returncode=0)

            mock_run.side_effect = side_effect

            results = proxy_gen.generate_contact_sheet(source_file)
            assert len(results) == 1
            assert results[0].output_hash == "sheethash"


# ======================================================================
# DerivativeMeta tests
# ======================================================================


class TestDerivativeMeta:
    def test_dataclass_fields(self):
        meta = DerivativeMeta(
            source_hash=SOURCE_HASH,
            derivative_type="thumbnail",
            output_path="/out/thumb.jpg",
            output_hash="abc123",
            width=320,
            size_bytes=4096,
        )
        assert meta.source_hash == SOURCE_HASH
        assert meta.derivative_type == "thumbnail"
        assert meta.width == 320
        assert meta.size_bytes == 4096

    def test_defaults(self):
        meta = DerivativeMeta(
            source_hash=SOURCE_HASH,
            derivative_type="waveform",
            output_path="/out/waveform.png",
            output_hash="def456",
        )
        assert meta.width is None
        assert meta.height is None
        assert meta.size_bytes == 0
        assert meta.extra == {}


# ======================================================================
# Deterministic naming tests
# ======================================================================


class TestDeterministicNaming:
    """Verify the core invariant: same source + same transform = same filename."""

    def test_filename_stability(self):
        for profile in [PROXY_360P, PROXY_720P, PROXY_1080P, AUDIO_AAC, AUDIO_WAV]:
            filename = f"{SOURCE_HASH}_{profile.transform_id}.{profile.output_format}"
            spec = DerivativeSpec(
                source_hash=SOURCE_HASH,
                transform_id=profile.transform_id,
                media_type=MediaType.VIDEO,
                output_format=profile.output_format,
                tenant_id=TENANT,
            )
            assert spec.output_filename == filename

    def test_different_source_different_name(self):
        spec1 = DerivativeSpec(
            source_hash="aaaa1111bbbb2222cccc3333",
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        spec2 = DerivativeSpec(
            source_hash="xxxx9999yyyy8888zzzz7777",
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        assert spec1.output_filename != spec2.output_filename

    def test_different_transform_different_name(self):
        spec1 = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_720p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        spec2 = DerivativeSpec(
            source_hash=SOURCE_HASH,
            transform_id="proxy_1080p",
            media_type=MediaType.VIDEO,
            output_format="mp4",
            tenant_id=TENANT,
        )
        assert spec1.output_filename != spec2.output_filename


# ======================================================================
# Tenant isolation tests
# ======================================================================


class TestTenantIsolation:
    def test_engine_tenant_dirs_are_separate(self, engine):
        dir_a = engine._tenant_dir("tenant-a", "video_proxy")
        dir_b = engine._tenant_dir("tenant-b", "video_proxy")
        assert dir_a != dir_b
        assert "tenant-a" in dir_a
        assert "tenant-b" in dir_b

    def test_events_carry_tenant_id(self, emitter):
        event = emitter.transcode_started(
            tenant_id="tenant-xyz",
            job_id="j1",
            source_hash=SOURCE_HASH,
            source_path="/clip.mov",
            profile_id="proxy_720p",
        )
        assert event.tenantid == "tenant-xyz"

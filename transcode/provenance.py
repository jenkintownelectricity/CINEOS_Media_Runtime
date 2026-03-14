"""
CINEOS Media Runtime — Derivative Provenance

W3C PROV-aligned provenance capture for media derivatives.
Every generated derivative records:
  - Entity (source)   — the input media asset
  - Activity           — the transform that was applied
  - Entity (output)    — the resulting derivative

Ref: https://www.w3.org/TR/prov-dm/
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# W3C PROV primitives
# ---------------------------------------------------------------------------


class ProvType(str, Enum):
    """W3C PROV core types."""

    ENTITY = "prov:Entity"
    ACTIVITY = "prov:Activity"
    AGENT = "prov:Agent"


class ProvRelation(str, Enum):
    """W3C PROV core relations."""

    WAS_GENERATED_BY = "prov:wasGeneratedBy"
    WAS_DERIVED_FROM = "prov:wasDerivedFrom"
    WAS_ATTRIBUTED_TO = "prov:wasAttributedTo"
    USED = "prov:used"
    WAS_ASSOCIATED_WITH = "prov:wasAssociatedWith"
    ACTED_ON_BEHALF_OF = "prov:actedOnBehalfOf"


class ProvEntity(BaseModel):
    """A W3C PROV Entity — a physical, digital, or conceptual thing."""

    entity_id: str = Field(..., description="Unique identifier for this entity")
    prov_type: str = ProvType.ENTITY
    attributes: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def for_source(
        cls,
        content_hash: str,
        path: str,
        tenant_id: str,
        **extra: Any,
    ) -> ProvEntity:
        return cls(
            entity_id=f"cineos:source:{content_hash}",
            attributes={
                "cineos:contentHash": content_hash,
                "cineos:path": path,
                "cineos:tenantId": tenant_id,
                **extra,
            },
        )

    @classmethod
    def for_derivative(
        cls,
        output_hash: str,
        output_path: str,
        transform_id: str,
        tenant_id: str,
        **extra: Any,
    ) -> ProvEntity:
        return cls(
            entity_id=f"cineos:derivative:{output_hash}",
            attributes={
                "cineos:contentHash": output_hash,
                "cineos:path": output_path,
                "cineos:transformId": transform_id,
                "cineos:tenantId": tenant_id,
                **extra,
            },
        )


class ProvActivity(BaseModel):
    """A W3C PROV Activity — something that occurs over time and acts upon entities."""

    activity_id: str = Field(..., description="Unique identifier for this activity")
    prov_type: str = ProvType.ACTIVITY
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def for_transcode(
        cls,
        profile_id: str,
        params: dict[str, Any],
        tenant_id: str,
    ) -> ProvActivity:
        return cls(
            activity_id=f"cineos:transcode:{uuid4().hex[:12]}",
            started_at=datetime.now(timezone.utc),
            attributes={
                "cineos:profileId": profile_id,
                "cineos:params": params,
                "cineos:tenantId": tenant_id,
                "prov:type": "cineos:TranscodeActivity",
            },
        )


class ProvAgent(BaseModel):
    """A W3C PROV Agent — something that bears responsibility for an activity."""

    agent_id: str = Field(..., description="Unique identifier for this agent")
    prov_type: str = ProvType.AGENT
    attributes: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def for_engine(cls, engine_name: str, version: str) -> ProvAgent:
        return cls(
            agent_id=f"cineos:agent:{engine_name}",
            attributes={
                "cineos:engineName": engine_name,
                "cineos:engineVersion": version,
            },
        )


# ---------------------------------------------------------------------------
# Provenance record — bundles the full derivation chain
# ---------------------------------------------------------------------------


class ProvenanceRecord(BaseModel):
    """
    Complete provenance record for a single derivative generation.

    Captures the full W3C PROV chain:
        output_entity  wasGeneratedBy  activity
        output_entity  wasDerivedFrom  source_entity
        activity       used            source_entity
        activity       wasAssociatedWith  agent
    """

    record_id: UUID = Field(default_factory=uuid4)
    source_entity: ProvEntity
    activity: ProvActivity
    output_entity: ProvEntity
    agent: ProvAgent
    tenant_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_prov_document(self) -> dict[str, Any]:
        """
        Serialize to a W3C PROV-JSON compatible document.
        See: https://www.w3.org/Submission/prov-json/
        """
        return {
            "prefix": {
                "cineos": "https://cineos.io/ns/",
                "prov": "http://www.w3.org/ns/prov#",
            },
            "entity": {
                self.source_entity.entity_id: self.source_entity.attributes,
                self.output_entity.entity_id: self.output_entity.attributes,
            },
            "activity": {
                self.activity.activity_id: {
                    "prov:startTime": (
                        self.activity.started_at.isoformat()
                        if self.activity.started_at
                        else None
                    ),
                    "prov:endTime": (
                        self.activity.ended_at.isoformat()
                        if self.activity.ended_at
                        else None
                    ),
                    **self.activity.attributes,
                },
            },
            "agent": {
                self.agent.agent_id: self.agent.attributes,
            },
            "wasGeneratedBy": {
                "_:wGB1": {
                    "prov:entity": self.output_entity.entity_id,
                    "prov:activity": self.activity.activity_id,
                }
            },
            "used": {
                "_:u1": {
                    "prov:activity": self.activity.activity_id,
                    "prov:entity": self.source_entity.entity_id,
                }
            },
            "wasDerivedFrom": {
                "_:wDF1": {
                    "prov:generatedEntity": self.output_entity.entity_id,
                    "prov:usedEntity": self.source_entity.entity_id,
                    "prov:activity": self.activity.activity_id,
                }
            },
            "wasAssociatedWith": {
                "_:wAW1": {
                    "prov:activity": self.activity.activity_id,
                    "prov:agent": self.agent.agent_id,
                }
            },
        }


# ---------------------------------------------------------------------------
# Provenance capture helper
# ---------------------------------------------------------------------------


class ProvenanceCapture:
    """
    Builder for provenance records.
    Collects source/activity/output/agent incrementally and finalizes
    into a ProvenanceRecord.
    """

    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self._source: Optional[ProvEntity] = None
        self._activity: Optional[ProvActivity] = None
        self._output: Optional[ProvEntity] = None
        self._agent: ProvAgent = ProvAgent.for_engine("cineos-transcode", "1.0.0")

    def set_source(
        self, content_hash: str, path: str, **extra: Any
    ) -> ProvenanceCapture:
        self._source = ProvEntity.for_source(
            content_hash=content_hash,
            path=path,
            tenant_id=self.tenant_id,
            **extra,
        )
        return self

    def start_activity(
        self, profile_id: str, params: dict[str, Any]
    ) -> ProvenanceCapture:
        self._activity = ProvActivity.for_transcode(
            profile_id=profile_id,
            params=params,
            tenant_id=self.tenant_id,
        )
        return self

    def finish_activity(self) -> ProvenanceCapture:
        if self._activity:
            self._activity.ended_at = datetime.now(timezone.utc)
        return self

    def set_output(
        self,
        output_hash: str,
        output_path: str,
        transform_id: str,
        **extra: Any,
    ) -> ProvenanceCapture:
        self._output = ProvEntity.for_derivative(
            output_hash=output_hash,
            output_path=output_path,
            transform_id=transform_id,
            tenant_id=self.tenant_id,
            **extra,
        )
        return self

    def set_agent(self, engine_name: str, version: str) -> ProvenanceCapture:
        self._agent = ProvAgent.for_engine(engine_name, version)
        return self

    def build(self) -> ProvenanceRecord:
        if not self._source:
            raise ValueError("Source entity is required")
        if not self._activity:
            raise ValueError("Activity is required")
        if not self._output:
            raise ValueError("Output entity is required")

        return ProvenanceRecord(
            source_entity=self._source,
            activity=self._activity,
            output_entity=self._output,
            agent=self._agent,
            tenant_id=self.tenant_id,
        )

"""Pydantic models for pipeline configuration canonicalization."""

import hashlib
from typing import Any, Dict
from pydantic import BaseModel, Field

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    import json
    HAS_ORJSON = False


class PipelineFingerprint(BaseModel):
    """Canonical pipeline configuration for fingerprinting."""
    
    pipeline_id: str = Field(..., description="Unique identifier for the pipeline")
    version: str = Field(..., description="Pipeline version string")
    src: Dict[str, Any] = Field(..., description="Pipeline-level source configuration")
    dst: Dict[str, Any] = Field(..., description="Pipeline-level destination configuration")
    streams: Dict[str, Any] = Field(..., description="Multi-stream configurations")
    
    def fingerprint(self) -> str:
        """Return a deterministic SHA256 fingerprint of the config subset."""
        data = self.model_dump(mode="json", exclude_unset=True)
        
        if HAS_ORJSON:
            raw = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)
        else:
            # Fallback to standard json with sorted keys
            raw = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
        
        return f"sha256:{hashlib.sha256(raw).hexdigest()}"
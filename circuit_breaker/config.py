"""Environment -> BreakerConfig (trip thresholds, local fallback RiskConfig, webhook)."""

from __future__ import annotations

import os
from typing import Optional, Set

from dotenv import load_dotenv
from followsm_sdk import RiskConfig
from pydantic import BaseModel, Field, model_validator

load_dotenv()


class BreakerConfig(BaseModel):
    followsm_api_key: Optional[str] = None
    symbols: Set[str] = Field(default_factory=set)

    vpin_percentile_trip: float = 0.90
    vpin_percentile_rearm: float = 0.80
    # Raw-VPIN fallback while the backend has not published vpin_percentile yet.
    vpin_trip: float = 0.85
    vpin_rearm: float = 0.75
    ob_toxicity_trip: float = 2.0
    imbalance_spike: float = 0.20
    imbalance_ewma_alpha: float = Field(default=0.10, gt=0, le=1)
    cooldown_secs: float = Field(default=5.0, ge=0)

    webhook_url: Optional[str] = None
    webhook_secret: Optional[str] = None
    callback_timeout_secs: float = Field(default=2.0, gt=0)

    risk: RiskConfig = Field(default_factory=RiskConfig)

    @model_validator(mode="after")
    def _check_hysteresis(self) -> "BreakerConfig":
        if self.vpin_rearm > self.vpin_trip or self.vpin_percentile_rearm > self.vpin_percentile_trip:
            raise ValueError("VPIN re-arm thresholds must be <= their trip thresholds")
        return self


def _env(name: str) -> Optional[str]:
    value = os.getenv(name, "").strip()
    return value or None


def load_config() -> BreakerConfig:
    return BreakerConfig(
        followsm_api_key=_env("FOLLOWSM_API_KEY"),
        symbols={s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()},
        vpin_percentile_trip=float(os.getenv("VPIN_PERCENTILE_TRIP", "0.90")),
        vpin_percentile_rearm=float(os.getenv("VPIN_PERCENTILE_REARM", "0.80")),
        vpin_trip=float(os.getenv("VPIN_TRIP", "0.85")),
        vpin_rearm=float(os.getenv("VPIN_REARM", "0.75")),
        ob_toxicity_trip=float(os.getenv("OB_TOXICITY_TRIP", "2.0")),
        imbalance_spike=float(os.getenv("IMBALANCE_SPIKE", "0.20")),
        cooldown_secs=float(os.getenv("COOLDOWN_SECS", "5")),
        webhook_url=_env("WEBHOOK_URL"),
        webhook_secret=_env("WEBHOOK_SECRET"),
        risk=RiskConfig(
            vpin_percentile_widen_threshold=float(os.getenv("VPIN_PERCENTILE_WIDEN_THRESHOLD", "0.90")),
            vpin_percentile_halt_threshold=float(os.getenv("VPIN_PERCENTILE_HALT_THRESHOLD", "0.95")),
            vpin_widen_threshold=float(os.getenv("VPIN_WIDEN_THRESHOLD", "0.80")),
            vpin_halt_threshold=float(os.getenv("VPIN_HALT_THRESHOLD", "0.90")),
            min_semantic_confidence=float(os.getenv("MIN_SEMANTIC_CONFIDENCE", "0.65")),
        ),
    )

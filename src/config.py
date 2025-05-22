#!/usr/bin/env python3

import logging

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import field_validator

logger = logging.getLogger(__name__)

class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""
    profile: str
    _max_heap_size_mb: int = 1024

    @field_validator("profile")
    @classmethod
    def profile_values(cls, value: str) -> str | None:
        """Check profile config option is one of `testing` or `production`."""
        if value not in ["testing", "production"]:
            raise ValueError("Value not one of 'testing' or 'production'")

        return value

#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Typed charm config with validation."""

import logging
from typing import Optional
from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import field_validator

logger = logging.getLogger(__name__)


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    profile: str
    cluster_name: Optional[str] = None

    @field_validator("profile")
    @classmethod
    def profile_values(cls, value: str) -> str | None:
        """Check profile config option is one of `testing` or `production`."""
        if value not in ["testing", "production"]:
            raise ValueError("Value not one of 'testing' or 'production'")

        return value    

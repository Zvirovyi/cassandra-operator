#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""
import re
import logging
import constants
import subprocess

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.operator_libs_linux.v2 import snap
from config import CharmConfig
from ops import (
    ActiveStatus,
    BlockedStatus,
    CharmBase,
    Framework,
    InstallEvent,
    MaintenanceStatus,
    StartEvent,
    main,
)

logger = logging.getLogger(__name__)

class CassandraOperatorCharm(TypedCharmBase[CharmConfig]):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, framework: Framework):
        super().__init__(framework)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.install, self._on_install)
        framework.observe(self.on.config_changed, self._on_config_changed)
        
    def _on_start(self, event: StartEvent) -> None:
        self._set_unit_workload_version()
        self._cassandra_snap().start(["daemon"], True)
        self._set_unit_status()

    def _on_install(self, event: InstallEvent) -> None:
        self.unit.status = MaintenanceStatus("Installing cassandra snap")
        logger.debug("Installing snap")
        snap.install_local("cassandra_5.0.4_amd64.snap", devmode=True, dangerous=True)
        cassandra = self._cassandra_snap()
        cassandra.connect("log-observe")
        cassandra.connect("mount-observe")
        cassandra.connect("process-control")
        cassandra.connect("system-observe")
        cassandra.connect("sys-fs-cgroup-service")
        cassandra.connect("shmem-perf-analyzer")
        logger.debug("Snap successfully installed")

        self._update_env_config()
        
        self._set_unit_status()

    def _set_unit_status(self) -> None:
        self.unit.status = ActiveStatus()

    def _cassandra_snap(self) -> snap.Snap:
        return snap.SnapCache()["cassandra"]

    def _set_unit_workload_version(self) -> None:
        client = snap.SnapClient()
        for snp in client.get_installed_snaps():
            if snp["name"] == "cassandra":
                self.unit.set_workload_version(str(snp["version"]))
                return

    def _on_config_changed(self, event) -> None:
        try:
            self._update_env_config()
        except Exception as e:
            self.unit.status = BlockedStatus("Configuration Error. Please check the logs")
            logger.error("Invalid configuration: %s", str(e))
            return

    def _update_env_config(self) -> bool:
        logger.debug("Updating env config")

        if self.config.profile == "testing":
            self._set_default_env_config_for_testing()

        elif self.config.profile == "production":
            self._set_default_env_config_for_production()
        
        return False
    
    def _set_default_env_config_for_testing(self) -> bool:
        self._swap_with_regex(
            path=constants.CAS_ENV_CONF_FILE,
            pattern=r'^[#\s]*MAX_HEAP_SIZE\s*=\s*".*"$',
            replacement=f'MAX_HEAP_SIZE="{self.config._max_heap_size_mb}M"'
        )
        self._swap_with_regex(
            path=constants.CAS_ENV_CONF_FILE,
            pattern=r'^[#\s]*HEAP_NEWSIZE\s*=\s*".*"$',
            replacement=f'HEAP_NEWSIZE="{self.config._max_heap_size_mb // 2}M"'
        )
        return True
        
    def _set_default_env_config_for_production(self) -> bool:
        # Comment out memory options
        self._swap_with_regex(
            path=constants.CAS_ENV_CONF_FILE,
            pattern=r'^[#\s]*MAX_HEAP_SIZE\s*=\s*".*"$',
            replacement=f'#MAX_HEAP_SIZE="{self.config._max_heap_size_mb}M"'
        )
        self._swap_with_regex(
            path=constants.CAS_ENV_CONF_FILE,
            pattern=r'^[#\s]*HEAP_NEWSIZE\s*=\s*".*"$',
            replacement=f'#HEAP_NEWSIZE="{self.config._max_heap_size_mb // 2}M"'
        )
        return True

    def _swap_with_regex(self, path: str, pattern: str, replacement: str) -> None:
        with open(path, 'r') as f:
            lines = f.readlines()
    
        with open(path, 'w') as f:
            for line in lines:
                new_line = re.sub(pattern, replacement, line)
                f.write(new_line)
                return


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)

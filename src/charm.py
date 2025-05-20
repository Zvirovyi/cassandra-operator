#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
import constants
import subprocess

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.operator_libs_linux.v2 import snap
from config import CharmConfig
from ops import (
    ActiveStatus,
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

        if self.config.profile == "testing":
           self.update_env_config(max_heap_size_mb=1024)
        
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

    def update_env_config(self, max_heap_size_mb: int | None = None) -> bool:
        logger.debug("Updating env config")
        if max_heap_size_mb is not None:
            if max_heap_size_mb <= 0:
                raise ValueError("MAX_HEAP_SIZE can not be <= 0")

            self._swap_with_sed(
                path=constants.CAS_ENV_CONF_FILE,
                oldstr='#MAX_HEAP_SIZE="20G"',
                newstr=f'MAX_HEAP_SIZE="{max_heap_size_mb}M"'
            )
            self._swap_with_sed(
                path=constants.CAS_ENV_CONF_FILE,
                oldstr='#HEAP_NEWSIZE="10G"',
                newstr=f'HEAP_NEWSIZE="{max_heap_size_mb // 2}M"'
            )
            return True

        return False

    def _swap_with_sed(self, path: str, oldstr: str, newstr: str) -> bool:
        try:
            subprocess.run(
                ["sed", "-i", f"s/{oldstr}/{newstr}/g", path],
                check=True
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"sed failed: {e}")
            return False        


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)

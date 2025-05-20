#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

from charms.operator_libs_linux.v2 import snap
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


class CassandraOperatorCharm(CharmBase):
    """Charm the application."""

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


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)

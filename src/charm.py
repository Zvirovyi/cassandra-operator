#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
import os

import requests_unixsocket
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
        """Handle start event."""
        self.unit.set_workload_version(self._get_workload_version())
        self._cassandra_snap().start(enable=True)
        self._set_unit_status()

    def _on_install(self, event: InstallEvent) -> None:
        """Handle install event."""
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

    def _get_workload_version(self) -> str:
        """Get the microsample workload version from the snapd API via unix-socket."""
        try:
            snap_name = "cassandra"
            snapd_url = f"http+unix://%2Frun%2Fsnapd.socket/v2/snaps/{snap_name}"
            session = requests_unixsocket.Session()
            response = session.get(snapd_url)
            if response.status_code == 200:
                data = response.json()
                return data["result"]["version"]
            else:
                logger.error(
                    f"Failed to retrieve snap apps for workload version. Status code: {response.status_code}"
                )
                return "unknown"
        except Exception as e:
            logger.error(f"Failed to get workload version: {e}")
            return "unknown"


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)

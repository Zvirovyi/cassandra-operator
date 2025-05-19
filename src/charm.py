#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
import os

import ops
import requests_unixsocket

logger = logging.getLogger(__name__)


class CassandraOperatorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.install, self._on_install)

    def _on_start(self, event: ops.StartEvent):
        """Handle start event."""
        self.unit.status = ops.ActiveStatus()

        try:
            version = self._get_workload_version()
            self.unit.set_workload_version(version)
        except Exception as e:
            logger.error(f"Failed to get workload version: {e}")
            self.unit.set_workload_version("unknown")

    def _on_install(self, event: ops.InstallEvent):
        """Handle install event."""
        self.unit.status = ops.MaintenanceStatus("Installing cassandra snap")
        logger.debug("---------- INSTALLING SNAP ----------")
        os.system("snap install cassandra_5.0.4_amd64.snap --devmode --dangerous")
        logger.debug("---------- SNAP INSTALLED ----------")
        self.unit.status = ops.ActiveStatus("Ready")

    def _get_workload_version(self):
        """Get the microsample workload version from the snapd API via unix-socket."""
        logger.debug("---------- GETTING WORKLOAD VERSION ----------")
        snap_name = "cassandra"
        snapd_url = f"http+unix://%2Frun%2Fsnapd.socket/v2/snaps/{snap_name}"
        session = requests_unixsocket.Session()
        # Use the requests library to send a GET request over the Unix domain socket
        response = session.get(snapd_url)
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"---------- SOCKET RESONSE: {data} ----------")
            workload_version = data["result"]["version"]
        else:
            workload_version = "unknown"
            print(f"Failed to retrieve Snap apps. Status code: {response.status_code}")

        # Return the workload version
        return workload_version


if __name__ == "__main__":  # pragma: nocover
    ops.main(CassandraOperatorCharm)

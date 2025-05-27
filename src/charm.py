#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
import os
import re
import yaml
from typing import cast

from ops import (
    ActiveStatus,
    BlockedStatus,
    ConfigChangedEvent,
    Framework,
    InstallEvent,
    MaintenanceStatus,
    RelationDataContent,
    StartEvent,
    UpdateStatusEvent,
    WaitingStatus,
    main,
)
from pydantic import ValidationError

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.operator_libs_linux.v2 import snap
from config import CharmConfig
from constants import CAS_CONF_FILE, CAS_ENV_CONF_FILE, MGMT_API_DIR, PEER_RELATION
from data_model import AppPeerData, UnitPeerData

logger = logging.getLogger(__name__)


class CassandraOperatorCharm(TypedCharmBase[CharmConfig]):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, framework: Framework):
        super().__init__(framework)
        
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.install, self._on_install)
        framework.observe(self.on.update_status, self._on_update_status)
        framework.observe(self.on.config_changed, self._on_config_changed)

    def _on_start(self, event: StartEvent) -> None:
        self.unit.status = MaintenanceStatus("Starting Cassandra daemon")

        self._set_unit_workload_version()

        logger.debug("Initializing Cassandra config")
        if not self._update_cassandra_config():
            logger.debug(
                "Deferring start event (workload initialization) due to config validation error"
            )
            event.defer()
            self._set_unit_status()
            return

        logger.debug("Starting Cassandra management API daemon (initializing workload)")

        self._cassandra_snap().start(["mgmt-server"])
        self._unit_peer_data.update({"workload-initialized": "True"})        

        self._set_unit_status()

    def _on_install(self, _: InstallEvent) -> None:
        self.unit.status = MaintenanceStatus("Installing Cassandra snap")
        logger.debug("Installing & configuring Cassandra snap")
        snap.install_local("cassandra_5.0.4_amd64.snap", devmode=True, dangerous=True)
        cassandra = self._cassandra_snap()
        cassandra.connect("log-observe")
        cassandra.connect("mount-observe")
        cassandra.connect("process-control")
        cassandra.connect("system-observe")
        cassandra.connect("sys-fs-cgroup-service")
        cassandra.connect("shmem-perf-analyzer")
        self._set_unit_status()

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        self._set_unit_status()

    @property
    def _app_peer_data(self) -> AppPeerData:
        relation = self.model.get_relation(PEER_RELATION)
        if relation is None:
            return AppPeerData(cast(RelationDataContent, {}))
        return AppPeerData(relation.data[self.app])

    @property
    def _unit_peer_data(self) -> UnitPeerData:
        relation = self.model.get_relation(PEER_RELATION)
        if relation is None:
            return UnitPeerData(cast(RelationDataContent, {}))
        return UnitPeerData(relation.data[self.unit])


    def _set_unit_status(self) -> None:
        if self._unit_peer_data.get("bad-config") == "True":
            self.unit.status = BlockedStatus("Configuration error. Check logs")
            return
        if self._unit_peer_data.get("workload-initialized") == "False":
            self.unit.status = WaitingStatus("Waiting for workload initialization")
            return
        if not self._cassandra_snap().services["mgmt-server"]["active"]:
            self.unit.status = WaitingStatus("Service is not healthy. Restarting")
            return
        self.unit.status = ActiveStatus()

    def _cassandra_snap(self) -> snap.Snap:
        return snap.SnapCache()["cassandra"]

    def _set_unit_workload_version(self) -> None:
        client = snap.SnapClient()
        for snp in client.get_installed_snaps():
            if snp["name"] == "cassandra":
                self.unit.set_workload_version(str(snp["version"]))
                return

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        if self._unit_peer_data.get("workload-initialized") == "False":
            logger.debug("Deferring config changed event due to workload isn't initialized")
            event.defer()
            return

        logger.debug("Updating Cassandra config due to charm config change")
        if not self._update_cassandra_config():
            logger.debug("Deferring config changed event due to charm config validation error")
            event.defer()
            self._set_unit_status()
            return

        logger.debug("Restarting Cassandra daemon due to charm config change")
        self._cassandra_snap().restart(["mgmt-server"])

        self._set_unit_status()


    def _update_cassandra_config(self) -> bool:
        try:
            self.config
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            self._unit_peer_data.update({"bad-config":  "True"})
            return False

        self._unit_peer_data.update({"bad-config":  "False"})

        logger.debug("Updating Cassandra env config")
        self._render_cassandra_env_config(
            max_heap_size_mb=1024 if self.config.profile == "testing" else None,
            enable_mgmt_server=True
        )

        logger.debug("Updating Cassandra config")
        self._render_cassandra_config(
            cluster_name=self.config.cluster_name
        )
        return True

    def _render_cassandra_config(self, cluster_name: str | None) -> None:
        with open(CAS_CONF_FILE, 'r', encoding='utf-8') as f:
            current_data = yaml.safe_load(f) or {}

        if not isinstance(current_data, dict):
            raise ValueError("Current cassandra config file is not valid")


        if cluster_name is not None:
            current_data.update({"cluster_name": cluster_name})

        with open(CAS_CONF_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(current_data, f, allow_unicode=True, default_flow_style=False)
                
    def _render_cassandra_env_config(self, max_heap_size_mb: int | None, enable_mgmt_server: bool = True) -> None:
        self._swap_with_regex(
            path=CAS_ENV_CONF_FILE,
            pattern=r'^\s*#?MAX_HEAP_SIZE="[^"]*"$',
            replacement=f'MAX_HEAP_SIZE="{max_heap_size_mb}M"'
            if max_heap_size_mb
            else '#MAX_HEAP_SIZE=""',
            count=1
        )
        self._swap_with_regex(
            path=CAS_ENV_CONF_FILE,
            pattern=r'^\s*#?HEAP_NEWSIZE="[^"]*"$',
            replacement=f'HEAP_NEWSIZE="{max_heap_size_mb // 2}M"'
            if max_heap_size_mb
            else '#HEAP_NEWSIZE=""',
            count=1
        )

        if enable_mgmt_server:
            mgmtapi_agent_line = (
                f'JVM_OPTS="$JVM_OPTS -javaagent:{MGMT_API_DIR}/datastax-mgmtapi-agent.jar"'
            )
            with open(CAS_ENV_CONF_FILE, "r+", encoding="utf-8") as f:
                content = f.read()
                if mgmtapi_agent_line not in content:
                    f.seek(0, os.SEEK_END)  # переместиться в конец
                    if not content.endswith("\n"):
                        f.write("\n")
                    f.write(mgmtapi_agent_line + "\n")
                    logger.debug("---------- Wrote line to env ----------")
                
    def _swap_with_regex(self, path: str, pattern: str, replacement: str, count: int = 0) -> None:
        with open(path, 'r') as f:
            content = f.read()

        new_content, _ = re.subn(pattern, replacement, content, count, flags=re.MULTILINE)

        with open(path, 'w') as f:
            f.write(new_content)


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)

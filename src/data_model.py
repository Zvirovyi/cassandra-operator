import logging
from typing import Any
from ops import RelationDataContent

logger = logging.getLogger(__name__)

class UnitPeerData:
    def __init__(self, raw: RelationDataContent):
        self._raw = raw
        self._defaults = {
            "bad-config": "False",
            "workload-initialized": "False",
        }

        for key, value in self._defaults.items():
            self._raw.setdefault(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return self._raw.get(key, default)

    def update(self, new_data: dict[str, str]) -> None:
        logger.info(f"[UnitPeerData] Updating with: {new_data}")
        for key, value in new_data.items():
            self._raw[key] = value
        logger.info(f"[UnitPeerData] updated unit data: {dict(self._raw)}")

    def as_dict(self) -> dict[str, str]:
        return dict(self._raw)

class AppPeerData:

    def __init__(self, raw: RelationDataContent):
        self._raw = raw

    def get(self, key: str, default: Any = None) -> Any:
        return self._raw.get(key, default)

    def update(self, new_data: dict[str, str]) -> None:
        logger.info(f"[AppPeerData] Updating with: {new_data}")
        for key, value in new_data.items():
            self._raw[key] = value
        logger.info(f"[AppPeerData] updated app data: {dict(self._raw)}")

    def as_dict(self) -> dict[str, str]:
        return dict(self._raw)

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dash.dependencies import Output


class Refreshable(ABC):
    @property
    @abstractmethod
    def dash_dependencies(self) -> list[Output]:
        raise NotImplementedError

    @property
    @abstractmethod
    def dash_dependencies_outputs(self) -> list[Any]:
        raise NotImplementedError

"""BaseSink abstract class."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from observe.report import ObservabilityReport


class BaseSink(ABC):
    """Abstract base class for all sinks.

    Subclasses implement `write(report)`. The runner calls `_safe_write`
    so a sink failure never breaks the user's pipeline — it logs and
    moves on to the next sink.
    """

    @abstractmethod
    def write(self, report: ObservabilityReport) -> None:
        """Write the report somewhere."""

    def _safe_write(self, report: ObservabilityReport) -> None:
        try:
            self.write(report)
        except Exception as e:
            logging.getLogger(__name__).error(
                "Sink %s failed: %s: %s",
                self.__class__.__name__,
                type(e).__name__,
                e,
            )

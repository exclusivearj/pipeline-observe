"""SlackSink: posts Block Kit message to a Slack webhook."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Optional

from observe.report import CheckStatus, ObservabilityReport
from observe.sinks.base import BaseSink

_STATUS_EMOJI = {
    CheckStatus.PASS: "✅",
    CheckStatus.WARN: "⚠️",
    CheckStatus.FAIL: "❌",
    CheckStatus.ERROR: "🔥",
    CheckStatus.SKIP: "⏭️",
}


class SlackSink(BaseSink):
    def __init__(
        self,
        webhook_url: str,
        channel: Optional[str] = None,
        only_on_failure: bool = False,
        timeout: float = 5.0,
    ) -> None:
        if not webhook_url:
            raise ValueError("SlackSink: webhook_url is required")
        self.webhook_url = webhook_url
        self.channel = channel
        self.only_on_failure = only_on_failure
        self.timeout = timeout
        self._logger = logging.getLogger(__name__)

    def write(self, report: ObservabilityReport) -> None:
        status = report.overall_status
        if self.only_on_failure and status == CheckStatus.PASS:
            return

        payload = self._build_payload(report)
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            self._logger.error("SlackSink HTTP %s: %s", e.code, e.reason)
        except urllib.error.URLError as e:
            self._logger.error("SlackSink URL error: %s", e.reason)

    def _build_payload(self, report: ObservabilityReport) -> dict:
        status = report.overall_status
        emoji = _STATUS_EMOJI.get(status, "•")
        header_text = (
            f"{emoji} {report.pipeline_name} / {report.table_name} — {status.value.upper()}"
        )

        blocks: list[dict] = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header_text[:150]},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run ID:*\n`{report.run_id}`"},
                    {"type": "mrkdwn", "text": f"*Evaluated:*\n{report.evaluated_at.isoformat()}"},
                    {"type": "mrkdwn", "text": f"*Rows:*\n{report.row_count:,}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{report.duration_ms:.1f}ms"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```\n{report.to_markdown_table()}\n```",
                },
            },
        ]
        payload: dict = {"blocks": blocks, "text": header_text}
        if self.channel:
            payload["channel"] = self.channel
        return payload

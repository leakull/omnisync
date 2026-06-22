import contextlib
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.events.schemas import NormalizedEventCreate
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector
from src.jira.config import jira_settings
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.jira")

REQUEST_TIMEOUT = 30


class JiraAPIError(Exception):
    pass


class JiraClient:
    """Reads issues from Jira Cloud REST API v3 with JQL incremental filtering."""

    def __init__(self, base_url: str, email: str, api_token: str, page_size: int = 50) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)
        self.page_size = page_size

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    async def _search(self, jql: str, start_at: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.base_url}/rest/api/3/search",
                params={
                    "jql": jql,
                    "startAt": start_at,
                    "maxResults": self.page_size,
                    "fields": "summary,description,status,reporter,updated,issuetype",
                },
                auth=self.auth,
                headers={"Accept": "application/json"},
            )
            if response.status_code == 429:
                raise JiraAPIError("Jira rate limit (HTTP 429)")
            if response.status_code >= 400:
                raise JiraAPIError(f"HTTP {response.status_code}: {response.text[:200]}")
            return cast(dict[str, Any], response.json())

    async def search_issues(
        self, project: str = "", since: datetime | None = None
    ) -> list[dict[str, Any]]:
        clauses = []
        if project:
            clauses.append(f'project = "{project}"')
        if since:
            # Jira JQL expects "yyyy/MM/dd HH:mm" in the instance timezone.
            clauses.append(f'updated >= "{since.strftime("%Y/%m/%d %H:%M")}"')
        jql = " AND ".join(clauses) + " ORDER BY updated ASC" if clauses else "ORDER BY updated ASC"

        issues: list[dict[str, Any]] = []
        start_at = 0
        with tracer.start_as_current_span("jira.search_issues") as span:
            span.set_attribute("jira.project", project)
            while True:
                data = await self._search(jql, start_at)
                batch = data.get("issues", [])
                issues.extend(batch)
                total = data.get("total", 0)
                start_at += len(batch)
                if not batch or start_at >= total:
                    break
            span.set_attribute("jira.issue_count", len(issues))
        logger.info("jira_issues_fetched", count=len(issues), project=project)
        return issues


@register_connector
class JiraConnector(BaseConnector):
    source = "jira"

    def __init__(
        self,
        base_url: str = "",
        email: str = "",
        api_token: str = "",
        project: str = "",
        page_size: int = 0,
    ) -> None:
        base_url = base_url or jira_settings.JIRA_BASE_URL
        if not base_url:
            raise ValueError("Jira base URL must be configured (JIRA_BASE_URL)")
        self.project = project or jira_settings.JIRA_PROJECT
        self.client = JiraClient(
            base_url=base_url,
            email=email or jira_settings.JIRA_EMAIL,
            api_token=api_token or jira_settings.JIRA_API_TOKEN,
            page_size=page_size or jira_settings.JIRA_PAGE_SIZE,
        )

    async def fetch(self, since: datetime | None = None) -> list[dict[str, Any]]:
        return await self.client.search_issues(self.project, since)

    @staticmethod
    def _extract_text(description: Any) -> str:
        """Flatten Atlassian Document Format (ADF) description to plain text."""
        if isinstance(description, str):
            return description
        if not isinstance(description, dict):
            return ""
        parts: list[str] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                if node.get("type") == "text" and "text" in node:
                    parts.append(node["text"])
                for child in node.get("content", []):
                    walk(child)
            elif isinstance(node, list):
                for child in node:
                    walk(child)

        walk(description)
        return " ".join(parts)

    def normalize(
        self, raw: dict[str, Any], raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        key = raw.get("key")
        if not key:
            return None
        fields = raw.get("fields", {})
        summary = fields.get("summary", "")
        description = self._extract_text(fields.get("description"))
        status = (fields.get("status") or {}).get("name", "")
        reporter = fields.get("reporter") or {}
        author_id = reporter.get("accountId", "") or reporter.get("emailAddress", "")
        author_name = reporter.get("displayName", "") or author_id

        timestamp = datetime.now(UTC)
        updated = fields.get("updated")
        if updated:
            with contextlib.suppress(ValueError, TypeError):
                timestamp = datetime.fromisoformat(updated.replace("Z", "+00:00"))

        content = f"[{status}] {key}: {summary}"
        if description:
            content += f"\n\n{description}"

        return NormalizedEventCreate(
            external_id=f"jira-{key}",
            source="jira",
            author_id=author_id,
            author_name=author_name,
            content=content,
            event_type="issue",
            timestamp=timestamp,
            raw_payload_id=raw_payload_id,
        )

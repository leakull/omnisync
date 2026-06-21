import asyncio
import time
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.github.config import github_settings
from src.github.constants import (
    RATE_LIMIT_BUFFER,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
)
from src.github.exceptions import GitHubAPIError, GitHubRateLimitError
from src.github.schemas import GitHubCommitData, GitHubPRData
from src.github.utils import parse_commit_to_event, parse_pr_to_event
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.github")


class GitHubClient:
    def __init__(self):
        self.base_url = github_settings.GITHUB_API_BASE
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if github_settings.GITHUB_TOKEN:
            self.headers["Authorization"] = f"Bearer {github_settings.GITHUB_TOKEN}"
        self._last_request_time = 0.0
        self._min_interval = 1.0
        self._throttle_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self.headers,
                timeout=REQUEST_TIMEOUT,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    )
    async def _request(self, endpoint: str, params: dict | None = None) -> Any:
        # Serialize the client-side throttle so concurrent coroutines in the
        # same process don't all fire at once and race on the timestamp.
        async with self._throttle_lock:
            wait_time = self._min_interval - (time.time() - self._last_request_time)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request_time = time.time()

        client = self._get_client()
        response = await client.get(endpoint, params=params)

        remaining = response.headers.get("x-ratelimit-remaining")
        if remaining and int(remaining) < RATE_LIMIT_BUFFER:
            reset_time = int(response.headers.get("x-ratelimit-reset", 0))
            sleep_seconds = max(reset_time - int(time.time()), 1)
            logger.warning(
                "github_rate_limit_low", remaining=remaining, sleep_seconds=sleep_seconds
            )
            await asyncio.sleep(sleep_seconds)

        if response.status_code == 403:
            raise GitHubRateLimitError()
        if response.status_code >= 400:
            raise GitHubAPIError(f"HTTP {response.status_code}: {response.text[:200]}")

        return response.json()

    async def get_commits(
        self, owner: str, repo: str, since: str | None = None
    ) -> list[GitHubCommitData]:
        with tracer.start_as_current_span("github.get_commits") as span:
            span.set_attribute("github.owner", owner)
            span.set_attribute("github.repo", repo)
            params: dict[str, Any] = {"per_page": 100}
            if since:
                params["since"] = since
            data = await self._request(f"/repos/{owner}/{repo}/commits", params)
            span.set_attribute("github.commits_count", len(data))
            return [GitHubCommitData.model_validate(c) for c in data]

    async def get_pull_requests(
        self, owner: str, repo: str, state: str = "all", since: str | None = None
    ) -> list[GitHubPRData]:
        with tracer.start_as_current_span("github.get_pull_requests") as span:
            span.set_attribute("github.owner", owner)
            span.set_attribute("github.repo", repo)
            params = {"state": state, "per_page": 100, "sort": "updated", "direction": "desc"}
            data = await self._request(f"/repos/{owner}/{repo}/pulls", params)
            # The PR list endpoint has no "since" filter, so trim client-side by
            # updated_at. Results are sorted desc, so we can stop at the first
            # PR older than the watermark.
            if since:
                trimmed = []
                for pr in data:
                    updated = pr.get("updated_at") or pr.get("created_at")
                    if updated and updated < since:
                        break
                    trimmed.append(pr)
                data = trimmed
            span.set_attribute("github.prs_count", len(data))
            return [GitHubPRData.model_validate(pr) for pr in data]


github_client = GitHubClient()


@register_connector
class GitHubConnector(BaseConnector):
    source = "github"

    def __init__(self, owner: str = "", repo: str = ""):
        self.owner = owner
        self.repo = repo
        self.client = github_client

    async def fetch(self, since: datetime | None = None) -> list[dict[str, Any]]:
        since_iso = since.isoformat() if since else None
        commits = await self.client.get_commits(self.owner, self.repo, since=since_iso)
        prs = await self.client.get_pull_requests(self.owner, self.repo, since=since_iso)
        return [{"type": "commit", "data": c} for c in commits] + [
            {"type": "pr", "data": p} for p in prs
        ]

    def normalize(self, raw: dict[str, Any], raw_payload_id: UUID | None = None) -> Any:
        item_type = raw.get("type")
        data: Any = raw.get("data")
        if item_type == "commit":
            return parse_commit_to_event(
                data, f"{self.owner}/{self.repo}", str(raw_payload_id) if raw_payload_id else None
            )
        elif item_type == "pr":
            return parse_pr_to_event(
                data, f"{self.owner}/{self.repo}", str(raw_payload_id) if raw_payload_id else None
            )
        return None

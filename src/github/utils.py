import contextlib
from datetime import UTC, datetime
from uuid import UUID

from src.events.schemas import NormalizedEventCreate
from src.github.schemas import GitHubCommitData, GitHubPRData


def _as_uuid(raw_payload_id: str | UUID | None) -> UUID | None:
    return UUID(raw_payload_id) if isinstance(raw_payload_id, str) else raw_payload_id


def parse_commit_to_event(
    commit: GitHubCommitData,
    repo_full_name: str,
    raw_payload_id: str | UUID | None = None,
) -> NormalizedEventCreate:
    author_name = ""
    author_id = ""
    timestamp = datetime.now(UTC)

    if commit.commit.author:
        author_name = commit.commit.author.name or ""
        author_id = commit.commit.author.email or ""
        if commit.commit.author.date:
            with contextlib.suppress(ValueError, TypeError):
                timestamp = datetime.fromisoformat(commit.commit.author.date.replace("Z", "+00:00"))

    return NormalizedEventCreate(
        external_id=commit.sha,
        source="github",
        author_id=author_id,
        author_name=author_name,
        content=commit.commit.message,
        event_type="commit",
        timestamp=timestamp,
        raw_payload_id=_as_uuid(raw_payload_id),
    )


def parse_pr_to_event(
    pr: GitHubPRData,
    repo_full_name: str,
    raw_payload_id: str | UUID | None = None,
) -> NormalizedEventCreate:
    author_name = ""
    author_id = ""
    if pr.user:
        author_name = pr.user.login
        author_id = pr.user.login

    timestamp = datetime.now(UTC)
    if pr.created_at:
        with contextlib.suppress(ValueError, TypeError):
            timestamp = datetime.fromisoformat(pr.created_at.replace("Z", "+00:00"))

    content = f"PR #{pr.number}: {pr.title}"
    if pr.body:
        content += f"\n\n{pr.body}"

    return NormalizedEventCreate(
        external_id=str(pr.id),
        source="github",
        author_id=author_id,
        author_name=author_name,
        content=content,
        event_type="pull_request",
        timestamp=timestamp,
        raw_payload_id=_as_uuid(raw_payload_id),
    )

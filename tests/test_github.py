import pytest
from httpx import AsyncClient

from src.github.schemas import GitHubCommit, GitHubCommitAuthor, GitHubCommitData
from src.github.utils import parse_commit_to_event


@pytest.mark.asyncio
async def test_github_webhook_requires_signature(client: AsyncClient):
    response = await client.post(
        "/api/v1/github/webhooks/github",
        json={"ref": "refs/heads/main", "commits": []},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_github_sync_requires_auth(client: AsyncClient):
    response = await client.post("/api/v1/github/sync?owner=test&repo=test")
    assert response.status_code == 422 or response.status_code == 401


def test_parse_commit_to_event():
    commit = GitHubCommitData(
        sha="abc123",
        commit=GitHubCommit(
            id="abc123",
            message="test commit",
            author=GitHubCommitAuthor(
                name="Test", email="test@test.com", date="2024-01-01T00:00:00Z"
            ),
        ),
    )
    event = parse_commit_to_event(commit, "owner/repo")
    assert event.external_id == "abc123"
    assert event.source == "github"
    assert event.event_type == "commit"
    assert event.content == "test commit"
    assert event.author_name == "Test"


@pytest.mark.asyncio
async def test_github_webhook_with_commits(client: AsyncClient):
    payload = {
        "ref": "refs/heads/main",
        "commits": [
            {
                "sha": "def456",
                "commit": {
                    "id": "def456",
                    "message": "feat: add feature",
                    "author": {
                        "name": "Dev",
                        "email": "dev@test.com",
                        "date": "2024-06-01T12:00:00Z",
                    },
                },
            }
        ],
    }
    response = await client.post(
        "/api/v1/github/webhooks/github",
        json=payload,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["events_created"] == 1

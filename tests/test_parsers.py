"""Unit coverage for pure parsing/normalization logic across connectors:
GitHub commit/PR parsing, Jira (ADF flattening, issue normalization, paginated
search) and IMAP message parsing — none of which need external services.
"""

import email
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from src.github.schemas import GitHubCommitData, GitHubPRData
from src.github.utils import _as_uuid, parse_commit_to_event, parse_pr_to_event


# ---------------------------------------------------------------------------
# GitHub utils
# ---------------------------------------------------------------------------
def test_as_uuid_variants():
    u = uuid4()
    assert _as_uuid(u) is u
    assert _as_uuid(str(u)) == u
    assert _as_uuid(None) is None


def test_parse_commit_with_author_and_date():
    commit = GitHubCommitData.model_validate(
        {
            "sha": "abc123",
            "commit": {
                "id": "abc123",
                "message": "Initial commit",
                "author": {
                    "name": "Ada",
                    "email": "ada@x.dev",
                    "date": "2026-06-20T10:00:00Z",
                },
            },
        }
    )
    pid = uuid4()
    ev = parse_commit_to_event(commit, "acme/widgets", str(pid))
    assert ev.external_id == "abc123"
    assert ev.source == "github"
    assert ev.event_type == "commit"
    assert ev.author_name == "Ada"
    assert ev.author_id == "ada@x.dev"
    assert ev.content == "Initial commit"
    assert ev.timestamp == datetime(2026, 6, 20, 10, 0, tzinfo=UTC)
    assert ev.raw_payload_id == pid


def test_parse_commit_without_author_defaults_to_now():
    commit = GitHubCommitData.model_validate(
        {"sha": "s1", "commit": {"id": "s1", "message": "msg"}}
    )
    ev = parse_commit_to_event(commit, "acme/widgets")
    assert ev.author_name == ""
    assert ev.author_id == ""
    assert ev.raw_payload_id is None
    assert ev.timestamp.tzinfo is not None  # fell back to datetime.now(UTC)


def test_parse_commit_bad_date_is_suppressed():
    commit = GitHubCommitData.model_validate(
        {
            "sha": "s2",
            "commit": {
                "id": "s2",
                "message": "msg",
                "author": {"name": "X", "email": "x@x", "date": "not-a-date"},
            },
        }
    )
    ev = parse_commit_to_event(commit, "r")
    assert ev.timestamp.tzinfo is not None  # no crash, defaulted


def test_parse_pr_with_body_and_user():
    pr = GitHubPRData.model_validate(
        {
            "id": 42,
            "number": 7,
            "title": "Add feature",
            "body": "Long description",
            "state": "open",
            "user": {"login": "octocat", "id": 1},
            "created_at": "2026-06-19T09:00:00Z",
        }
    )
    ev = parse_pr_to_event(pr, "acme/widgets")
    assert ev.external_id == "42"
    assert ev.event_type == "pull_request"
    assert ev.author_name == "octocat"
    assert ev.author_id == "octocat"
    assert ev.content == "PR #7: Add feature\n\nLong description"
    assert ev.timestamp == datetime(2026, 6, 19, 9, 0, tzinfo=UTC)


def test_parse_pr_without_body_or_user():
    pr = GitHubPRData.model_validate({"id": 5, "number": 1, "title": "Tiny", "state": "closed"})
    ev = parse_pr_to_event(pr, "r")
    assert ev.content == "PR #1: Tiny"
    assert ev.author_name == ""
    assert ev.timestamp.tzinfo is not None


# ---------------------------------------------------------------------------
# Jira service: ADF flattening + normalization
# ---------------------------------------------------------------------------
def test_jira_extract_text_variants():
    from src.jira.service import JiraConnector

    assert JiraConnector._extract_text("plain string") == "plain string"
    assert JiraConnector._extract_text(None) == ""
    assert JiraConnector._extract_text(123) == ""

    adf = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Hello"}]},
            {"type": "paragraph", "content": [{"type": "text", "text": "World"}]},
        ],
    }
    assert JiraConnector._extract_text(adf) == "Hello World"


def _jira_connector(monkeypatch):
    from src.jira.service import JiraConnector

    # Construct without touching live Jira config.
    monkeypatch.setattr(JiraConnector, "__init__", lambda self: None)
    c = JiraConnector()
    c.project = "ENG"
    c.client = None
    return c


def test_jira_normalize_full_issue(monkeypatch):
    c = _jira_connector(monkeypatch)
    raw = {
        "key": "ENG-9",
        "fields": {
            "summary": "Crash on login",
            "description": {
                "type": "doc",
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": "stack"}]}],
            },
            "status": {"name": "Done"},
            "reporter": {"accountId": "acc-1", "displayName": "Grace"},
            "updated": "2026-06-18T12:00:00.000+0000",
        },
    }
    ev = c.normalize(raw, None)
    assert ev.external_id == "jira-ENG-9"
    assert ev.event_type == "issue"
    assert ev.author_id == "acc-1"
    assert ev.author_name == "Grace"
    assert ev.content == "[Done] ENG-9: Crash on login\n\nstack"
    assert ev.timestamp == datetime(2026, 6, 18, 12, 0, tzinfo=UTC)


def test_jira_normalize_without_key_returns_none(monkeypatch):
    c = _jira_connector(monkeypatch)
    assert c.normalize({"fields": {}}, None) is None


def test_jira_normalize_reporter_email_and_no_description(monkeypatch):
    c = _jira_connector(monkeypatch)
    raw = {
        "key": "ENG-1",
        "fields": {
            "summary": "S",
            "status": {"name": "To Do"},
            "reporter": {"emailAddress": "r@x.dev"},
        },
    }
    ev = c.normalize(raw, None)
    assert ev.author_id == "r@x.dev"
    assert ev.author_name == "r@x.dev"  # falls back to author_id
    assert ev.content == "[To Do] ENG-1: S"  # no description appended


@pytest.mark.asyncio
async def test_jira_search_issues_paginates(monkeypatch):
    from src.jira.service import JiraClient

    async def fake_search(self, jql, start_at):
        if start_at == 0:
            return {"issues": [{"key": "A-1"}], "total": 2}
        return {"issues": [{"key": "A-2"}], "total": 2}

    monkeypatch.setattr(JiraClient, "_search", fake_search)
    client = JiraClient("https://x.atlassian.net", "e@x", "token", page_size=1)
    issues = await client.search_issues(project="ENG", since=datetime(2026, 1, 1, tzinfo=UTC))
    assert [i["key"] for i in issues] == ["A-1", "A-2"]


# ---------------------------------------------------------------------------
# IMAP message parsing
# ---------------------------------------------------------------------------
def _imap_client():
    from src.imap.service import IMAPClient

    return IMAPClient("imap.acme.dev", 993, "bot", "pw")


def test_imap_decode_header():
    client = _imap_client()
    assert client._decode_header("") == ""
    assert client._decode_header("=?utf-8?b?SGVsbG8=?=") == "Hello"
    assert client._decode_header("Plain Subject") == "Plain Subject"


def test_imap_parse_simple_message():
    client = _imap_client()
    raw = (
        "Subject: Server down\r\n"
        "From: ops@acme.dev\r\n"
        "Date: Wed, 20 Jun 2026 10:00:00 +0000\r\n"
        "Message-ID: <m1@acme.dev>\r\n"
        "\r\n"
        "Disk full on db-1"
    )
    msg = email.message_from_string(raw)
    parsed = client._parse_message(msg, b"7", "INBOX", "9")
    assert parsed["subject"] == "Server down"
    assert parsed["sender"] == "ops@acme.dev"
    assert parsed["message_id"] == "<m1@acme.dev>"
    assert parsed["uid"] == "7"
    assert parsed["folder"] == "INBOX"
    assert parsed["uidvalidity"] == "9"
    assert "Disk full on db-1" in parsed["body"]
    assert isinstance(parsed["date"], datetime)


def test_imap_parse_multipart_extracts_plain_text():
    client = _imap_client()
    raw = (
        "Subject: Multi\r\n"
        "From: a@b.dev\r\n"
        'Content-Type: multipart/alternative; boundary="BND"\r\n'
        "\r\n"
        "--BND\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "plain body\r\n"
        "--BND\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "\r\n"
        "<p>html body</p>\r\n"
        "--BND--\r\n"
    )
    msg = email.message_from_string(raw)
    parsed = client._parse_message(msg, b"3", "INBOX", None)
    assert "plain body" in parsed["body"]
    assert "html" not in parsed["body"]


def test_imap_connector_normalize_skips_empty(monkeypatch):
    from src.imap.service import IMAPConnector

    connector = IMAPConnector(host="h", username="u", password="p")
    assert connector.normalize({"subject": "", "body": ""}) is None

    pid = uuid4()
    ev = connector.normalize(
        {
            "subject": "S",
            "body": "B",
            "message_id": "<mid@x>",
            "host": "h",
            "folder": "INBOX",
            "uidvalidity": "1",
            "uid": "2",
            "sender": "x@x.dev",
            "date": datetime(2026, 6, 22, tzinfo=UTC),
        },
        pid,
    )
    assert ev.external_id == "imap-mid-<mid@x>"
    assert ev.event_type == "email"
    assert ev.author_id == "x@x.dev"
    assert ev.content.startswith("Subject: S")
    assert isinstance(ev.raw_payload_id, UUID)

from pydantic import BaseModel


class GitHubCommitAuthor(BaseModel):
    name: str | None = None
    email: str | None = None
    date: str | None = None


class GitHubCommit(BaseModel):
    id: str
    message: str
    author: GitHubCommitAuthor | None = None
    url: str | None = None


class GitHubCommitData(BaseModel):
    sha: str
    commit: GitHubCommit
    html_url: str | None = None
    author: dict | None = None


class GitHubPRUser(BaseModel):
    login: str
    id: int
    html_url: str | None = None


class GitHubPRData(BaseModel):
    id: int
    number: int
    title: str
    body: str | None = None
    state: str
    user: GitHubPRUser | None = None
    html_url: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    merged_at: str | None = None


class GitHubWebhookPayload(BaseModel):
    ref: str | None = None
    commits: list[GitHubCommitData] | None = None
    pull_request: GitHubPRData | None = None
    action: str | None = None
    repository: dict | None = None
    sender: dict | None = None

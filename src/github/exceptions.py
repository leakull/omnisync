from src.exceptions import ExternalAPIError


class GitHubAPIError(ExternalAPIError):
    def __init__(self, detail: str = "GitHub API error"):
        super().__init__(detail=detail)


class GitHubRateLimitError(ExternalAPIError):
    def __init__(self, detail: str = "GitHub rate limit exceeded"):
        super().__init__(detail=detail)


class GitHubWebhookError(ExternalAPIError):
    def __init__(self, detail: str = "Invalid webhook payload"):
        super().__init__(detail=detail)

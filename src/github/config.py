from pydantic_settings import BaseSettings


class GitHubSettings(BaseSettings):
    GITHUB_TOKEN: str = ""
    GITHUB_API_BASE: str = "https://api.github.com"
    GITHUB_WEBHOOK_SECRET: str = ""

    model_config = {"env_file": ".env"}


github_settings = GitHubSettings()

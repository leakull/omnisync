from pydantic_settings import BaseSettings


class JiraSettings(BaseSettings):
    JIRA_BASE_URL: str = ""
    JIRA_EMAIL: str = ""
    JIRA_API_TOKEN: str = ""
    JIRA_PROJECT: str = ""
    JIRA_PAGE_SIZE: int = 50

    model_config = {"env_file": ".env"}


jira_settings = JiraSettings()

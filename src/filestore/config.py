from pydantic_settings import BaseSettings


class FileStoreSettings(BaseSettings):
    FILESTORE_ENDPOINT_URL: str = ""
    FILESTORE_ACCESS_KEY: str = ""
    FILESTORE_SECRET_KEY: str = ""
    FILESTORE_BUCKET: str = ""
    FILESTORE_PREFIX: str = ""
    FILESTORE_REGION: str = "us-east-1"

    model_config = {"env_file": ".env"}


filestore_settings = FileStoreSettings()

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from uuid import UUID

from pydantic import AnyHttpUrl, Field, TypeAdapter, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppMode(StrEnum):
    WEB = "web"
    WORKER = "worker"
    ALL = "all"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_mode: AppMode = Field(default=AppMode.WEB, alias="APP_MODE")
    database_url: str = Field(alias="DATABASE_URL", repr=False)
    integration_id: UUID = Field(alias="INTEGRATION_ID")

    zzap_base_url: str = Field(alias="ZZAP_BASE_URL")
    zzap_api_key: str = Field(alias="ZZAP_API_KEY", repr=False)

    chatwoot_base_url: str = Field(alias="CHATWOOT_BASE_URL")
    chatwoot_account_id: int = Field(alias="CHATWOOT_ACCOUNT_ID")
    chatwoot_inbox_id: int = Field(alias="CHATWOOT_INBOX_ID")
    chatwoot_api_token: str = Field(alias="CHATWOOT_API_TOKEN", repr=False)
    chatwoot_webhook_secret: str = Field(alias="CHATWOOT_WEBHOOK_SECRET", repr=False)

    max_attachment_bytes: int = Field(default=10 * 1024 * 1024, alias="MAX_ATTACHMENT_BYTES")
    successful_message_retention_days: int = Field(
        default=60,
        alias="SUCCESSFUL_MESSAGE_RETENTION_DAYS",
    )
    failed_record_retention_days: int = Field(default=30, alias="FAILED_RECORD_RETENTION_DAYS")
    webhook_delivery_retention_days: int = Field(
        default=30,
        alias="WEBHOOK_DELIVERY_RETENTION_DAYS",
    )

    zzap_regular_timeout_seconds: float = Field(default=30.0, alias="ZZAP_TIMEOUT_SECONDS")
    chatwoot_regular_timeout_seconds: float = Field(default=30.0, alias="CHATWOOT_TIMEOUT_SECONDS")
    attachment_timeout_seconds: float = Field(default=60.0, alias="ATTACHMENT_TIMEOUT_SECONDS")

    @field_validator("zzap_base_url", "chatwoot_base_url", mode="before")
    @classmethod
    def _validate_http_url(cls, value: object) -> str:
        return str(TypeAdapter(AnyHttpUrl).validate_python(value))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

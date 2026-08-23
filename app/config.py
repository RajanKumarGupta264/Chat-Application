import os
from pathlib import Path
import uuid
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"


class Settings(BaseSettings):
    """Application settings with environment variable management."""

    # Server configuration
    APP_NAME: str = "Distributed Chat Engine"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    WORKER_ID: str = os.getenv("WORKER_ID", f"stranger-{uuid.uuid4().hex[:6]}")
    LOG_LEVEL: str = "INFO"

    # Redis Distributed Backplane configuration
    REDIS_URL: str = "redis://127.0.0.1:6379/0"
    CHANNEL_PREFIX: str = "chat:rooms:"
    PRESENCE_PREFIX: str = "chat:presence:"
    RECONNECT_BACKOFF_BASE: float = 1.0
    MAX_RECONNECT_DELAY: float = 30.0

    # WebSocket & Heartbeat settings
    PING_INTERVAL_SECONDS: int = 25
    PING_TIMEOUT_SECONDS: int = 10
    MAX_MESSAGE_LENGTH: int = 5000
    MAX_ROOM_MEMBERS: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Return cached instance of application settings."""
    return Settings()


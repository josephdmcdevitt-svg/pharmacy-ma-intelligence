from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://pharmacy:changeme123@db:5432/pharmacy_intel"
    DATABASE_URL_SYNC: str = "postgresql://pharmacy:changeme123@db:5432/pharmacy_intel"
    SECRET_KEY: str = "devsecretkey_change_in_production"
    DATA_DIR: str = "/app/data"
    ADMIN_EMAIL: str = "admin@pharma.local"
    ADMIN_PASSWORD: str = "admin123"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()

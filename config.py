import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    DB_USERNAME: str | None = os.getenv("DB_USERNAME")
    DB_PASSWORD: str | None = os.getenv("DB_PASSWORD")
    DB_HOST: str = "host.docker.internal" if os.path.exists("/.dockerenv") else os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", 3306))  # 转为 int
    DATABASE: str | None = os.getenv("DATABASE")
    AUTH_SECRET: str | None = os.getenv("AUTH_SECRET")

    DEBUG: bool = os.getenv("DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}
    PORT: int = int(os.getenv("PORT", 5000))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    MODEL_NAME: str = os.getenv("MODEL_NAME", "deepseek-chat")

    DB_CONNECT_TIMEOUT_SECONDS: int = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", 10))
    QUESTION_MAX_CHARS: int = int(os.getenv("QUESTION_MAX_CHARS", 1000))
    EXPORT_ESTIMATED_ROW_LIMIT: int = int(os.getenv("EXPORT_ESTIMATED_ROW_LIMIT", 200000))

    def validate(self) -> None:
        missing: list[str] = []
        if not self.DB_USERNAME:
            missing.append("DB_USERNAME")
        if not self.DB_PASSWORD:
            missing.append("DB_PASSWORD")
        if not self.DATABASE:
            missing.append("DATABASE")
        if not self.AUTH_SECRET:
            missing.append("AUTH_SECRET")
        if missing:
            raise RuntimeError(f"缺少环境变量：{', '.join(missing)}")


settings = Settings()

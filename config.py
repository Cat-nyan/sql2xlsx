import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    DB_USERNAME: str = os.getenv("DB_USERNAME")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")  # 提供默认值
    DB_PORT: int = int(os.getenv("DB_PORT", 3306))  # 转为 int
    DATABASE: str = os.getenv("DATABASE")
    AUTH_SECRET: str = os.getenv("AUTH_SECRET")


settings = Settings()

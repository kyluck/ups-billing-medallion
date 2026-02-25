from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env from project root


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


def get_settings() -> Settings:
    return Settings(
        pg_host=os.getenv("POSTGRES_HOST", "localhost"),
        pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
        pg_db=os.getenv("POSTGRES_DB", "ups_billing"),
        pg_user=os.getenv("POSTGRES_USER", "ups_user"),
        pg_password=os.getenv("POSTGRES_PASSWORD", "CHANGE_ME"),
    )


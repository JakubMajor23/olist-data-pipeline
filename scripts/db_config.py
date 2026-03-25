import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import URL

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    def sqlalchemy_url(self) -> URL:
        return URL.create(
            "postgresql",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value

    raise RuntimeError(
        f"CONFIGURATION ERROR: Missing environment variable {name}. "
        "Please check your .env file or container environment."
    )


def _get_port(name: str) -> int:
    raw_value = _get_required_env(name)
    try:
        return int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"CONFIGURATION ERROR: Expected an integer port in {name}, "
            f"got {raw_value!r}."
        ) from exc


def get_source_db_config() -> PostgresConfig:
    return PostgresConfig(
        host=_get_required_env("SOURCE_POSTGRES_DOCKER_HOST"),
        port=_get_port("SOURCE_POSTGRES_DOCKER_PORT"),
        user=_get_required_env("SOURCE_POSTGRES_USER"),
        password=_get_required_env("SOURCE_POSTGRES_PASSWORD"),
        database=_get_required_env("SOURCE_POSTGRES_DB"),
    )


def get_dwh_db_config() -> PostgresConfig:
    return PostgresConfig(
        host=_get_required_env("DWH_POSTGRES_DOCKER_HOST"),
        port=_get_port("DWH_POSTGRES_DOCKER_PORT"),
        user=_get_required_env("POSTGRES_USER"),
        password=_get_required_env("POSTGRES_PASSWORD"),
        database=_get_required_env("POSTGRES_DB"),
    )

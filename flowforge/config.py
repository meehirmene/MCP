"""FlowForge Configuration — centralized settings for all components."""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class KafkaConfig(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    internal_bootstrap_servers: str = "kafka:29092"

    model_config = {"env_prefix": "KAFKA_"}


class FlinkConfig(BaseSettings):
    jobmanager_url: str = "http://localhost:8081"

    model_config = {"env_prefix": "FLINK_"}


class IcebergConfig(BaseSettings):
    rest_url: str = "http://localhost:8181"
    warehouse: str = "s3://lakehouse/"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "admin"
    s3_secret_key: str = "password123"

    model_config = {"env_prefix": "ICEBERG_"}


class PostgresConfig(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    database: str = "ecommerce"
    user: str = "flowforge"
    password: str = "flowforge123"

    model_config = {"env_prefix": "POSTGRES_"}

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class MongoConfig(BaseSettings):
    host: str = "localhost"
    port: int = 27017
    database: str = "ecommerce"
    user: str = "flowforge"
    password: str = "flowforge123"

    model_config = {"env_prefix": "MONGO_"}

    @property
    def uri(self) -> str:
        return f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}"


class RedisConfig(BaseSettings):
    host: str = "localhost"
    port: int = 6379
    db: int = 0

    model_config = {"env_prefix": "REDIS_"}


class LLMConfig(BaseSettings):
    provider: str = "openai"  # openai | gemini | anthropic | ollama
    model: str = "gpt-4o"
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: float = 0.1

    model_config = {"env_prefix": "LLM_"}


class FlowForgeConfig(BaseSettings):
    """Root configuration that aggregates all sub-configs."""

    app_name: str = "FlowForge"
    debug: bool = False
    log_level: str = "INFO"

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    flink: FlinkConfig = Field(default_factory=FlinkConfig)
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    mongo: MongoConfig = Field(default_factory=MongoConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)

    model_config = {"env_prefix": "FLOWFORGE_"}


# Global config instance
config = FlowForgeConfig()

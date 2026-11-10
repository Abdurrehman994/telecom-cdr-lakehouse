"""Configuration management for CDR Ingestion Service"""
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings from environment variables - do not expose sensitive values"""
    
    # RabbitMQ Configuration
    rabbitmq_host: str = Field(..., description="RabbitMQ host from RABBITMQ_HOST env var")
    rabbitmq_port: int = Field(..., description="RabbitMQ port from RABBITMQ_PORT env var")
    rabbitmq_user: str = Field(..., description="RabbitMQ username from RABBITMQ_USER env var")
    rabbitmq_password: str = Field(..., description="RabbitMQ password from RABBITMQ_PASSWORD env var")
    rabbitmq_queue: str = Field(..., description="RabbitMQ queue from RABBITMQ_QUEUE env var")
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field("kafka:9092", description="Kafka brokers from KAFKA_BOOTSTRAP_SERVERS env var")
    kafka_topic: str = Field("cdr-events", description="Kafka topic from KAFKA_TOPIC env var")

    # PostgreSQL Configuration
    postgres_host: str = Field(..., description="PostgreSQL host from POSTGRES_HOST env var")
    postgres_port: int = Field(..., description="PostgreSQL port from POSTGRES_PORT env var")
    postgres_user: str = Field(..., description="PostgreSQL username from POSTGRES_USER env var")
    postgres_password: str = Field(..., description="PostgreSQL password from POSTGRES_PASSWORD env var")
    postgres_db: str = Field(..., description="PostgreSQL database from POSTGRES_DB env var")
    
    # Service Configuration
    service_name: str = Field(..., description="Service name from SERVICE_NAME env var")
    service_version: str = Field(..., description="Service version from SERVICE_VERSION env var")
    debug: bool = Field(..., description="Debug mode from DEBUG env var")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"
    
    @property
    def database_url(self) -> str:
        """Generate database connection URL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


settings = Settings()

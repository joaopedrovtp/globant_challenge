from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    """
    Configurações gerais usadas na aplicação
    """
    # Database URL
    # DB_URL: str = "postgresql+asyncpg://postgres:admin@localhost:5432/globant"
    DB_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:admin@localhost:5432/globant")

    
    # Configs S3
    AWS_ACCESS_KEY: str = 'AKIA22JI76RHFKZO625S' 
    AWS_SECRET_KEY: str  = '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'
    BUCKET_NAME: str  = 'globant.bucket.case'


    class Config:
        case_sensitive = True


settings = Settings()

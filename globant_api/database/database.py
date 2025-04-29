
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base
from configs import settings

engine = create_async_engine(settings.DATABASE_URL)
Base = declarative_base()

async def get_session():
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
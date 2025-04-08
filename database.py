from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from configs import settings


engine = create_async_engine(settings.DB_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()


# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# from configs import settings

# engine = create_async_engine(settings.DATABASE_URL)


# async def get_session():
#     async with AsyncSession(engine, expire_on_commit=False) as session:
#         yield session
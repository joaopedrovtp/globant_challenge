from fastapi import APIRouter, Depends, HTTPException
from database.database import engine, get_session
from database.models import Base
from utils.ingestion_db import export_csvs_s3
from sqlalchemy.ext.asyncio import AsyncSession



router = APIRouter(prefix='/import', tags=['import'])

# Manual import csv data from S3 to Postgres
@router.post("/", description="Manual import of CSVs from S3 to Postgres")
async def importar_endpoint(db: AsyncSession = Depends(get_session)):

    # Drop and recreate the tables synchronously
    # This is necessary to ensure that the tables are dropped and recreated in the correct order
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    try:
        await export_csvs_s3(db)
    except Exception as e:
        print(f"Erro ao importar os CSVs: {str(e)}")

    return {"message": "Manual import completed"}

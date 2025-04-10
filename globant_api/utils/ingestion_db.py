from fastapi.concurrency import run_in_threadpool
from fastapi import Depends
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Base
from database.database import get_session
import logging

import pandas as pd
from utils.s3_reader import listar_csvs, ler_csv_do_s3


# Get metadata of models
def get_model_classes():
    return {table_name: model for table_name, model in Base.metadata.tables.items()}


# Reusable function to import CSVs
async def export_csvs_s3(db: AsyncSession = Depends(get_session)):
    arquivos = await run_in_threadpool(listar_csvs)
    model_map = get_model_classes()

    for arquivo in arquivos:
        nome_tabela = arquivo.split("/")[-1].replace(".csv", "")
        model_class = model_map.get(nome_tabela)

        try:
            columns = model_class.columns.keys()
            df = await run_in_threadpool(ler_csv_do_s3, arquivo, columns)

            data = [
                {k: (None if pd.isna(v) else v) for k, v in row.items()}
                for row in df.to_dict(orient="records")
            ]

            total = len(data)
            BATCH_SIZE = 1000  # Batch processing limit 1000
            for i in range(0, total, BATCH_SIZE):
                batch = data[i:i + BATCH_SIZE]

                stmt = insert(model_class).values(batch)
                await db.execute(stmt)
                await db.commit()
                
                logging.info(f"Batch {i // BATCH_SIZE + 1} of {nome_tabela} inserted successfully.")

            print(f"File '{arquivo}' imported successfully.")
            
        except Exception as e:
            print(f"Error importing '{arquivo}': {str(e)}")

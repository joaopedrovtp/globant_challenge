from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi.concurrency import run_in_threadpool
from database import AsyncSessionLocal, engine
from models import Base
import models
from s3_reader import listar_csvs, ler_csv_do_s3
import pandas as pd
from sqlalchemy import insert, text
import logging
from contextlib import asynccontextmanager



# Eventos ao iniciar  a aplicação
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Executa na inicialização
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all)
#         await conn.run_sync(Base.metadata.create_all)

#     await importar_csvs_s3()
#     yield


# app = FastAPI(title='API - Globant Challenge', \
#               lifespan=lifespan)


app = FastAPI(title='API - Globant Challenge')

# Metadados dos modelos
def get_model_classes():
    model_dict = {}
    for name in dir(models):
        attr = getattr(models, name)
        if hasattr(attr, "__tablename__"):
            model_dict[attr.__tablename__] = attr
    return model_dict

# Sessão do banco
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# Função reutilizável para importar CSVs
async def importar_csvs_s3():
    arquivos = await run_in_threadpool(listar_csvs)
    model_map = get_model_classes()

    async with AsyncSessionLocal() as db:
        for arquivo in arquivos:
            nome_tabela = arquivo.split("/")[-1].replace(".csv", "")
            model_class = model_map.get(nome_tabela)

            if not model_class:
                logging.warning(f"Tabela '{nome_tabela}' não encontrada para o arquivo {arquivo}")
                continue

            try:
                columns = [col.name for col in model_class.__table__.columns]
                df = await run_in_threadpool(ler_csv_do_s3, arquivo, columns)

                data = [
                    {k: (None if pd.isna(v) else v) for k, v in row.items()}
                    for row in df.to_dict(orient="records")
                ]

                total = len(data)
                BATCH_SIZE = 1000  # Define o tamanho do lote 
                for i in range(0, total, BATCH_SIZE):
                    batch = data[i:i + BATCH_SIZE]

                    stmt = insert(model_class).values(batch)
                    await db.execute(stmt)
                    await db.commit()
                    
                    logging.info(f"Batch {i // BATCH_SIZE + 1} de {nome_tabela} inserido com sucesso.")

                print(f"Arquivo '{arquivo}' importado com sucesso.")
                
            except Exception as e:
                print(f"Erro ao importar '{arquivo}': {str(e)}")



# Manual import if necessary
@app.post("/import/", description="Manual import of CSVs from S3 to Postgres")
async def importar_endpoint():
    # Drop and recreate the tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await importar_csvs_s3()
    return {"message": "Manual import completed"}


# Hired employees by quarter 2021
@app.get("/hired-employees-by-quarter-2021", description="Fetch the number of employees hired for each job and department in 2021 with quarters as columns")
async def hired_employees_summary(db: AsyncSession = Depends(get_db)):
    query = """
        SELECT 
            d.name AS department,
            j.title AS job,
            SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime::timestamp) = 1 THEN 1 ELSE 0 END) AS q1,
            SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime::timestamp) = 2 THEN 1 ELSE 0 END) AS q2,
            SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime::timestamp) = 3 THEN 1 ELSE 0 END) AS q3,
            SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime::timestamp) = 4 THEN 1 ELSE 0 END) AS q4
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE 
            EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
        GROUP BY d.name, j.title
        ORDER BY d.name, j.title;
    """

    result = await db.execute(text(query))
    records = result.fetchall()

    # Format the result as a list of dictionaries
    return [
        {
            "department": row.department,
            "job": row.job,
            "q1": row.q1,
            "q2": row.q2,
            "q3": row.q3,
            "q4": row.q4
        }
        for row in records
    ]



@app.get("/departments-above-mean-2021", description="List departments that hired more employees than the mean in 2021")
async def departments_above_mean_2021(db: AsyncSession = Depends(get_db)):
    query = """
        WITH department_hires AS (
            SELECT 
                d.id AS department_id,
                d.name AS department_name,
                COUNT(he.id) AS total_hired
            FROM hired_employees he
            JOIN departments d ON he.department_id = d.id
            WHERE EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
            GROUP BY d.id, d.name
        ),
        mean_hires AS (
            SELECT AVG(total_hired) AS mean_hired
            FROM department_hires
        )
        SELECT 
            dh.department_id,
            dh.department_name,
            dh.total_hired
        FROM department_hires dh
        CROSS JOIN mean_hires mh
        WHERE dh.total_hired > mh.mean_hired
        ORDER BY dh.total_hired DESC;
    """

    result = await db.execute(text(query))
    records = result.fetchall()

    return [
        {
            "department_id": row.department_id,
            "department_name": row.department_name,
            "total_hired": row.total_hired
        }
        for row in records
    ]


@app.get("/tabela/{nome_tabela}")
async def listar_dados(nome_tabela: str, db: AsyncSession = Depends(get_db)):
    model_map = get_model_classes()
    model_class = model_map.get(nome_tabela)

    if not model_class:
        raise HTTPException(status_code=404, detail=f"Tabela '{nome_tabela}' não encontrada")

    result = await db.execute(select(model_class))
    registros = result.scalars().all()

    return [r.__dict__ for r in registros]

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("main:app",host='0.0.0.0', log_level='info', reload=True)



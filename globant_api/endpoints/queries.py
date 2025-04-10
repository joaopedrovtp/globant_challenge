from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from database.database import get_session
from sqlalchemy import select, text

router = APIRouter(prefix='/queries', tags=['queries'])

# Query hired employees by quarter 2021
@router.get("/hired-employees-by-quarter-2021",
             description="Fetch the number of employees hired for each job and department in 2021 with quarters as columns",
             response_model=list[dict]) # todo add response model
async def hired_employees_summary(db: AsyncSession = Depends(get_session)):
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


@router.get("/departments-above-mean-2021", 
            description="List departments that hired more employees than the mean in 2021",
            response_model=list[dict]) # todo add response model
async def departments_above_mean_2021(db: AsyncSession = Depends(get_session)):
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


@router.get("/tabela/{nome_tabela}")
async def listar_dados(nome_tabela: str, db: AsyncSession = Depends(get_session)):
    # Check if the table exists in the database
    table_check_query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = :nome_tabela
        ) AS table_exists;
    """
    result = await db.execute(text(table_check_query), {"nome_tabela": nome_tabela})
    table_exists = result.scalar()

    if not table_exists:
        raise HTTPException(status_code=404, detail=f"'{nome_tabela}' table not found in the postgres database")

    # Executar a consulta diretamente na tabela
    query = text(f"SELECT * FROM {nome_tabela}")
    result = await db.execute(query)
    registros = result.mappings().all()

    # Retornar os registros como uma lista de dicionários
    return [dict(row) for row in registros]
from http import HTTPStatus
from fastapi import FastAPI
from endpoints import pipeline, queries



app = FastAPI(title='REST API - Globant Challenge')

app.include_router(pipeline.router)
app.include_router(queries.router)


@app.get('/', status_code=HTTPStatus.OK)
def read_root():
    return {'REST API': 'Globant Challenge API'}



if __name__ == '__main__':
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", log_level='info', reload=True)
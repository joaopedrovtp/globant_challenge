import boto3
import pandas as pd
from io import StringIO
from configs import settings


s3 = boto3.client('s3',
    aws_access_key_id=settings.AWS_ACCESS_KEY,
    aws_secret_access_key=settings.AWS_SECRET_KEY
)

def listar_csvs() -> list[str]:
    response = s3.list_objects_v2(Bucket=settings.BUCKET_NAME)
    arquivos = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    return arquivos

def ler_csv_do_s3(nome_arquivo: str, colunas: list[str]) -> pd.DataFrame:
    response = s3.get_object(Bucket=settings.BUCKET_NAME, Key=nome_arquivo)
    content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content), header=None, names=colunas)
    return df
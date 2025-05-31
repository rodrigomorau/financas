import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os

def carregar_dados_conexao(caminho_env: str = ".env") -> Engine:
    """
    Carrega variáveis de ambiente e cria uma conexão com o banco (NeonDB).
    """
    load_dotenv(dotenv_path=caminho_env)

    usuario = os.getenv("DB_USUARIO")
    senha = os.getenv("DB_SENHA")
    host = os.getenv("DB_HOST")
    porta = os.getenv("DB_PORTA")
    banco = os.getenv("DB_NOME")

    if not all([usuario, senha, host, porta, banco]):
        raise ValueError("❌ Variáveis de ambiente ausentes no .env")

    url = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}?sslmode=require"
    engine = create_engine(url)
    print("✅ Conexão criada com sucesso!")
    return engine

def gravar_excel_no_postgre(
    engine: Engine,
    schema: str,
    tabela: str,
    caminho_arquivo: str,
    if_exists: str = 'append'
):
    """
    Lê um arquivo Excel (.xlsx) e grava os dados no PostgreSQL (NeonDB).
    """
    try:
        df = pd.read_excel(caminho_arquivo)
        print(f"🔍 Lendo {len(df)} registros de {caminho_arquivo}...")

        df.to_sql(
            name=tabela,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        print(f"✅ Dados gravados com sucesso em {schema}.{tabela}!")
    except Exception as e:
        print("❌ Erro ao gravar os dados:", e)

if __name__ == "__main__":
    # Altere esses valores conforme necessário
    caminho_excel = "dados.xlsx"
    schema = "fluxo"
    tabela = "itau"

    engine = carregar_dados_conexao()
    gravar_excel_no_postgre(engine, schema, tabela, caminho_excel)

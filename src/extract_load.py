import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Lista de commodities
commodities = ['CL=F', 'GC=F', 'SI=F']

# Configuração do banco de dados
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# String de conexão corrigida
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Criar engine do SQLAlchemy
engine = create_engine(DATABASE_URL)

# Função para buscar dados de uma commodity
def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

# Função para buscar dados de todas as commodities
def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

# Função para salvar no PostgreSQL
def salvar_no_postgres(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

# Execução principal
if __name__ == "__main__":
    try:
        dados_concatenados = buscar_todos_dados_commodities(commodities)
        salvar_no_postgres(dados_concatenados, schema=DB_SCHEMA)
        print("Dados salvos com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro durante a execução: {e}")

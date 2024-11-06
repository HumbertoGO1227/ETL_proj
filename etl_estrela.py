import pandas as pd
from sqlalchemy import create_engine, text

# Configurações de conexão com o banco de dados Oracle
username = 'system'
password = ''
host = 'DESKTOP-JKUOM6P'
port = '1521'
sid = 'XE'
connection_string = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={sid}"
engine = create_engine(connection_string)

# Função para carregar dados na Dim_Acao
def load_dim_acao():
    query = """
        INSERT INTO Dim_Acao (Codigo_Acao, Nome_Empresa)
        SELECT DISTINCT codigo_acao, nome_empresa FROM staging_cotacoes
        WHERE (codigo_acao, nome_empresa) NOT IN (SELECT Codigo_Acao, Nome_Empresa FROM Dim_Acao)
    """
    with engine.connect() as connection:
        connection.execute(text(query))
    print("Dim_Acao preenchida.")

# Função para carregar dados na Dim_Mercado
def load_dim_mercado():
    query = """
        INSERT INTO Dim_Mercado (Tipo_Mercado)
        SELECT DISTINCT tipo_mercado FROM staging_cotacoes
        WHERE tipo_mercado NOT IN (SELECT Tipo_Mercado FROM Dim_Mercado)
    """
    with engine.connect() as connection:
        connection.execute(text(query))
    print("Dim_Mercado preenchida.")

# Função para carregar dados na Dim_Tempo
def load_dim_tempo():
    query = """
        INSERT INTO Dim_Tempo (Data, Ano, Mes, Dia)
        SELECT DISTINCT data, EXTRACT(YEAR FROM data), EXTRACT(MONTH FROM data), EXTRACT(DAY FROM data)
        FROM staging_cotacoes
        WHERE data NOT IN (SELECT Data FROM Dim_Tempo)
    """
    with engine.connect() as connection:
        connection.execute(text(query))
    print("Dim_Tempo preenchida.")

# Função para carregar dados na Fato_Cotacoes
def load_fato_cotacoes():
    query = """
        INSERT INTO Fato_Cotacoes (ID_Acao, ID_Mercado, ID_Tempo, Preco_Abertura, Preco_Maximo, Preco_Minimo, Preco_Fechamento, Volume_Negociado)
        SELECT 
            a.ID_Acao,
            m.ID_Mercado,
            t.ID_Tempo,
            s.preco_abertura,
            s.preco_maximo,
            s.preco_minimo,
            s.preco_fechamento,
            s.volume_negociado
        FROM staging_cotacoes s
        JOIN Dim_Acao a ON s.codigo_acao = a.Codigo_Acao
        JOIN Dim_Mercado m ON s.tipo_mercado = m.Tipo_Mercado
        JOIN Dim_Tempo t ON s.data = t.Data
    """
    with engine.connect() as connection:
        connection.execute(text(query))
    print("Fato_Cotacoes preenchida.")

# Executando as funções para preencher as tabelas de dimensões e fatos
load_dim_acao()
load_dim_mercado()
load_dim_tempo()
load_fato_cotacoes()

print("ETL para o modelo estrela completo.")

import pandas as pd
from sqlalchemy import create_engine, text

def parse_line(line):
    return {
        "data": line[2:10],
        "codigo_acao": line[12:23].strip(),
        "nome_empresa": line[27:38].strip(),
        "tipo_mercado": line[39:47].strip(),
        "preco_abertura": float(line[56:69].strip()) / 100,
        "preco_maximo": float(line[69:82].strip()) / 100,
        "preco_minimo": float(line[82:95].strip()) / 100,
        "preco_fechamento": float(line[95:108].strip()) / 100,
        "volume_negociado": int(line[108:121].strip())
    }

arquivos = ["COTAHIST_A2022.TXT", "COTAHIST_A2023.TXT", "COTAHIST_A2024.TXT"]

todos_dados = []
for arquivo in arquivos:
    with open(arquivo, "r", encoding="utf-8") as file:
        linhas = file.readlines()
    dados_processados = [parse_line(linha) for linha in linhas if linha.startswith("01")]
    df = pd.DataFrame(dados_processados)
    todos_dados.append(df)

df_final = pd.concat(todos_dados, ignore_index=True)
df_final['data'] = pd.to_datetime(df_final['data'], format='%Y%m%d')

username = 'system'
password = ''
host = 'DESKTOP-JKUOM6P'
port = '1521'
sid = 'XE'

connection_string = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={sid}"
engine = create_engine(connection_string)

create_table_query = """
    CREATE TABLE staging_cotacoes (
        data DATE,
        codigo_acao VARCHAR2(10),
        nome_empresa VARCHAR2(50),
        tipo_mercado VARCHAR2(10),
        preco_abertura NUMBER(10, 2),
        preco_maximo NUMBER(10, 2),
        preco_minimo NUMBER(10, 2),
        preco_fechamento NUMBER(10, 2),
        volume_negociado NUMBER
    )
"""

with engine.connect() as connection:
    try:
        connection.execute(text(create_table_query))
        print("Tabela criada com sucesso.")
    except Exception as e:
        if "ORA-00955" in str(e):
            print("Tabela já existe. Prosseguindo com a inserção de dados.")
        else:
            print("Erro ao criar tabela:", e)
            raise

insert_query = """
    INSERT INTO staging_cotacoes 
    (data, codigo_acao, nome_empresa, tipo_mercado, preco_abertura, preco_maximo, preco_minimo, preco_fechamento, volume_negociado)
    VALUES (:data, :codigo_acao, :nome_empresa, :tipo_mercado, :preco_abertura, :preco_maximo, :preco_minimo, :preco_fechamento, :volume_negociado)
"""

chunk_size = 5000
with engine.connect() as connection:
    for i in range(0, len(df_final), chunk_size):
        chunk = df_final.iloc[i:i+chunk_size]
        data_to_insert = chunk.to_dict(orient='records')
        connection.execute(text(insert_query), data_to_insert)
        print(f"Chunk {i // chunk_size + 1} inserido com sucesso.")

print("Dados carregados com sucesso.")

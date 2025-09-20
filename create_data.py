import pandas as pd
from sqlalchemy import create_engine
from timer.timer import medir_tempo
from pathlib import Path
from gerar_dados import gerando_registros,gerar_produtos,gerar_vendas
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

CONN_STRING = os.getenv("CONN_STRING")



@medir_tempo
def conversao_to_df(dados : list[dict] , nome_arquivo:str):
    print("-"*100)
    print("Convertendo para o DataFrame do Pandas")

    df = pd.DataFrame(dados)
    if 'data_cadastro' in df.columns:
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
    if 'data_venda' in df.columns:
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        
    PATH_ = Path(__file__).parent
    file_path = PATH_/"data"/f"data_{nome_arquivo}.csv"
    df.to_csv(str(file_path), sep=';', encoding='utf-8', index=False)
    return df

@medir_tempo
def insercao_duckdb(df = pd.DataFrame):
    print('-'*100)
    print("Realizando a inserção de dados no DUCKDB.db")
    # DuckDB : {'DUCKDB.db'}
    ROOT_PATH = Path(__file__).parent
    file_path = ROOT_PATH / "duck" / "duck_users.db"
    if file_path.exists():
        file_path.unlink()
        print(f' -> Banco removido!')
    
    engine = create_engine(f"duckdb:///{file_path}")
    with engine.connect() as con:
        for nome_tabela,df in df.items():
            print(f" -> Inserindo dados na tabela '{nome_tabela}'...")
            df.to_sql(nome_tabela,con,if_exists = 'replace' ,index=False)            
    print("Processo concluido")
    
# @medir_tempo
# @with_mongo_db(db_name="mongo_db" , collection_name="users")
# def insercao_mongo(collection, df):
#     try:
#         print("Limpando coleção antes da importação...")
#         collection.delete_many({})
        
#         dados_para_inserir = df.to_dict(orient='records')
        
#         if dados_para_inserir:
#             result = collection.insert_many(dados_para_inserir)
#             print(f"✅ {len(result.inserted_ids)} registros inseridos com sucesso no MongoDB.")
#         else:
#             print('-> DF vazio....')
#     except Exception as e:
#         print(f"🐞 Ocorreu um erro inesperado durante a inserção no MongoDB: {e}")
        
def insercao_mongo(config_dict: dict, db_name: str):
    """Insere um dicionário de DataFrames no MongoDB, uma coleção para cada item."""
    print('-'*100)
    print("Realizando a inserção completa no MongoDB...")
    
    # A Connection String deve ser acessível aqui (global ou carregada do .env)
    
    try:
        # O 'with' gerencia a abertura e fechamento do cliente
        with pymongo.MongoClient(CONN_STRING) as client:
            db = client[db_name]
            
            # O loop itera sobre as coleções que você precisa
            for nome_colecao, config in config_dict.items():
                df = config['df']
                index_fields = config['indexes']
                print(f" -> Processando a coleção '{nome_colecao}'...")
                collection = db[nome_colecao]
                
                # Limpa a coleção
                collection.delete_many({})
                if index_fields:
                    for campo in index_fields:
                        collection.create_index([(campo,1)])
                
                # Converte e insere os dados
                dados_para_inserir = df.to_dict(orient='records')
                if dados_para_inserir:
                    collection.insert_many(dados_para_inserir)
        print("✅ Inserção completa no MongoDB concluída.")

    except Exception as e:
        print(f"🐞 Ocorreu um erro inesperado durante a inserção no MongoDB: {e}")

# gerando_registros(num_registros)
if __name__ == "__main__":
    
    NUM_USERS = 10000
    NUM_PRODUTOS = 1000
    NUM_VENDAS = 50000
    
    usuarios_data = gerando_registros(NUM_USERS)
    produtos_data = gerar_produtos(NUM_PRODUTOS)
    vendas_data = gerar_vendas(NUM_VENDAS, usuarios_data, produtos_data)
    
    
    df_usuarios = conversao_to_df(usuarios_data,'usuarios')
    df_produtos= conversao_to_df(produtos_data , 'produtos')
    df_vendas= conversao_to_df(vendas_data,'vendas')
    
    df_final ={
        "usuarios": df_usuarios,
        "produtos" : df_produtos,
        "vendas": df_vendas
    }
    insercao_duckdb(df_final)
    
    dataframes_para_carregar = {
        "usuarios": {
            "df": df_usuarios,
            "indexes": ["id", "estado", "email"] # Adicionamos 'email' que usamos para OLTP
        },
        "produtos": {
            "df": df_produtos,
            "indexes": ["id_produto", "categoria"]
        },
        "vendas": {
            "df": df_vendas,
            "indexes": ["id_venda", "id_usuario", "id_produto", "data_venda"]
        }
    }
    
    insercao_mongo(dataframes_para_carregar, db_name="mongo_db")
    
    
    # lista_registros = gerando_registros(1000)
    # df = conversao_to_df(lista_registros)
    # insercao_duckdb(df)
    # insercao_mongo(df)
    
    
    
    # teste1 = 10
    # teste2 = 100
    # teste3 = 1000
    # teste4 = 10000

    # print("Iniciando testes de comparação de tempo....")
    # print("-"*100)
    # print()
    # print(f"Teste 1: {teste1} registros")
    # usuarios_gerados_1 = gerando_registros(teste1)
    # duracao1 = gerando_registros.duracao
    # print(f"Tempo de {duracao1} segundos!")

    # print("-"*100)
    # print()
    # print(f"Teste 2: {teste2} registros")
    # usuarios_gerados_2 = gerando_registros(teste2)
    # duracao2 = gerando_registros.duracao
    # print(f"Tempo de {duracao2} segundos!")

    # print("-"*100)
    # print()
    # print(f"Teste 3: {teste3} registros")
    # usuarios_gerados_3 = gerando_registros(teste3)
    # duracao3 = gerando_registros.duracao
    # print(f"Tempo de {duracao3} segundos!")

    # print("-"*100)
    # print()
    # print(f"Teste 4: {teste4} registros")
    # usuarios_gerados_4 = gerando_registros(teste4)
    # duracao4 = gerando_registros.duracao
    # print(f"Tempo de {duracao4} segundos!")
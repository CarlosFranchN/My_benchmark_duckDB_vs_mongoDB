import pymongo
import pandas as pd
from datetime import datetime
import functools

CONN_STRING = "mongodb+srv://carlosfranch29:Total909090!@cluster0.9ul7ywk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

DB_NAME = "mongo_db"
COLLECTION_NAME = "users"

def with_mongo_db(db_name, collection_name):
    global CONN_STRING,DB_NAME,COLLECTION_NAME
    def decorator(func):
        @functools.wraps(func)
        def wrapper (*args,**kwargs):
            client = None
            try:
                print("Conectando")
                client = pymongo.MongoClient(CONN_STRING)
                db = client[db_name]
                collection = db[collection_name]
                print(f"Conexão com a coleção {db_name}.{collection_name}")
                return func(collection, *args, **kwargs)
                # collection.delete_many({})

            except pymongo.errors.ConnectionFailure as e:
                print(f"❌ Erro de Conexão: Verifique sua Connection String e IP. Detalhes: {e}")
            except Exception as e:
                print(f"🐞 Ocorreu um erro inesperado: {e}")
            finally:
                if client:
                    client.close()
                    print("🔌 Conexão com o MongoDB fechada.")  
        return wrapper
    return decorator

@with_mongo_db(db_name="mongo_db" , collection_name="users")
def popular_produtos_from_csv(collection, csv_path: str):
    """
    Limpa a coleção e a popula com dados de um arquivo CSV.
    Lê o arquivo em pedaços (chunks) para otimizar o uso de memória.
    """
    print(f"\n--- Populando a coleção com dados do arquivo: '{csv_path}' ---")
    
    try:
        # 1. Limpa a coleção para garantir uma carga de dados limpa
        print("Limpando a coleção antes da importação...")
        collection.delete_many({})

        # 2. Lê o CSV em pedaços (chunks)
        print("Iniciando a leitura e inserção em lotes...")
        chunk_iterator = pd.read_csv(csv_path, chunksize=1000, sep=';',index_col=0) # Lotes de 1000 linhas
        total_linhas_importadas = 0

        for i, chunk_df in enumerate(chunk_iterator):
            # Converte o chunk para o formato de dicionário do MongoDB
            dados_para_inserir = chunk_df.to_dict(orient='records')
            
            # Insere o lote no banco
            collection.insert_many(dados_para_inserir)
            
            total_linhas_importadas += len(dados_para_inserir)
            print(f"  -> Lote {i+1} inserido. Total de linhas: {total_linhas_importadas}")
        
        print(f"\n✅ Importação concluída. {total_linhas_importadas} registros inseridos.")

    except FileNotFoundError:
        print(f"❌ ERRO: O arquivo CSV '{csv_path}' não foi encontrado.")
    except Exception as e:
        print(f"🐞 Ocorreu um erro durante a importação do CSV: {e}")


if __name__=="__main__":
    CSV_FILE = "./data.csv"
    # df = pd.read_csv("./data.csv", sep=';')
    popular_produtos_from_csv(CSV_FILE)

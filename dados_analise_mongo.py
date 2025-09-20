import pandas as pd
from pathlib import Path
import sys
from dotenv import load_dotenv
from mongo.mock_mongodb import with_mongo_db
import pymongo
import os
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))


from timer.timer import medir_tempo
from mongo.mock_mongodb import with_mongo_db

load_dotenv()

DB_NAME = "mongo_db"
USUARIOS_COLLECTION = "usuarios"
PRODUTOS_COLLECTION = "produtos"
VENDAS_COLLECTION = "vendas"
CONN_STRING = os.getenv("CONN_STRING")

def retornar_tempo(func,pipeline):
    func(pipeline)
    return func.duracao

@medir_tempo
def executar_teste_mongo(pipeline: list, db_name: str, collection_name: str):
    """Conecta, executa um pipeline e retorna o RESULTADO (os dados)."""
    with pymongo.MongoClient(CONN_STRING) as client:
        collection = client[db_name][collection_name]
        df_result = pd.DataFrame(list(collection.aggregate(pipeline)))
        return df_result

PIPELINE_TEMPORAL = [
    {'$group': {'_id': {'$dateToString': {'format': '%Y-%m', 'date': '$data_cadastro'}}, 'novos_usuarios': {'$sum': 1}}},
    {'$sort': {'_id': 1}},
    {'$project': {'_id': 0, 'ano_mes': '$_id', 'novos_usuarios': 1}}
]
PIPELINE_CATEGORIAS = [
    {'$lookup': {'from': 'produtos', 'localField': 'id_produto', 'foreignField': 'id_produto', 'as': 'info_produto'}},
    {'$unwind': '$info_produto'},
    {'$match': {'info_produto.categoria': 'Eletrônicos'}},
    {'$group': {'_id': '$info_produto.categoria', 'total_unidades_vendidas': {'$sum': '$quantidade'}, 'faturamento_total': {'$sum': {'$multiply': ['$quantidade', '$info_produto.preco']}}}}
]
PIPELINE_CLIENTES_RFV = [
    {'$lookup': {'from': 'produtos', 'localField': 'id_produto', 'foreignField': 'id_produto', 'as': 'info_produto'}},
    {'$unwind': '$info_produto'},
    {'$match': {'info_produto.preco': {'$gt': 1500}}},
    {'$group': {'_id': '$id_usuario', 'numero_de_compras_caras': {'$sum': 1}, 'valor_total_gasto': {'$sum': {'$multiply': ['$quantidade', '$info_produto.preco']}}, 'data_ultima_compra': {'$max': '$data_venda'}}},
    {'$lookup': {'from': 'usuarios', 'localField': '_id', 'foreignField': 'id', 'as': 'info_usuario'}},
    {'$unwind': '$info_usuario'},
    {'$sort': {'valor_total_gasto': -1}},
    {'$limit': 20},
    # --- CORREÇÃO APLICADA AQUI ---
    {
        '$project': {
            '_id': 0,
            'nome': '$info_usuario.nome',
            'email': '$info_usuario.email',
            'numero_de_compras': '$numero_de_compras_caras',
            'valor_total_gasto': 1,
            'data_ultima_compra': 1
        }
    }
]
PIPELINE_PRODUTO_POR_ESTADO = [
    {'$lookup': {'from': 'usuarios', 'localField': 'id_usuario', 'foreignField': 'id', 'as': 'info_usuario'}},
    {'$unwind': '$info_usuario'},
    {'$match': {'info_usuario.estado': 'SP'}},
    {'$lookup': {'from': 'produtos', 'localField': 'id_produto', 'foreignField': 'id_produto', 'as': 'info_produto'}},
    {'$unwind': '$info_produto'},
    {'$group': {'_id': {'estado': '$info_usuario.estado', 'produto': '$info_produto.nome_produto'}, 'total_vendido': {'$sum': '$quantidade'}}},
    {'$sort': {'total_vendido': -1}},
    {'$limit': 10},
    {'$project': {'_id': 0, 'estado': '$_id.estado', 'produto': '$_id.produto', 'total_vendido': 1}}
]


def rodar_benchmark_mongo(repeticoes: int = 50):
    testes_a_executar = [
        {
            "nome": "Analise_Temporal",
            "pipeline": PIPELINE_TEMPORAL,
            "collection": "usuarios"
        },
        {
            "nome": "Performance_Categorias",
            "pipeline": PIPELINE_CATEGORIAS,
            "collection": "vendas"
        },
        {
            "nome": "Ranking_Clientes_RFV",
            "pipeline": PIPELINE_CLIENTES_RFV,
            "collection": "vendas"
        },
        {
            "nome": "Produto_Por_Estado",
            "pipeline": PIPELINE_PRODUTO_POR_ESTADO,
            "collection": "vendas"
        }
    ]


    resultados = {teste["nome"]: [] for teste in testes_a_executar}

    print(f"Iniciando benchmark com {repeticoes} repetições para cada teste...")

    for i in range(repeticoes):
        print(f" -> Rodada {i + 1}/{repeticoes}")
        for teste in testes_a_executar:
            nome_teste = teste["nome"]
            # Executa o teste e armazena a duração
            executar_teste_mongo(
                pipeline=teste["pipeline"],
                db_name=DB_NAME,
                collection_name=teste["collection"]
            )

            duracao = executar_teste_mongo.duracao
            resultados[nome_teste].append(duracao)
    
    print("\nBenchmark concluído!")
    
    # Retorna os resultados como um DataFrame do Pandas
    return pd.DataFrame(resultados)

if __name__ == "__main__":
    # rodar_benchmark_mongo(1)
    print(executar_teste_mongo(PIPELINE_TEMPORAL,"mongo_db",USUARIOS_COLLECTION))
import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from timer.timer import medir_tempo
from duck.duck_query import with_duck_db


DB_PATH = 'duck_users.db'



@medir_tempo
@with_duck_db(db_path=DB_PATH)
def teste_olap_duck(con , query):
    df = con.execute(query).df()
    print("   -> Consulta OLAP no DuckDB concluída.")    
    return df
def retornar_duracao(query:str):
    teste_olap_duck(query)
    return teste_olap_duck.duracao   

query_temporal = """
    SELECT
        strftime(data_cadastro::TIMESTAMP, '%Y-%m') AS ano_mes,
        COUNT(id) AS novos_usuarios
    FROM
        usuarios
    GROUP BY
        ano_mes
    ORDER BY
        ano_mes;
"""

query_categorias = """
SELECT
    p.categoria,
    SUM(s.quantidade) AS total_unidades_vendidas,
    SUM(s.quantidade * p.preco) AS faturamento_total
FROM
    vendas AS s
JOIN
    produtos AS p ON s.id_produto = p.id_produto
GROUP BY
    p.categoria
ORDER BY
    faturamento_total DESC;
"""

query_clientes_rfv = """
SELECT
    u.nome,
    u.email,
    COUNT(s.id_venda) AS numero_de_compras,
    SUM(s.quantidade * p.preco) AS valor_total_gasto,
    MAX(s.data_venda) AS data_ultima_compra
FROM
    usuarios AS u
JOIN
    vendas AS s ON u.id = s.id_usuario
JOIN
    produtos AS p ON s.id_produto = p.id_produto
GROUP BY
    u.id, u.nome, u.email
ORDER BY
    valor_total_gasto DESC
LIMIT 20;
"""

query_produto_por_estado = """
WITH VendasDetalhes AS (
    SELECT
        u.estado,
        p.nome_produto,
        s.quantidade
    FROM vendas AS s
    JOIN usuarios AS u ON s.id_usuario = u.id
    JOIN produtos AS p ON s.id_produto = p.id_produto
),
RankingProdutos AS (
    SELECT
        estado,
        nome_produto,
        SUM(quantidade) as total_vendido,
        
        ROW_NUMBER() OVER(PARTITION BY estado ORDER BY SUM(quantidade) DESC) as ranking
    FROM VendasDetalhes
    GROUP BY estado, nome_produto
)

SELECT
    estado,
    nome_produto,
    total_vendido
FROM RankingProdutos
WHERE ranking = 1
ORDER BY estado;
"""
def rodar_benchmark_duckdb(repeticoes: int = 50):
    """
    Roda uma lista pré-definida de queries um número de vezes
    e retorna os resultados em um DataFrame.
    """
    # --- CONFIGURAÇÃO DOS TESTES A EXECUTAR ---
    # Centralize todos os testes aqui. Muito mais fácil de manter!
    testes_a_executar = {
        "Analise_Temporal": query_temporal,
        "Performance_Categorias": query_categorias,
        "Ranking_Clientes_RFV": query_clientes_rfv,
        "Produto_Por_Estado": query_produto_por_estado
    }

    # Dicionário para armazenar as listas de durações
    resultados = {nome_teste: [] for nome_teste in testes_a_executar}

    print(f"Iniciando benchmark do DuckDB com {repeticoes} repetições para cada teste...")

    for i in range(repeticoes):
        print(f" -> Rodada {i + 1}/{repeticoes}")
        for nome_teste, query_sql in testes_a_executar.items():
            # Chama a função de teste diretamente
            teste_olap_duck(query=query_sql)
            # Pega a duração do atributo que o decorator criou
            duracao = teste_olap_duck.duracao
            # Armazena a duração na lista correta
            resultados[nome_teste].append(duracao)
    
    print("\nBenchmark do DuckDB concluído!")
    
    # Retorna os resultados como um DataFrame do Pandas
    return pd.DataFrame(resultados)

if __name__ == "__main__":
    print(teste_olap_duck(query=query_temporal))
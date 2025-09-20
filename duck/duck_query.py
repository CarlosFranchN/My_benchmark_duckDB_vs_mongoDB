import duckdb
import pandas as pd
from pathlib import Path
import functools


DB_PATH = 'duck_users.db'

def with_duck_db(db_path : str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            con = None
            try:
                ROOT_PATH = Path(__file__).parent

                db_file = ROOT_PATH / db_path
                con = duckdb.connect(database=str(db_file),read_only=True)
                print(f"Conexão com {db_file} realizada com sucesso!")
                
                return func(con, *args, **kwargs)
            
            except duckdb.IOException as e:
                print("Erro de conexão!")
                print(f"Erro: {e}")
            except Exception as e:
                print(f"Erro -> {e}")
            finally:
                if con:
                    con.close()
                    print("Finalizando conexão")
                    print('-'*50)
        return wrapper
    return decorator

@with_duck_db(db_path=DB_PATH)
def select_users(con):
    df = con.execute("SELECT * FROM users LIMIT 5").df()
    print(df)
    print('-' * 50)

@with_duck_db(db_path=DB_PATH)
def count_users_by_state(con, state):
    print(f"Quantos usuarios tem no ESTADO: {state}")
    query = "SELECT COUNT(*) FROM users WHERE estado = ?"
    user_count = con.execute(query,[state]).fetchone()[0]
    print(f"Resultado: Existem {user_count:,} usuários em {state}.")
    print('-' * 50)


@with_duck_db(db_path=DB_PATH)
def joins_querys(con):
    QUERY_VENDAS_COM_DETALHES = """
        SELECT
            s.id_venda,
            s.data_venda,
            u.nome AS nome_cliente,
            p.nome_produto AS nome_produto,
            p.categoria,
            s.quantidade,
            p.preco
        FROM
            vendas AS s
        JOIN
            usuarios AS u ON s.id_usuario = u.id
        JOIN
            produtos AS p ON s.id_produto = p.id_produto
        ORDER BY
            s.data_venda DESC
        LIMIT 10;
    """
    df = con.execute(QUERY_VENDAS_COM_DETALHES).df()
    return df
    
if __name__ == "__main__":
    

    # select_users()

    # count_users_by_state('CE')
    print(joins_querys())
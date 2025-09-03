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

if __name__ == "__main__":
    

    select_users()

    count_users_by_state('CE')
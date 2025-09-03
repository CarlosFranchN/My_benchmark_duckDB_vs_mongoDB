# from create_data import gerando_registros,conversao_to_df
import pandas as pd
import duckdb
from sqlalchemy import create_engine
# from timer import medir_tempo
from pathlib import Path

def insercao_duckdb(df = pd.DataFrame):
    print('-'*100)
    print("Realizando a inserção de dados no DUCKDB.db")
    # DuckDB : {'DUCKDB.db'}
    ROOT_PATH = Path(__file__).parent
    file_path = f"{ROOT_PATH}/duck_users.db"
    engine = create_engine(f"duckdb:///{file_path}")
    df.to_sql(
        name='users',
        con=engine,
        if_exists='replace',
        index=False
    )


    print("Processo concluido")


# df = conversao_to_df(lista_registros)
df = pd.read_csv("./data.csv", sep=';',index_col=0)

print(df.head())
insercao_duckdb(df)

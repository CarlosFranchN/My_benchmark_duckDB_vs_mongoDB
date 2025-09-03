import pandas as pd
from faker import Faker
import duckdb
from sqlalchemy import create_engine
from timer import medir_tempo
from pathlib import Path



@medir_tempo
def gerando_registros(num_registros):
    faker = Faker("pt_BR")
    lista_users = []

    for i in range(num_registros):
        user = {
            'id': i,
            'nome': faker.name(),
            'email': faker.email(),
            'cidade': faker.city(),
            'estado': faker.state_abbr(),
            'data_cadastro': faker.date_time_between(start_date='-5y',end_date='now'),
        }
        lista_users.append(user)
    print(f"Lista de usuarios foi gerada com sucesso: {len(lista_users)}")
    return lista_users

@medir_tempo
def conversao_to_df(dados : list[dict]):
    print("-"*100)
    print("Convertendo para o DataFrame do Pandas")

    df = pd.DataFrame(dados)
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
    PATH_ = Path(__file__).parent
    file_path = PATH_/"data.csv"
    df.to_csv(str(file_path),sep=';')
    return df



# gerando_registros(num_registros)
if __name__ == "__main__":
    lista_registros = gerando_registros(1000)
    conversao_to_df(lista_registros)
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
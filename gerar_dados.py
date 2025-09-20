import pandas as pd
from faker import Faker
import random
from datetime import datetime


faker = Faker("pt_BR")


def gerando_registros(num_registros):
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


def gerar_produtos(num_produtos):
    lista_produtos = []
    categorias = ['Eletrônicos', 'Roupas', 'Livros', 'Casa', 'Esportes', 'Brinquedos']
    for i in range(num_produtos):
        produto = {
            'id_produto': i,
            'nome_produto': faker.company() + " " + faker.word().capitalize(), # Nome do produto fictício
            'categoria': random.choice(categorias),
            'preco': round(random.uniform(10, 2500), 2)
        }
        lista_produtos.append(produto)
    return lista_produtos
        

def gerar_vendas(num_vendas, usuarios, produtos):
    print(f"Gerando {num_vendas} vendas...")
    lista_vendas = []
    
    # Pega os IDs disponíveis para criar links válidos
    user_ids = [u['id'] for u in usuarios]
    product_ids = [p['id_produto'] for p in produtos]
    
    for i in range(num_vendas):
        venda = {
            'id_venda': i,
            'id_usuario': random.choice(user_ids),
            'id_produto': random.choice(product_ids),
            'quantidade': random.randint(1, 5),
            'data_venda': faker.date_time_between(start_date='-4y', end_date='now')
        }
        lista_vendas.append(venda)
    return lista_vendas
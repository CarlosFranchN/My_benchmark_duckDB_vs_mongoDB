# 🚀 Benchmark de Performance: DuckDB vs. MongoDB

![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg?logo=python)
![Database](https://img.shields.io/badge/Databases-DuckDB%20%7C%20MongoDB-green.svg)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🎯 Objetivo do Projeto

Este projeto realiza uma análise comparativa de performance (benchmark) entre o **DuckDB** e o **MongoDB**, duas tecnologias de banco de dados com arquiteturas fundamentalmente diferentes.

O objetivo principal não é determinar qual banco é "melhor", mas sim **quantificar e demonstrar na prática** como cada um se comporta em uma carga de trabalho analítica pesada (**OLAP - Online Analytical Processing**).

A análise responde à seguinte pergunta: *Qual é o custo de performance ao utilizar um banco de dados otimizado para transações (OLTP) em tarefas que exigem agregações complexas e junções de dados em massa?*

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Python 3.11+
- **Bancos de Dados:**
    - 🦆 **DuckDB:** Como representante de um banco de dados analítico, colunar e em-processo (in-process).
    - 🍃 **MongoDB:** Utilizando o serviço em nuvem **MongoDB Atlas** (Cluster M0 gratuito) como representante de um banco de dados NoSQL, orientado a documentos e distribuído.
- **Bibliotecas Principais:**
    - `pandas`: Para manipulação e estruturação dos dados.
    - `faker`: Para geração de dados fictícios (mock data) de um e-commerce.
    - `pymongo`: Driver oficial para o MongoDB.
    - `sqlalchemy` & `duckdb-engine`: Para facilitar a inserção de dados no DuckDB.
    - `python-dotenv`: Para gerenciamento seguro de variáveis de ambiente.

---

## 📂 Estrutura do Projeto

```
My_benchmark_duckDB_vs_mongoDB
├─ analise.ipynb
├─ create_data.py
├─ dados_analise_duck.py
├─ dados_analise_mongo.py
├─ data
│  ├─ data_produtos.csv
│  ├─ data_usuarios.csv
│  └─ data_vendas.csv
├─ duck
│  ├─ duck_query.py
│  ├─ mock_duckdb.py
│  └─ __init__.py
├─ gerar_dados.py
├─ mongo
│  ├─ mock_mongodb.py
│  └─ querys_mongodb.py
├─ README.MD
├─ requirements.txt
└─ timer
   ├─ timer.py
   └─ __init__.py

```
---

## 🚀 Guia de Instalação e Execução

Siga os passos abaixo para configurar e executar o projeto em sua máquina local.

### 1. Pré-requisitos
* [Git](https://git-scm.com/)
* [Python 3.11+](https://www.python.org/)
* Uma conta no [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) com um cluster M0 gratuito.

### 2. Configuração do Ambiente

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/My_benchmark_duckDB_vs_mongoDB.git](https://github.com/seu-usuario/My_benchmark_duckDB_vs_mongoDB.git)
    cd My_benchmark_duckDB_vs_mongoDB
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    # Cria o ambiente
    python -m venv venv

    # Ativa no Windows
    .\venv\Scripts\activate

    # Ativa no macOS/Linux
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure as Variáveis de Ambiente (Passo Crucial):**
    * Crie um arquivo chamado `.env` na raiz do projeto.
    * Dentro deste arquivo, adicione sua **String de Conexão** do MongoDB Atlas.
    * **Importante:** No painel do MongoDB Atlas, vá em `Network Access` e libere o acesso para o seu endereço de IP atual (`Add Current IP Address`).

    **Arquivo `.env`:**
    ```env
    MONGO_CONN_STRING="mongodb+srv://seu_usuario:sua_senha@seu_cluster.mongodb.net/"
    ```

### 3. Execução do Projeto

O projeto é executado em duas fases:

#### Fase 1: Geração e População dos Dados

Este script irá gerar todos os dados fictícios (usuários, produtos e vendas) e carregá-los tanto no DuckDB quanto no MongoDB.

```bash
    python popular_tudo_ecommerce.py

🤝 Contribuições

Contribuições são muito bem-vindas!
Sinta-se à vontade para abrir issues com sugestões ou enviar pull requests com melhorias.

💡 Autor

💡 Autor
Desenvolvido por Carlos Franch


Projeto voltado para estudo e comparação de desempenho entre sistemas de bancos de dados modernos.

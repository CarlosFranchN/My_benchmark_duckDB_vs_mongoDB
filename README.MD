# 🚀 Avaliação de desempenho de consultas analíticas

![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg?logo=python)
![Database](https://img.shields.io/badge/Databases-DuckDB%20%7C%20MongoDB-green.svg)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🎯 Objetivo do Projeto
Este projeto realiza uma análise comparativa de desempenho (benchmark) entre duas arquiteturas distintas de banco de dados: um banco analítico embarcado (DuckDB) e um banco NoSQL distribuído em nuvem (MongoDB).

O objetivo principal não é determinar qual tecnologia é superior, mas sim quantificar e analisar, de forma prática, como diferentes arquiteturas de dados se comportam ao executar cargas de trabalho analíticas intensivas (OLAP – Online Analytical Processing), considerando métricas como latência, estabilidade e variabilidade de execução.

A análise busca responder à seguinte questão:
Quais são os trade-offs de desempenho observados ao executar consultas analíticas complexas — envolvendo agregações e junções de grande volume de dados — em arquiteturas originalmente projetadas para propósitos distintos?

---

## 📈 Resultados de Impacto
### ⚡ Comparativo de Latência e Speedup
O gráfico abaixo demonstra a estabilidade da performance ao longo de 10 rodadas. O DuckDB opera na casa dos **milissegundos**, enquanto o MongoDB paga o "pedágio" da latência de rede.

![Estabilidade e Latência](./img/estabilidade_performace_lineplot.png)
*Figura 1: Comparativo de tempo de execução por rodada (Escala Logarítmica).*

### 📊 Tabela Resumo Estatístico
Hard data extraído das 10 rodadas de teste. Note a diferença brutal nas medianas.

| Banco | Query | Mediana (s) | Desvio Padrão | Coef. Variação | Outliers |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **DuckDB (Local)** | Analise_Temporal | **0.0135** | 0.0012 | 8.68% | 2 |
| **DuckDB (Local)** | Ranking_Clientes_RFV | **0.0216** | 0.0016 | 7.08% | 1 |
| **MongoDB (Nuvem)** | Analise_Temporal | **0.6988** | 0.0875 | 12.09% | 1 |
| **MongoDB (Nuvem)** | Ranking_Clientes_RFV | **5.7483** | 0.1268 | 2.20% | 0 |

---
## 🧠 Discussão Técnica & Insights
Os resultados refletem as decisões arquiteturais de cada motor:

1.  **Eficiência Colunar (DuckDB):** Por processar dados em colunas e rodar *in-process*, o DuckDB elimina o I/O desnecessário e a latência de rede, sendo ideal para *Time-to-Insight* rápido.
2.  **Trade-offs do MongoDB:** O MongoDB prioriza escalabilidade e flexibilidade (OLTP). No cenário analítico, o custo do framework de agregação (`$lookup`) e a latência da nuvem tornam as consultas ordens de magnitude mais lentas.
3.  **Variabilidade:** O gráfico de Boxplot abaixo mostra que o DuckDB possui uma dispersão mínima, enquanto o MongoDB sofre com a instabilidade inerente de ambientes distribuídos.

![Distribuição de Performance](./img/variabilidde_outliers_boxplot.png)
*Figura 2: Análise de variabilidade e detecção de pontos fora da curva (Outliers).*

---

## 🧭 Escopo e Metodologia

### 🔹 Escopo
- **Workload:** Consultas analíticas envolvendo agregações massivas e junções (Joins/Lookups).
- **Dataset:** ~50k registros gerados via `Faker` (Usuários, Produtos, Vendas).
- **Métricas:** Latência, Desvio Padrão e Coeficiente de Variação (CV%).

### ⚠️ Limitações
- O MongoDB foi testado no tier gratuito (M0) em nuvem, o que introduz latência de rede.
- O modelo de dados é relacional, desafiando a natureza documental do MongoDB.

---
---
## 🧠 Discussão Técnica (Insights)

Os resultados observados refletem diretamente as escolhas arquiteturais de cada tecnologia.

O DuckDB, por ser um banco analítico colunar e executado em-processo, apresentou tempos de resposta significativamente menores e maior previsibilidade em consultas OLAP, beneficiando-se da ausência de latência de rede e da otimização para agregações em larga escala.

O MongoDB, por outro lado, mesmo apresentando tempos superiores, demonstrou estabilidade em algumas consultas específicas. Esse comportamento está associado à sua arquitetura distribuída e orientada a documentos, que prioriza flexibilidade de esquema, escalabilidade e workloads transacionais (OLTP), não sendo otimizado nativamente para junções complexas e consultas analíticas pesadas.

Dessa forma, o benchmark não indica um “vencedor”, mas evidencia como o uso de uma tecnologia fora de seu domínio principal implica trade-offs claros de desempenho e previsibilidade.

![Distribuição de Performance](./img/variabilidde_outliers_boxplot.png)
*Figura 2: Análise de variabilidade e detecção de outliers entre as rodadas.*

---

### Análise de Resultados

O gráfico de linha abaixo demonstra a estabilidade da performance ao longo das 10 rodadas de teste.

![Estabilidade e Latência](img/estabilidade_performace_lineplot.png)

* **DuckDB:** Apresenta uma linha praticamente plana na base do gráfico, evidenciando a ausência de latência de rede e alta previsibilidade do motor local.
* **MongoDB:** Revela o "piso de latência" (network floor) em nuvem. Mesmo na consulta mais simples, o tempo nunca desce abaixo de ~0.6s devido ao tempo de ida e volta (Round Trip Time) da internet.



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

## 🚀 Como Executar
### 1.Setup: pip install -r requirements.txt
### 2.Ambiente: Configure o .env com sua MONGO_CONN_STRING.
### 3. Pipeline:
```bash
    python popular_tudo_ecommerce.py         # 1. Popula os bancos
    python -m benchmark.OLAP.analise_duckdb  # 2. Benchmark DuckDB
    python -m benchmark.OLAP.analise_mongo
```

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
```

#### Fase 2: Execução dos Benchmarks de Análise

Após a população, execute os scripts de análise para cada banco de dados. Eles rodarão uma série de queries OLAP complexas e imprimirão os resultados e o tempo de execução.

Executar o benchmark do DuckDB:
```
    python -m benchmark.OLAP.analise_duckdb
```

Executar o benchmark do MongoDB:
```
    python -m benchmark.OLAP.analise_mongo
```

📈 Resultados Esperados e Análise

O benchmark confirma a especialização de cada tecnologia. As consultas, embora idênticas em seu objetivo de negócio, demonstram uma diferença significativa de performance:

O DuckDB consistentemente supera o MongoDB em ordens de magnitude em todas as tarefas analíticas (OLAP).



🤝 Contribuições

Contribuições são muito bem-vindas!
Sinta-se à vontade para abrir issues com sugestões ou enviar pull requests com melhorias.

💡 Autor
Desenvolvido por Carlos Franch


Projeto voltado para estudo e comparação de desempenho entre sistemas de bancos de dados modernos.

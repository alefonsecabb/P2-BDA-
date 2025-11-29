# 🛒 Projeto de Engenharia de Dados: Pipeline ETL & Analytics (Olist)

Este repositório contém um projeto completo de Engenharia de Dados desenvolvido para a disciplina de Banco de Dados e Armazem de Dados do curso de Ciência de Dados - Turma 1 (2024-2026) - FATEC Jundiaí. O objetivo é construir um pipeline ETL (Extract, Transform, Load) que processa dados brutos de E-commerce, normaliza-os em um ambiente OLTP e, posteriormente, modela-os em um Data Warehouse (Star Schema) para análise de BI.

---

## 🏗️ Arquitetura e Estrutura

O projeto organiza o fluxo de dados em camadas lógicas dentro do PostgreSQL, orquestradas via Python.

### Estrutura de Diretórios

```text
projeto-dw-grupo-A/
│
├── data/                       # Armazena os arquivos CSV brutos (Dataset Olist)
├── notebooks/                  # Jupyter Notebooks para orquestração e análise
│   ├── 01_pipeline.ipynb       # Executa todo o ETL (Staging -> OLTP -> DW)
│   ├── 02_analytics.ipynb      # Consultas de negócio e KPIs
│   └── 03_visualization.ipynb  # Geração de gráficos e dashboards
├── scripts/                    # Scripts SQL DDL e DML
│   ├── 00_staging.sql          # Carga inicial (Raw Data)
│   ├── 01_oltp.sql             # Normalização e limpeza
│   ├── 02_dw_model.sql         # Estrutura do Data Warehouse
│   ├── 03_etl_load.sql         # Carga das Dimensões e Fatos
│   ├── 04_validation.sql       # Auditoria de dados
│   └── 99_cleanup.sql          # Limpesa das tabelas temporárias
├── src/                        # Módulos auxiliares Python
│   ├── connection.py           # Gerenciador de conexão PostgreSQL
│   ├── graphics.py             # Funções de plotagem
│   └── util.py                 # Utilitários gerais
└── visualizations/             # Pasta de saída para imagens dos gráficos (.png)
````

-----

## 🛠️ Tecnologias Utilizadas

  * **Linguagem:** Python 3.10+
  * **Banco de Dados:** PostgreSQL 13+ (Local ou Cloud)
  * **Orquestração:** Jupyter Notebooks
  * **Bibliotecas Python:**
      * `pandas` (Manipulação de dados e validação)
      * `sqlalchemy` & `psycopg2` (Conectividade com Banco)
      * `plotly` & `matplotlib` (Visualização de dados)
      * `kaleido` (Exportação de imagens estáticas)

-----

## 🚀 Como Executar o Projeto

### 1\. Pré-requisitos: Dados (Kaggle)

Este projeto utiliza o **Brazilian E-Commerce Public Dataset by Olist**.

1.  Acesse o dataset no Kaggle: [Link para Download](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2.  Faça o download do arquivo `archive.zip`.
3.  Extraia **todos os arquivos .csv** para a pasta `data/` na raiz deste projeto.
      * *Exemplo:* `projeto-dw-grupo-A/data/olist_orders_dataset.csv`

### 2\. Configuração do Ambiente

Instale as dependências necessárias:

```bash
pip install pandas sqlalchemy psycopg2-binary plotly matplotlib notebook kaleido
```

### 3\. Configuração do Banco de Dados

1.  Abra o arquivo `src/connection.py`.
2.  Edite as variáveis de conexão com as credenciais do seu PostgreSQL:
    ```python
    DB_USER = "seu_usuario"
    DB_PASSWORD = "sua_senha"
    DB_HOST = "seu_host"
    # ...
    ```

### 4\. Executando o Pipeline

Navegue até a pasta `notebooks/` e execute os arquivos na seguinte ordem:

1.  **`01_pipeline.ipynb`**:

      * Cria os schemas.
      * Carrega os CSVs para tabelas de Staging.
      * Normaliza os dados para tabelas relacionais (OLTP).
      * Cria e popula o Data Warehouse (Schema `analytics`).
      * Realiza validações de volumetria e integridade.

2.  **`02_analytics.ipynb`**:

      * Executa consultas SQL analíticas (Ticket médio, Cohort Analysis, RFM, etc.).

3.  **`03_visualization.ipynb`**:

      * Gera gráficos visuais baseados nas consultas do DW.
      * Salva os resultados na pasta `visualizations/`.

-----

## 👥 Autores

  * **Alexandre Fonseca** - [GitHub](https://github.com/alefonsecabb)
  * **Andre Oliveira** - [GitHub](https://github.com/aso1976)
  * **Carlos de Rezende** - [GitHub](https://github.com/rezendedsc)
  * **Matheus Mendonça** - [GitHub](https://github.com/RockMatalacth)

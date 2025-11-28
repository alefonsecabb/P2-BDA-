# connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ArgumentError
from sqlalchemy import text, inspect
from pathlib import Path
import pandas as pd
import os
import re
import csv
from datetime import date, datetime

def get_engine():
    """
    Cria e retorna uma engine SQLAlchemy para conexão com o banco PostgreSQL.
    A string de conexão é ajustada para definir o 'analytics' como o schema de busca padrão.
    """
    DB_USER = "avnadmin"
    DB_NAME = "defaultdb"
    DB_HOST = "pg-602cf04-teusmath89-cc57.h.aivencloud.com"
    DB_PORT = 23430
    DB_PASSWORD = "AVNS_Q2o5_cku1dndcK4X0nt"
    
    # Parâmetro de conexão para definir o search_path
    # options=-csearch_path%3Danalytics define que o schema 'analytics' será o primeiro a ser buscado.
    SEARCH_PATH_OPTION = "?options=-csearch_path%3Danalytics"

    DATABASE_URL = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}{SEARCH_PATH_OPTION}"
    )

    return create_engine(DATABASE_URL)

def get_session():
    """
    Cria e retorna uma sessão SQLAlchemy que, por padrão, buscará no schema 'analytics'.
    """
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()

def connection_test():
    """
    Tenta obter e testar a conexão com o banco de dados.
    Retorna uma mensagem de status (sucesso ou erro tratado).
    """
    session = None
    connection = None # Usaremos este objeto para o teste de conexão

    try:
        # 1. Tenta obter o Session
        session = get_session()
        
        # 2. Obtém o Engine vinculado ao Session (o Engine é que tem o método .connect())
        engine = session.bind
        
        # 3. Tenta estabelecer a conexão física com o Engine
        with engine.connect() as connection:
            print("✅ Sucesso: Conexão estabelecida com o banco de dados.")
            #return True # Retorna True em caso de sucesso

    except OperationalError as e:
        # 4. Trata erros operacionais (como 'Connection timed out', 'Authentication failed', etc.)
        # Extrai a mensagem de erro do driver (psycopg2, etc.)
        mensagem_erro_driver = str(e.orig) if e.orig else str(e)
        
        print("❌ ERRO de Conexão (Operacional):")
        print(f"Ocorreu uma falha ao conectar ao servidor. Verifique os seguintes pontos:")
        print(f" - Servidor está online e na porta correta?")
        print(f" - Regras de Firewall (seu IP está autorizado?).")
        print(f" - Credenciais de login (usuário e senha).")
        print(f"Detalhe do Erro: {mensagem_erro_driver}")
        return False # Retorna False em caso de erro

    except ArgumentError as e:
        # 5. Trata erros de Argumento (geralmente string de conexão malformada ou driver ausente)
        print("❌ ERRO de Configuração (Argumento):")
        print(f"Falha ao criar o Engine do SQLAlchemy. A string de conexão fornecida está malformada.")
        print(f"Detalhe do Erro: {e}")
        return False

    except Exception as e:
        # 6. Trata outros erros inesperados
        print("❌ ERRO Desconhecido:")
        print(f"Ocorreu um erro inesperado ao tentar conectar. Detalhes: {e}")
        return False

    finally:
        # O objeto Session DEVE ser fechado para liberar recursos
        if session:
            session.close()

        # O `with engine.connect()` garante que a Connection seja fechada,
        # mas adicionamos por segurança.
        if connection and hasattr(connection, 'close'):
            connection.close()

def get_schema_metadata(session, schema_name='analytics', csv_output_path='tabelas_colunas.csv'):
    """
    Inspeciona o schema do banco de dados para listar tabelas, contar registros 
    e listar colunas. Retorna um DataFrame e salva um CSV.

    Args:
        session: Sessão SQLAlchemy ativa (ex: retorno de get_session()).
        schema_name (str): Nome do schema a ser inspecionado. Padrão: 'analytics'.
        csv_output_path (str): Caminho para salvar o CSV. Se None, não salva arquivo.

    Returns:
        pd.DataFrame: DataFrame contendo 'tablename', 'record_count' e 'columns_list'.
    """
    dados_tabelas = []

    try:
        # 1. Busca otimizada dos nomes das tabelas e criação do inspector
        # Passamos session.bind (a Engine) para o inspect
        inspector = inspect(session.bind)
        
        # Verifica se o schema existe ou tem tabelas
        tabelas = inspector.get_table_names(schema=schema_name)
        
        if not tabelas:
            print(f"Aviso: Nenhuma tabela encontrada no schema '{schema_name}'.")
            return pd.DataFrame()

        print(f"Inspecionando {len(tabelas)} tabelas no schema '{schema_name}'...")

        # 2. Itera sobre as tabelas
        for tabela in tabelas:
            # 2.1. Obtém a lista de colunas
            try:
                colunas_info = inspector.get_columns(tabela, schema=schema_name)
                # Extrai apenas o nome ('name') de cada coluna
                lista_colunas = [col['name'] for col in colunas_info]
            except Exception as e:
                lista_colunas = f"ERRO INSPECT: {e.__class__.__name__}"

            # 2.2. Obtém a contagem de registros
            try:
                sql_count = text(f"SELECT COUNT(*) FROM {schema_name}.{tabela};")
                # Executa a query
                count = session.execute(sql_count).scalar_one()
            except Exception as e:
                count = f"ERRO COUNT: {e.__class__.__name__}"

            # 2.3. Adiciona os dados à lista
            dados_tabelas.append({
                'tablename': tabela,
                'record_count': count,
                'columns_list': lista_colunas
            })

        # 3. Cria o DataFrame
        df_tabelas = pd.DataFrame(dados_tabelas)

        # 4. Salva em CSV (se um caminho foi fornecido)
        if csv_output_path:
            try:
                df_tabelas.to_csv(csv_output_path, index=False)
                print(f"Arquivo '{csv_output_path}' salvo com sucesso.")
            except Exception as e:
                print(f"Erro ao salvar CSV: {e}")

        return df_tabelas

    except Exception as e:
        print(f"Erro crítico ao gerar metadados: {e}")
        return pd.DataFrame()

def execute_pipeline_script(sql_file_path):
    """
    Lê um arquivo .sql, separa os comandos e executa no banco.
    Se encontrar um comando COPY, executa via 'copy_expert' (client-side) para contornar
    permissões de leitura de arquivo no servidor (nuvem).
    """
    path = Path(sql_file_path)
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}")
        return

    print(f"--- Processando script: {path.name} ---")

    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
    except Exception as e:
        print(f"[ERRO] Falha ao ler arquivo: {e}")
        return

    # 1. Separa os comandos por ponto e vírgula (;)
    # Remove comentários SQL para evitar erros de split
    content_clean = re.sub(r'--.*', '', raw_content) 
    commands = [cmd.strip() for cmd in content_clean.split(';') if cmd.strip()]

    # Precisamos da conexão 'raw' do psycopg2 para rodar o copy_expert
    engine = get_engine()
    # Usamos uma conexão direta aqui para ter controle granular
    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        print(f"--- Iniciando execução de {len(commands)} bloco(s) ---")
        
        for i, cmd in enumerate(commands, 1):
            
            # Verifica se é um comando COPY (case insensitive)
            if cmd.upper().startswith('COPY'):
                print(f"Executando comando {i} (COPY especial)...")
                
                # Regex para extrair: Tabela e Caminho do arquivo
                # Procura por: COPY tabela FROM 'caminho' ...
                match = re.search(r"COPY\s+([\w\.]+)\s+FROM\s+'([^']+)'", cmd, re.IGNORECASE)
                
                if match:
                    table_name = match.group(1)
                    file_path_str = match.group(2)
                    
                    # Resolve o caminho do CSV relativo ao script SQL
                    csv_full_path = path.parent / file_path_str
                    
                    # Reconstrói o comando SQL substituindo o caminho por STDIN
                    # Mantém o restante das opções (WITH, DELIMITER, etc)
                    sql_copy_stdin = re.sub(r"FROM\s+'[^']+'", "FROM STDIN", cmd, flags=re.IGNORECASE)
                    
                    if not csv_full_path.exists():
                         raise FileNotFoundError(f"CSV não encontrado: {csv_full_path}")

                    with open(csv_full_path, 'r', encoding='utf-8') as f_csv:
                        cursor.copy_expert(sql_copy_stdin, f_csv)
                        print(f"  -> Carga de '{file_path_str}' em '{table_name}' concluída.")
                else:
                    # Se não casar com o padrão esperado, tenta rodar como está (arriscado na nuvem)
                    cursor.execute(cmd)

            else:
                # Comandos normais (CREATE, DROP, INSERT, etc.)
                # print(f"Executando comando {i} (SQL Padrão)...")
                cursor.execute(cmd)

        # Se tudo der certo, commita
        connection.commit()
        print(f"--- [SUCESSO] Script '{path.name}' finalizado com sucesso. ---")

    except Exception as e:
        connection.rollback()
        print(f"--- [FALHA] Erro na execução. Rollback realizado. ---")
        print(f"Erro detalhado: {e}")
        
    finally:
        cursor.close()
        connection.close()

def execute_query_script(sql_file_path):
    """
    Lê um arquivo .sql contendo consultas (SELECTs), limpa comentários,
    executa cada uma e exibe o resultado formatado via Pandas.
    """
    path = Path(sql_file_path)
    
    if not path.exists():
        print(f"❌ Arquivo não encontrado: {path}")
        return

    print(f"\n--- 🔍 Iniciando Validação: {path.name} ---")
    
    session = get_session()
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # 1. Remove comentários de bloco /* ... */
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

        # 2. Separa por ponto e vírgula
        commands = sql_content.split(';')

        count = 0
        for cmd in commands:
            # 3. Limpeza inteligente de linhas
            # Quebra o comando em linhas e remove aquelas que começam com --
            lines = [line for line in cmd.split('\n') if not line.strip().startswith('--')]
            cmd_clean = '\n'.join(lines).strip()
            
            # Ignora comandos vazios após a limpeza
            if not cmd_clean:
                continue
            
            # 4. Verifica se é um comando de leitura válido
            if not (cmd_clean.upper().startswith('SELECT') or cmd_clean.upper().startswith('WITH')):
                continue

            count += 1
            print(f"\n📊 Consulta #{count}:")
            
            try:
                # Executa o comando limpo
                df = pd.read_sql(sql=text(cmd_clean), con=session.bind)
                
                if df.empty:
                    print("[A consulta não retornou registros]")
                else:
                    # Imprime formatado (sem índice numérico)
                    print(df.to_string(index=False)) 
                    
            except Exception as e:
                print(f"⚠️ Erro ao executar a consulta #{count}:")
                print(f"Detalhe: {e}")

        print(f"\n--- ✅ Validação Concluída. {count} consultas executadas. ---")

    except Exception as e:
        print(f"❌ Erro crítico ao processar o arquivo: {e}")
    
    finally:
        session.close()

def generate_python_backup(output_file='backup_dados_analytics.sql'):
    """
    Gera um backup SQL (apenas INSERTs) dos dados do schema 'analytics' usando apenas Python/Pandas.
    Ideal para quando não se tem acesso ao 'pg_dump' local.
    """
    print(f"--- 🐍 Iniciando Backup Python (Pure SQL) para '{output_file}' ---")
    
    session = get_session()
    engine = session.bind
    
    try:
        # 1. Identifica as tabelas do schema analytics
        # A ordem aqui é importante por causa das chaves estrangeiras (Dimensões -> Fato)
        tables_order = ['dim_customer', 'dim_product', 'dim_seller', 'dim_date', 'fact_sales']
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"-- BACKUP GERADO VIA PYTHON EM {datetime.now()}\n")
            f.write("-- SCHEMA: analytics\n\n")
            f.write("BEGIN;\n\n")

            for table in tables_order:
                print(f"Lendo tabela: analytics.{table}...")
                
                # Lê os dados para um DataFrame
                df = pd.read_sql(text(f"SELECT * FROM analytics.{table}"), con=engine)
                
                if df.empty:
                    f.write(f"-- Tabela {table} está vazia.\n\n")
                    continue

                # Prepara as colunas
                columns = ", ".join(df.columns)
                
                # Gera os INSERTs
                # Iterar linha a linha é lento para milhões de registros, mas funcional para este projeto.
                count = 0
                for index, row in df.iterrows():
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append("NULL")
                        elif isinstance(val, (str, date, datetime)):
                            # Escapa aspas simples duplicando-as (ex: O'Brian -> O''Brian)
                            val_str = str(val).replace("'", "''")
                            values.append(f"'{val_str}'")
                        else:
                            values.append(str(val))
                    
                    values_str = ", ".join(values)
                    sql = f"INSERT INTO analytics.{table} ({columns}) VALUES ({values_str});\n"
                    f.write(sql)
                    count += 1
                
                print(f"  -> {count} registros escritos para '{table}'.")
                f.write(f"\n-- Fim da tabela {table}\n\n")

            f.write("COMMIT;\n")
            print(f"✅ Backup concluído! Arquivo salvo em: {output_file}")

    except Exception as e:
        print(f"❌ Erro ao gerar backup Python: {e}")
    finally:
        session.close()
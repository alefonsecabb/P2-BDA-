# connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ArgumentError


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
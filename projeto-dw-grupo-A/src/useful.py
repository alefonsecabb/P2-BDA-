import src.connection as cn
from sqlalchemy import text

def drop_table_cascade(table_name: str):
    """
    Executa um DROP TABLE com CASCADE para a tabela especificada.
    
    Args:
        table_name (str): Nome da tabela (ex: 'analytics.dim_date')
    """
    # 1. Abre a sessão conectada ao banco
    session = cn.get_session()

    try:
        print(f"Tentando apagar a tabela '{table_name}'...")
        
        # 2. Prepara e executa o comando SQL
        # Nota: Usamos f-string porque nomes de tabelas não podem ser bind parameters.
        # Adicionei IF EXISTS para evitar erro se a tabela já não existir.
        sql_command = text(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        session.execute(sql_command)
        
        # 3. Commita a transação
        session.commit()
        print(f"Sucesso: Tabela '{table_name}' processada.")

    except Exception as e:
        # Em caso de erro, desfaz qualquer transação pendente
        session.rollback()
        print(f"Erro ao executar o DROP na tabela '{table_name}': {e}")

    finally:
        # 4. Fecha a sessão para liberar recursos
        session.close()
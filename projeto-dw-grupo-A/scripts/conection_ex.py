from connection import engine, get_session
from sqlalchemy import text

def teste_conexao():
    session = get_session()
    resultado = session.execute(text("SELECT NOW();"))
    print("Conectado! Horário:", list(resultado)[0][0])

def inserir_dados(df):
    """
    Exemplo de bulk insert usando dataframe → banco
    """
    df.to_sql(
        "nome_da_tabela",
        con=engine,
        if_exists="append",
        index=False
    )

if __name__ == "__main__":
    teste_conexao()
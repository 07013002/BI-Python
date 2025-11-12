import psycopg2

# --- 1. CONFIGURAÇÕES DOS BANCOS DE DADOS ---

# Configuração do banco de origem (Sults)
SULTS_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgresql"
}

# Configuração do banco de destino (Data Warehouse)
DW_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "dw_chamados",
    "user": "postgres",
    "password": "postgresql"
}


def criar_dim_responsavel(conn_dw):
    """
    Cria a tabela Dim_Responsavel no Data Warehouse se ela não existir.
    A coluna Fonte_ID é TEXT para compatibilidade.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Responsavel' no Data Warehouse...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Responsavel (
            Responsavel_SK SERIAL PRIMARY KEY,
            Fonte_ID TEXT NOT NULL,                  -- ALTERADO PARA TEXT
            Fonte_Sistema VARCHAR(10) NOT NULL,
            Nome TEXT,
            Sobrenome TEXT,
            Nome_Completo TEXT NOT NULL,
            Email TEXT,
            UNIQUE(Fonte_ID, Fonte_Sistema)
        );
        """)
        conn_dw.commit()
        print("'Dim_Responsavel' pronta para uso.")


def extrair_responsaveis_sults(conn_sults):
    """
    Extrai todos os responsáveis da tabela 'responsaveis_sults'.
    """
    with conn_sults.cursor() as cur:
        print("Extraindo responsáveis do banco de dados 'Sults'...")
        query = "SELECT id, nome FROM responsaveis_sults;"
        cur.execute(query)
        responsaveis = cur.fetchall()
        print(f"Extração concluída. {len(responsaveis)} responsáveis encontrados.")
        return responsaveis


def carregar_dim_responsavel(conn_dw, responsaveis):
    """
    Transforma os nomes dos responsáveis, dividindo-os em nome e sobrenome,
    e os carrega na Dim_Responsavel.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Dim_Responsavel'...")
        registros_processados = 0

        for responsavel_id, nome_completo in responsaveis:
            if not nome_completo:
                continue

            partes_nome = nome_completo.strip().split(' ', 1)
            nome = partes_nome[0]
            sobrenome = partes_nome[1] if len(partes_nome) > 1 else None
            
            query = """
            INSERT INTO Dim_Responsavel (Fonte_ID, Fonte_Sistema, Nome, Sobrenome, Nome_Completo)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (Fonte_ID, Fonte_Sistema) DO UPDATE SET
                Nome = EXCLUDED.Nome,
                Sobrenome = EXCLUDED.Sobrenome,
                Nome_Completo = EXCLUDED.Nome_Completo;
            """
            # O ID é convertido para string para garantir a inserção correta na coluna TEXT
            params = (str(responsavel_id), 'Sults', nome, sobrenome, nome_completo)
            cur.execute(query, params)
            registros_processados += 1
        
        conn_dw.commit()
        print(f"Carga finalizada. {registros_processados} registros de responsáveis processados.")


def main():
    """Função principal que orquestra todo o processo de ETL para a Dim_Responsavel."""
    conn_sults = None
    conn_dw = None
    try:
        conn_sults = psycopg2.connect(**SULTS_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        # 1. Garantir que a tabela de destino exista
        criar_dim_responsavel(conn_dw)

        # 2. Extrair os dados da origem
        lista_responsaveis = extrair_responsaveis_sults(conn_sults)

        # 3. Carregar os dados no destino
        if lista_responsaveis:
            carregar_dim_responsavel(conn_dw, lista_responsaveis)
        else:
            print("Nenhum responsável encontrado para carregar.")

        print("\nProcesso de ETL para a 'Dim_Responsavel' (fonte: Sults) concluído com sucesso!")

    except psycopg2.Error as e:
        print(f"\nOcorreu um erro de banco de dados: {e}")
    finally:
        if conn_sults:
            conn_sults.close()
            print("\nConexão com 'Sults' fechada.")
        if conn_dw:
            conn_dw.close()
            print("Conexão com 'Data Warehouse' fechada.")


if __name__ == "__main__":
    main()

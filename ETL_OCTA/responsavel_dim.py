import psycopg2

# --- 1. CONFIGURAÇÕES DOS BANCOS DE DADOS ---

# Configuração do banco de origem (Octa)
OCTA_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgresocta",
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
    Cria a tabela Dim_Responsavel no Data Warehouse, garantindo que
    a coluna Fonte_ID seja do tipo TEXT para compatibilidade.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Responsavel' no Data Warehouse...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Responsavel (
            Responsavel_SK SERIAL PRIMARY KEY,
            Fonte_ID TEXT NOT NULL,
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


def extrair_responsaveis_octa(conn_octa):
    """
    Extrai todos os responsáveis únicos da tabela 'assigned_octa'.
    O DISTINCT é crucial para não processar a mesma pessoa múltiplas vezes.
    """
    with conn_octa.cursor() as cur:
        print("Extraindo responsáveis do banco de dados 'Octa'...")
        query = """
        SELECT DISTINCT assigned_id, assigned_name, assigned_email
        FROM assigned_octa
        WHERE assigned_id IS NOT NULL AND assigned_name IS NOT NULL;
        """
        cur.execute(query)
        responsaveis = cur.fetchall()
        print(f"Extração concluída. {len(responsaveis)} responsáveis únicos encontrados.")
        return responsaveis


def carregar_dim_responsavel(conn_dw, responsaveis):
    """
    Transforma os nomes dos responsáveis e os carrega na Dim_Responsavel.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Dim_Responsavel'...")
        registros_processados = 0

        for responsavel_id, nome_completo, email in responsaveis:
            if not nome_completo:
                continue

            partes_nome = nome_completo.strip().split(' ', 1)
            nome = partes_nome[0]
            sobrenome = partes_nome[1] if len(partes_nome) > 1 else None

            query = """
            INSERT INTO Dim_Responsavel (Fonte_ID, Fonte_Sistema, Nome, Sobrenome, Nome_Completo, Email)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (Fonte_ID, Fonte_Sistema) DO UPDATE SET
                Nome = EXCLUDED.Nome,
                Sobrenome = EXCLUDED.Sobrenome,
                Nome_Completo = EXCLUDED.Nome_Completo,
                Email = EXCLUDED.Email;
            """
            params = (responsavel_id, 'Octa', nome, sobrenome, nome_completo, email)
            cur.execute(query, params)
            registros_processados += 1
        
        conn_dw.commit()
        print(f"Carga finalizada. {registros_processados} registros de responsáveis processados.")


def main():
    """Função principal que orquestra todo o processo de ETL para a Dim_Responsavel."""
    conn_octa = None
    conn_dw = None
    try:
        conn_octa = psycopg2.connect(**OCTA_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        # 1. Garantir que a tabela de destino exista
        criar_dim_responsavel(conn_dw)

        # 2. Extrair os dados da origem
        lista_responsaveis = extrair_responsaveis_octa(conn_octa)

        # 3. Carregar os dados no destino
        if lista_responsaveis:
            carregar_dim_responsavel(conn_dw, lista_responsaveis)
        else:
            print("Nenhum responsável encontrado para carregar.")

        print("\nProcesso de ETL para a 'Dim_Responsavel' (fonte: Octa) concluído com sucesso!")

    except psycopg2.Error as e:
        print(f"\nOcorreu um erro de banco de dados: {e}")
    finally:
        if conn_octa:
            conn_octa.close()
            print("\nConexão com 'Octa' fechada.")
        if conn_dw:
            conn_dw.close()
            print("Conexão com 'Data Warehouse' fechada.")


if __name__ == "__main__":
    main()

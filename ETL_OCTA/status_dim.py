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


def criar_dim_status(conn_dw):
    """
    Cria a tabela Dim_Status no Data Warehouse se ela não existir.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Status' no Data Warehouse...")
        # A coluna Nome é UNIQUE para garantir que não teremos status duplicados.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Status (
            Status_SK SERIAL PRIMARY KEY,
            Nome TEXT UNIQUE NOT NULL
        );
        """)
        conn_dw.commit()
        print("'Dim_Status' pronta para uso.")


def extrair_status_octa(conn_octa):
    """
    Extrai todos os nomes de status únicos da tabela 'status_octa'.
    """
    with conn_octa.cursor() as cur:
        print("Extraindo status únicos do banco de dados 'Octa'...")
        # Usamos DISTINCT para pegar cada nome de status apenas uma vez.
        query = "SELECT DISTINCT status_name FROM status_octa WHERE status_name IS NOT NULL;"
        cur.execute(query)
        # cur.fetchall() retorna uma lista de tuplas, ex: [('Resolvido',), ('Novo',)]
        # Por isso, extraímos o primeiro elemento de cada tupla.
        lista_status = [item[0] for item in cur.fetchall()]
        print(f"Extração concluída. {len(lista_status)} status únicos encontrados.")
        return lista_status


def carregar_dim_status(conn_dw, lista_status):
    """
    Carrega os nomes de status na tabela Dim_Status.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Dim_Status'...")
        registros_processados = 0

        for nome_status in lista_status:
            # ON CONFLICT(Nome) DO NOTHING é a forma mais eficiente de evitar
            # duplicatas. Se o status já existe, o comando é simplesmente ignorado.
            query = """
            INSERT INTO Dim_Status (Nome)
            VALUES (%s)
            ON CONFLICT (Nome) DO NOTHING;
            """
            # CORREÇÃO: Adicionada uma vírgula para criar uma tupla de um elemento.
            params = (nome_status,)
            cur.execute(query, params)
            registros_processados += 1
        
        conn_dw.commit()
        # Note que o número de registros processados pode ser maior que os inseridos
        # se alguns status já existirem na tabela.
        print(f"Carga finalizada. {registros_processados} nomes de status processados.")


def main():
    """Função principal que orquestra todo o processo de ETL para a Dim_Status."""
    conn_octa = None
    conn_dw = None
    try:
        conn_octa = psycopg2.connect(**OCTA_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        # 1. Garantir que a tabela de destino exista
        criar_dim_status(conn_dw)

        # 2. Extrair os dados da origem
        status_unicos = extrair_status_octa(conn_octa)

        # 3. Carregar os dados no destino
        if status_unicos:
            carregar_dim_status(conn_dw, status_unicos)
        else:
            print("Nenhum status encontrado para carregar.")

        print("\nProcesso de ETL para a 'Dim_Status' (fonte: Octa) concluído com sucesso!")

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

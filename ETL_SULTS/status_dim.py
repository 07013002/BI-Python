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


def criar_dim_status(conn_dw):
    """
    Cria a tabela Dim_Status no Data Warehouse se ela não existir.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Status' no Data Warehouse...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Status (
            Status_SK SERIAL PRIMARY KEY,
            Nome TEXT UNIQUE NOT NULL
        );
        """)
        conn_dw.commit()
        print("'Dim_Status' pronta para uso.")


def extrair_status_sults(conn_sults):
    """
    Extrai todos os códigos de situação únicos da tabela 'chamados_sults'.
    """
    with conn_sults.cursor() as cur:
        print("Extraindo códigos de status únicos do banco de dados 'Sults'...")
        query = "SELECT DISTINCT situacao FROM chamados_sults WHERE situacao IS NOT NULL;"
        cur.execute(query)
        lista_codigos = [item[0] for item in cur.fetchall()]
        print(f"Extração concluída. {len(lista_codigos)} códigos únicos encontrados.")
        return lista_codigos


def carregar_dim_status(conn_dw, lista_codigos):
    """
    Mapeia os códigos de status do Sults para nomes padronizados e os 
    carrega na tabela Dim_Status.
    """
    # Mapeamento (dicionário) que traduz o código do Sults para o nome padrão no DW.
    mapeamento_status = {
        1: 'Novo',
        2: 'Resolvido',
        3: 'Resolvido',
        4: 'Em andamento',
        5: 'Em andamento',
        6: 'Em andamento'
    }

    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Dim_Status'...")
        registros_processados = 0

        for codigo in lista_codigos:
            # Pega o nome padrão do dicionário. Se o código não existir no mapa, ignora.
            nome_status = mapeamento_status.get(codigo)
            
            if not nome_status:
                print(f"Atenção: Código de status '{codigo}' do Sults não possui mapeamento e será ignorado.")
                continue

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
        print(f"Carga finalizada. {registros_processados} códigos de status processados.")


def main():
    """Função principal que orquestra todo o processo de ETL para a Dim_Status."""
    conn_sults = None
    conn_dw = None
    try:
        conn_sults = psycopg2.connect(**SULTS_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        criar_dim_status(conn_dw)
        codigos_unicos = extrair_status_sults(conn_sults)

        if codigos_unicos:
            carregar_dim_status(conn_dw, codigos_unicos)
        else:
            print("Nenhum código de status encontrado para carregar.")

        print("\nProcesso de ETL para a 'Dim_Status' (fonte: Sults) concluído com sucesso!")

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

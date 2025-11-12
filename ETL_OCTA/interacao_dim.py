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


def criar_dim_interacao_publica(conn_dw):
    """
    Cria a tabela Dim_Interacao_Publica no Data Warehouse se ela não existir.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Interacao_Publica' no Data Warehouse...")
        # A coluna Contagem é UNIQUE para garantir que não teremos números duplicados.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Interacao_Publica (
            Interacao_SK SERIAL PRIMARY KEY,
            Contagem INTEGER UNIQUE NOT NULL
        );
        """)
        conn_dw.commit()
        print("'Dim_Interacao_Publica' pronta para uso.")


def extrair_contagens_octa(conn_octa):
    """
    Extrai todos os valores únicos de contagem de interações da tabela 'tickets_octa'.
    """
    with conn_octa.cursor() as cur:
        print("Extraindo contagens únicas de interações do banco de dados 'Octa'...")
        # Usamos DISTINCT para pegar cada valor de contagem apenas uma vez.
        query = "SELECT DISTINCT interactions_count FROM tickets_octa WHERE interactions_count IS NOT NULL;"
        cur.execute(query)
        lista_contagens = [item[0] for item in cur.fetchall()]
        print(f"Extração concluída. {len(lista_contagens)} valores de contagem únicos encontrados.")
        return lista_contagens


def carregar_dim_interacao_publica(conn_dw, lista_contagens):
    """
    Carrega os valores de contagem na tabela Dim_Interacao_Publica.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Dim_Interacao_Publica'...")
        registros_processados = 0

        for contagem in lista_contagens:
            # ON CONFLICT(Contagem) DO NOTHING é a forma mais eficiente de evitar
            # duplicatas. Se a contagem já existe, o comando é simplesmente ignorado.
            query = """
            INSERT INTO Dim_Interacao_Publica (Contagem)
            VALUES (%s)
            ON CONFLICT (Contagem) DO NOTHING;
            """
            params = (contagem,)
            cur.execute(query, params)
            registros_processados += 1
        
        conn_dw.commit()
        print(f"Carga finalizada. {registros_processados} valores de contagem processados.")


def main():
    """Função principal que orquestra todo o processo de ETL para a Dim_Interacao_Publica."""
    conn_octa = None
    conn_dw = None
    try:
        conn_octa = psycopg2.connect(**OCTA_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        # 1. Garantir que a tabela de destino exista
        criar_dim_interacao_publica(conn_dw)

        # 2. Extrair os dados da origem
        contagens_unicas = extrair_contagens_octa(conn_octa)

        # 3. Carregar os dados no destino
        if contagens_unicas:
            carregar_dim_interacao_publica(conn_dw, contagens_unicas)
        else:
            print("Nenhuma contagem de interação encontrada para carregar.")

        print("\nProcesso de ETL para a 'Dim_Interacao_Publica' (fonte: Octa) concluído com sucesso!")

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

import psycopg2
import psycopg2.extras # Essencial para usar DictCursor e facilitar o acesso aos dados

# --- 1. CONFIGURAÇÕES DOS BANCOS DE DADOS ---
OCTA_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgresocta",
    "user": "postgres",
    "password": "postgresql"
}
DW_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "dw_chamados",
    "user": "postgres",
    "password": "postgresql"
}


def criar_fact_chamados(conn_dw):
    """
    Cria a tabela Fact_Chamados no Data Warehouse, caso ela não exista.
    A definição é a mesma do script do Sults.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Fact_Chamados'...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Fact_Chamados (
            Chamado_SK SERIAL PRIMARY KEY,
            Fonte_ID TEXT NOT NULL,
            Fonte_Sistema VARCHAR(10) NOT NULL,
            Titulo TEXT,

            -- Chaves Estrangeiras para as Dimensões
            Data_Abertura_SK INTEGER REFERENCES Dim_Data(Data_SK),
            Hora_Abertura_SK INTEGER REFERENCES Dim_Hora(Hora_SK),
            Data_Conclusao_SK INTEGER REFERENCES Dim_Data(Data_SK),
            Hora_Conclusao_SK INTEGER REFERENCES Dim_Hora(Hora_SK),
            Responsavel_SK INTEGER REFERENCES Dim_Responsavel(Responsavel_SK),
            Status_SK INTEGER REFERENCES Dim_Status(Status_SK),
            Interacao_Publica_SK INTEGER REFERENCES Dim_Interacao_Publica(Interacao_SK),
            
            UNIQUE(Fonte_ID, Fonte_Sistema)
        );
        """)
        conn_dw.commit()
        print("'Fact_Chamados' pronta para uso.")


def carregar_dimensoes_em_memoria(conn_dw):
    """
    Carrega as dimensões em dicionários (mapas) para lookups rápidos.
    """
    print("Carregando dimensões em memória para otimização...")
    mapas = {}
    with conn_dw.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Mapas de Data e Hora
        cur.execute("SELECT Data_SK, Data_Completa FROM Dim_Data;")
        mapas['data'] = {row['data_completa']: row['data_sk'] for row in cur.fetchall()}
        cur.execute("SELECT Hora_SK, Hora_Completa FROM Dim_Hora;")
        mapas['hora'] = {row['hora_completa']: row['hora_sk'] for row in cur.fetchall()}

        # Mapa de Responsáveis (agora carregando os do Octa)
        cur.execute("SELECT Responsavel_SK, Fonte_ID FROM Dim_Responsavel WHERE Fonte_Sistema = 'Octa';")
        mapas['responsavel'] = {row['fonte_id']: row['responsavel_sk'] for row in cur.fetchall()}

        # Mapa de Status
        cur.execute("SELECT Status_SK, Nome FROM Dim_Status;")
        mapas['status'] = {row['nome']: row['status_sk'] for row in cur.fetchall()}

        # Mapa de Interações
        cur.execute("SELECT Interacao_SK, Contagem FROM Dim_Interacao_Publica;")
        mapas['interacao'] = {row['contagem']: row['interacao_sk'] for row in cur.fetchall()}
    
    print("Dimensões carregadas.")
    return mapas


def extrair_chamados_octa(conn_octa):
    """
    Extrai e consolida os dados de chamados do Octa, unindo as tabelas necessárias.
    """
    with conn_octa.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        print("Extraindo chamados do banco de dados 'Octa'...")
        # A consulta une as tabelas do Octa para trazer todos os dados necessários de uma vez.
        query = """
        SELECT
            t.id,
            t.summary AS titulo,
            t.created_at AS aberto,
            t.interactions_count,
            -- A regra de negócio para a data de resolução é aplicada aqui
            CASE
                WHEN s.status_name = 'Resolvido' THEN t.updated_at
                ELSE NULL
            END AS concluido,
            s.status_name,
            a.assigned_id AS responsavel_id
        FROM
            tickets_octa t
        LEFT JOIN
            status_octa s ON t.id = s.ticket_id
        LEFT JOIN
            assigned_octa a ON t.id = a.ticket_id;
        """
        cur.execute(query)
        chamados = cur.fetchall()
        print(f"Extração concluída. {len(chamados)} chamados encontrados.")
        return chamados


def carregar_fact_chamados(conn_dw, chamados, mapas):
    """
    Processa cada chamado do Octa, faz o lookup das chaves e carrega o resultado na Fact_Chamados.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados na 'Fact_Chamados'...")
        for chamado in chamados:
            # --- Processo de LOOKUP ---

            # Data/Hora de Abertura
            data_abertura = chamado['aberto'].date() if chamado.get('aberto') else None
            hora_abertura = chamado['aberto'].time() if chamado.get('aberto') else None
            data_abertura_sk = mapas['data'].get(data_abertura)
            hora_abertura_sk = mapas['hora'].get(hora_abertura)

            # Data/Hora de Conclusão (usa a coluna 'concluido' que já tem a regra aplicada)
            data_conclusao = chamado['concluido'].date() if chamado.get('concluido') else None
            hora_conclusao = chamado['concluido'].time() if chamado.get('concluido') else None
            data_conclusao_sk = mapas['data'].get(data_conclusao)
            hora_conclusao_sk = mapas['hora'].get(hora_conclusao)

            # Responsável
            responsavel_sk = mapas['responsavel'].get(chamado['responsavel_id'])

            # Status
            status_sk = mapas['status'].get(chamado['status_name'])

            # Interação Pública
            interacao_sk = mapas['interacao'].get(chamado['interactions_count'])

            # --- Carga na Tabela Fato ---
            query = """
            INSERT INTO Fact_Chamados (
                Fonte_ID, Fonte_Sistema, Titulo, Data_Abertura_SK, Hora_Abertura_SK,
                Data_Conclusao_SK, Hora_Conclusao_SK, Responsavel_SK, Status_SK, Interacao_Publica_SK
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Fonte_ID, Fonte_Sistema) DO UPDATE SET
                Titulo = EXCLUDED.Titulo,
                Data_Abertura_SK = EXCLUDED.Data_Abertura_SK,
                Hora_Abertura_SK = EXCLUDED.Hora_Abertura_SK,
                Data_Conclusao_SK = EXCLUDED.Data_Conclusao_SK,
                Hora_Conclusao_SK = EXCLUDED.Hora_Conclusao_SK,
                Responsavel_SK = EXCLUDED.Responsavel_SK,
                Status_SK = EXCLUDED.Status_SK,
                Interacao_Publica_SK = EXCLUDED.Interacao_Publica_SK;
            """
            params = (
                chamado['id'], 'Octa', chamado['titulo'], data_abertura_sk, hora_abertura_sk,
                data_conclusao_sk, hora_conclusao_sk, responsavel_sk, status_sk, interacao_sk
            )
            cur.execute(query, params)

        conn_dw.commit()
        print(f"Carga finalizada. {len(chamados)} registros de fatos processados.")


def main():
    conn_octa = None
    conn_dw = None
    try:
        conn_octa = psycopg2.connect(**OCTA_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        # 1. Preparar o DW
        criar_fact_chamados(conn_dw)
        mapas_dimensoes = carregar_dimensoes_em_memoria(conn_dw)
        
        # 2. Extrair dados da origem
        chamados_octa = extrair_chamados_octa(conn_octa)

        # 3. Carregar dados na tabela Fato
        if chamados_octa:
            carregar_fact_chamados(conn_dw, chamados_octa, mapas_dimensoes)
        else:
            print("Nenhum chamado encontrado para carregar.")

        print("\nProcesso de ETL para a 'Fact_Chamados' (fonte: Octa) concluído com sucesso!")

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

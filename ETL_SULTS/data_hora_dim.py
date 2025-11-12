import psycopg2
from datetime import datetime

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


def criar_dimensoes_data_hora(conn_dw):
    """
    Cria as tabelas Dim_Data e Dim_Hora no Data Warehouse se elas não existirem.
    """
    with conn_dw.cursor() as cur:
        print("Verificando/Criando a tabela 'Dim_Data'...")
        # A coluna Periodo_Do_Dia foi removida daqui, pois pertence à hora.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Data (
            Data_SK SERIAL PRIMARY KEY,
            Data_Completa DATE UNIQUE NOT NULL,
            Ano INTEGER NOT NULL,
            Mes INTEGER NOT NULL,
            Dia INTEGER NOT NULL,
            Trimestre INTEGER NOT NULL,
            Dia_Da_Semana INTEGER NOT NULL, -- 0=Segunda, 6=Domingo
            Nome_Mes TEXT NOT NULL,
            Nome_Dia_Da_Semana TEXT NOT NULL
        );
        """)
        print("'Dim_Data' pronta para uso.")

        print("Verificando/Criando a tabela 'Dim_Hora'...")
        # A coluna Periodo_Do_Dia foi adicionada aqui.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Hora (
            Hora_SK SERIAL PRIMARY KEY,
            Hora_Completa TIME UNIQUE NOT NULL,
            Hora INTEGER NOT NULL,
            Minuto INTEGER NOT NULL,
            Segundo INTEGER NOT NULL,
            Periodo_Do_Dia TEXT NOT NULL -- Manhã, Tarde, Noite, Madrugada
        );
        """)
        print("'Dim_Hora' pronta para uso.")
        conn_dw.commit()


def extrair_datas_sults(conn_sults):
    """
    Extrai todas as datas e horas únicas das colunas 'aberto' e 'concluido'
    da tabela 'chamados_sults'.
    """
    with conn_sults.cursor() as cur:
        print("Extraindo datas únicas do banco de dados 'Sults'...")
        # A cláusula UNION remove duplicatas automaticamente
        query = """
        SELECT aberto FROM chamados_sults WHERE aberto IS NOT NULL
        UNION
        SELECT concluido FROM chamados_sults WHERE concluido IS NOT NULL;
        """
        cur.execute(query)
        datas = [row[0] for row in cur.fetchall()]
        print(f"Extração concluída. {len(datas)} registros de data/hora únicos encontrados.")
        return datas


def carregar_dimensoes(conn_dw, datas_horas):
    """
    Recebe uma lista de datetimes, separa em componentes de data e hora,
    e carrega nas tabelas Dim_Data e Dim_Hora, sem duplicatas.
    """
    with conn_dw.cursor() as cur:
        print("Iniciando a carga de dados nas dimensões 'Dim_Data' e 'Dim_Hora'...")
        
        datas_unicas = {dt.date() for dt in datas_horas}
        horas_unicas = {dt.time() for dt in datas_horas}

        # --- Carga da Dim_Data ---
        registros_data_inseridos = 0
        meses = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
        dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]

        for data in datas_unicas:
            query_data = """
            INSERT INTO Dim_Data (
                Data_Completa, Ano, Mes, Dia, Trimestre, Dia_Da_Semana, Nome_Mes, Nome_Dia_Da_Semana
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Data_Completa) DO NOTHING;
            """
            params_data = (
                data, data.year, data.month, data.day,
                (data.month - 1) // 3 + 1, data.weekday(),
                meses[data.month], dias_semana[data.weekday()]
            )
            cur.execute(query_data, params_data)
            if cur.rowcount > 0:
                registros_data_inseridos += 1
        print(f"Carga da Dim_Data finalizada. {registros_data_inseridos} novos registros inseridos.")
        
        # --- Carga da Dim_Hora ---
        registros_hora_inseridos = 0
        for hora_obj in horas_unicas:
            if 6 <= hora_obj.hour < 12: periodo = "Manhã"
            elif 12 <= hora_obj.hour < 18: periodo = "Tarde"
            elif 18 <= hora_obj.hour < 24: periodo = "Noite"
            else: periodo = "Madrugada"
            
            query_hora = """
            INSERT INTO Dim_Hora (
                Hora_Completa, Hora, Minuto, Segundo, Periodo_Do_Dia
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (Hora_Completa) DO NOTHING;
            """
            params_hora = (
                hora_obj, hora_obj.hour, hora_obj.minute, hora_obj.second, periodo
            )
            cur.execute(query_hora, params_hora)
            if cur.rowcount > 0:
                registros_hora_inseridos += 1
        print(f"Carga da Dim_Hora finalizada. {registros_hora_inseridos} novos registros inseridos.")

        conn_dw.commit()


def main():
    """Função principal que orquestra todo o processo de ETL."""
    conn_sults = None
    conn_dw = None
    try:
        conn_sults = psycopg2.connect(**SULTS_DB_CONFIG)
        conn_dw = psycopg2.connect(**DW_DB_CONFIG)

        criar_dimensoes_data_hora(conn_dw)
        datas_horas_unicas = extrair_datas_sults(conn_sults)
        
        if datas_horas_unicas:
            carregar_dimensoes(conn_dw, datas_horas_unicas)
        else:
            print("Nenhuma data encontrada para carregar.")

        print("\nProcesso de ETL para 'Dim_Data' e 'Dim_Hora' (fonte: Sults) concluído com sucesso!")

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

# database.py

import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

@st.cache_resource
def get_connection():
    """Cria e gerencia o cache da conexão com o banco de dados."""
    return create_engine(f"postgresql://postgres:postgresql@localhost:5432/dw_chamados")

# --- FUNÇÕES PARA CARREGAR OPÇÕES DOS FILTROS ---
@st.cache_data(ttl=3600)
def carregar_opcoes_filtro(_engine):
    with _engine.connect() as connection:
        anos = pd.read_sql("SELECT DISTINCT Ano FROM Dim_Data ORDER BY Ano DESC", connection)['ano'].tolist()
        meses_df = pd.read_sql("SELECT DISTINCT Mes, Nome_Mes FROM Dim_Data ORDER BY Mes ASC", connection)
        responsaveis = pd.read_sql("SELECT DISTINCT Nome_Completo FROM Dim_Responsavel ORDER BY Nome_Completo ASC", connection)['nome_completo'].tolist()
        status = pd.read_sql("SELECT DISTINCT Nome FROM Dim_Status ORDER BY Nome ASC", connection)['nome'].tolist()
    return anos, meses_df, responsaveis, status

# --- FUNÇÃO PRINCIPAL DE BUSCA DE DADOS ---
@st.cache_data(ttl=600)
def carregar_chamados_filtrados(_engine, ano, mes, responsaveis, status):
    """Carrega um DataFrame base com os dados brutos, aplicando TODOS os filtros."""
    base_query = """
        SELECT
            F.Chamado_SK,
            D.Nome_Dia_Da_Semana,
            H.Hora,
            R.Nome_Completo AS nome_responsavel,
            S.Nome AS nome_status,
            I.Contagem AS contagem_interacao
        FROM
            Fact_Chamados AS F
        JOIN Dim_Data AS D ON F.Data_Abertura_SK = D.Data_SK
        JOIN Dim_Hora AS H ON F.Hora_Abertura_SK = H.Hora_SK
        JOIN Dim_Responsavel AS R ON F.Responsavel_SK = R.Responsavel_SK
        JOIN Dim_Status AS S ON F.Status_SK = S.Status_SK
        JOIN Dim_Interacao_Publica AS I ON F.Interacao_Publica_SK = I.Interacao_SK
    """
    
    where_clauses, params = [], {}
    
    if ano != 'Todos':
        where_clauses.append("D.Ano = %(ano)s")
        params['ano'] = ano
    if mes != 'Todos':
        where_clauses.append("D.Mes = %(mes)s")
        params['mes'] = mes
    # Filtro de lista para responsáveis (usa o operador 'IN')
    if responsaveis:
        where_clauses.append("R.Nome_Completo IN %(responsaveis)s")
        params['responsaveis'] = tuple(responsaveis)
    # Filtro de lista para status
    if status:
        where_clauses.append("S.Nome IN %(status)s")
        params['status'] = tuple(status)

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    
    with _engine.connect() as connection:
        df = pd.read_sql(base_query, connection, params=params)
    return df
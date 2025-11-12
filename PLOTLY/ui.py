# ui.py

import streamlit as st

def render_sidebar(engine, anos, meses_df, responsaveis, status):
    """Renderiza a barra lateral com todos os filtros."""
    st.sidebar.header("Filtros")

    # Filtros de Data
    ano_selecionado = st.sidebar.selectbox("Ano", options=['Todos'] + anos)
    mes_selecionado_nome = st.sidebar.selectbox("Mês", options=['Todos'] + meses_df['nome_mes'].tolist())

    if mes_selecionado_nome == 'Todos':
        mes_selecionado_numero = 'Todos'
    else:
        mes_selecionado_numero = int(meses_df.loc[meses_df['nome_mes'] == mes_selecionado_nome, 'mes'].iloc[0])

    st.sidebar.markdown("---")

    # Novos Filtros de Seleção Múltipla
    responsaveis_selecionados = st.sidebar.multiselect("Responsáveis", options=responsaveis, placeholder="Todos")
    status_selecionados = st.sidebar.multiselect("Status dos Chamados", options=status, placeholder="Todos")
        
    return ano_selecionado, mes_selecionado_numero, responsaveis_selecionados, status_selecionados
# dashboard.py

import streamlit as st
import database
import ui
import plots

st.set_page_config(layout="wide")
st.title("📊 Dashboard de Análise de Chamados")

engine = database.get_connection()

# --- CORREÇÃO AQUI ---
# A função agora retorna 4 itens, e precisamos receber todos eles.
anos, meses_df, responsaveis, status = database.carregar_opcoes_filtro(engine)

# Passamos os 4 itens para a função que renderiza a sidebar.
ano_selecionado, mes_selecionado, resp_selecionados, status_selecionados = ui.render_sidebar(engine, anos, meses_df, responsaveis, status)

# O restante do código continua igual...
df_base = database.carregar_chamados_filtrados(engine, ano_selecionado, mes_selecionado, resp_selecionados, status_selecionados)

# --- Layout Principal ---
st.caption(f"Filtros aplicados: Ano(`{ano_selecionado}`), Mês(`{mes_selecionado}`), Responsáveis(`{resp_selecionados if resp_selecionados else 'Todos'}`), Status(`{status_selecionados if status_selecionados else 'Todos'}`)")

if not df_base.empty:
    st.markdown("### Métricas Gerais")
    col1, col2 = st.columns(2)
    total_chamados = len(df_base)
    media_interacoes = df_base['contagem_interacao'].mean()
    col1.metric("Total de Chamados Filtrados", f"{total_chamados:,}".replace(",", "."))
    col2.metric("Média de Interações por Chamado", f"{media_interacoes:.2f}")

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.header("Análise por Hora do Dia")
        fig_por_hora = plots.plot_chamados_por_hora(df_base)
        st.plotly_chart(fig_por_hora, use_container_width=True)
        
        st.header("Chamados por Responsável")
        fig_responsaveis = plots.plot_chamados_por_responsavel(df_base)
        st.plotly_chart(fig_responsaveis, use_container_width=True)

    with col2:
        st.header("Análise por Dia da Semana")
        fig_dia_semana = plots.plot_chamados_por_dia_semana(df_base)
        st.plotly_chart(fig_dia_semana, use_container_width=True)

        st.header("Funil de Status")
        fig_status = plots.plot_chamados_por_status(df_base)
        st.plotly_chart(fig_status, use_container_width=True)

    with st.expander("Ver dados brutos filtrados"):
        st.dataframe(df_base)
else:
    st.warning("Nenhum chamado encontrado para o período e filtros selecionados.")
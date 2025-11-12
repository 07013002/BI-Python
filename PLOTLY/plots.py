# plots.py

import plotly.express as px
import pandas as pd

def plot_chamados_por_dia_semana(df_bruto):
    """Cria um gráfico de barras com a contagem de chamados por dia da semana."""
    # CORREÇÃO: Usa 'chamado_sk' para contar
    dados_agrupados = df_bruto.groupby('nome_dia_da_semana')['chamado_sk'].count()
    ordem_dias = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    dados_reindexado = dados_agrupados.reindex(ordem_dias, fill_value=0).reset_index()
    dados_para_plotar = dados_reindexado.rename(columns={'index': 'nome_dia_da_semana', 'chamado_sk': 'Quantidade de Chamados'})
    fig = px.bar(dados_para_plotar, x='nome_dia_da_semana', y='Quantidade de Chamados', title='Distribuição de Chamados ao Longo da Semana', text_auto=True)
    fig.update_layout(xaxis_title="Dia da Semana", yaxis_title="Nº de Chamados")
    return fig

def plot_chamados_por_hora(df_bruto):
    """Cria um gráfico de linha com a contagem de chamados por hora do dia."""
    # CORREÇÃO: Usa 'chamado_sk' para contar
    dados_agrupados = df_bruto.groupby('hora')['chamado_sk'].count()
    horas_completas = pd.DataFrame({'hora': range(24)}).set_index('hora')
    dados_completos = dados_agrupados.reindex(horas_completas.index, fill_value=0).reset_index().rename(columns={'chamado_sk': 'Quantidade de Chamados'})
    fig = px.line(dados_completos, x='hora', y='Quantidade de Chamados', title='Distribuição de Chamados por Hora do Dia', markers=True)
    fig.update_layout(xaxis_title="Hora do Dia", yaxis_title="Nº de Chamados", xaxis=dict(tickmode='linear',tick0=0,dtick=1,range=[-0.5, 23.5],fixedrange=True), yaxis=dict(fixedrange=True))
    fig.update_traces(textposition='top center')
    return fig

def plot_chamados_por_responsavel(df_bruto):
    """Cria um gráfico de pizza com a contagem de chamados por responsável."""
    # CORREÇÃO: Usa 'chamado_sk' para contar
    dados_grafico = df_bruto.groupby('nome_responsavel')['chamado_sk'].count().reset_index().rename(columns={'chamado_sk': 'Quantidade'})
    fig = px.pie(dados_grafico, names='nome_responsavel', values='Quantidade', title='Chamados por Responsável', hole=0.4)
    return fig

def plot_chamados_por_status(df_bruto):
    """Cria um gráfico de funil com a contagem de chamados por status."""
    # CORREÇÃO: Usa 'chamado_sk' para contar
    dados_grafico = df_bruto.groupby('nome_status')['chamado_sk'].count().reset_index().rename(columns={'chamado_sk': 'Quantidade'})
    dados_grafico = dados_grafico.sort_values('Quantidade', ascending=False)
    fig = px.funnel(dados_grafico, x='Quantidade', y='nome_status', title='Funil de Status de Chamados')
    return fig
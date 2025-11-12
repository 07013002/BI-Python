import requests
import psycopg2
import time
from datetime import datetime

# Configuração da API
url_base = "https://api.sults.com.br/api/v1/chamado/ticket"
headers = {
    "Authorization": "token",  #Token sults
    "Content-Type": "application/json;charset=UTF-8"
}

# Configuração do banco
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="postgresql"
)
cur = conn.cursor()

def inserir_simples(tabela, id_valor, nome_valor):
    if id_valor is None or nome_valor is None:
        return  # ignora inserções inválidas
    cur.execute(f"""
        INSERT INTO {tabela} (id, nome)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (id_valor, nome_valor))


def inserir_etiqueta(etiqueta):
    cur.execute("""
        INSERT INTO etiquetas_sults (id, nome, cor)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (etiqueta["id"], etiqueta["nome"], etiqueta["cor"]))

def chamado_existe(id_chamado):
    cur.execute("SELECT 1 FROM chamados_sults WHERE id = %s", (id_chamado,))
    return cur.fetchone() is not None

def inserir_chamado(chamado):

    # Inserção dos dados auxiliares, com validação
    if chamado.get("solicitante"):
        inserir_simples("solicitantes_sults", chamado["solicitante"].get("id"), chamado["solicitante"].get("nome"))

    if chamado.get("responsavel"):
        inserir_simples("responsaveis_sults", chamado["responsavel"].get("id"), chamado["responsavel"].get("nome"))

    if chamado.get("unidade"):
        inserir_simples("unidades_sults", chamado["unidade"].get("id"), chamado["unidade"].get("nome"))

    if chamado.get("departamento"):
        inserir_simples("departamentos_sults", chamado["departamento"].get("id"), chamado["departamento"].get("nome"))

    if chamado.get("assunto"):
        inserir_simples("assuntos_sults", chamado["assunto"].get("id"), chamado["assunto"].get("nome"))

    # Inserir o chamado principal
    cur.execute("""
        INSERT INTO chamados_sults (
            id, titulo, tipo, situacao, aberto, resolvido, concluido,
            resolver_planejado, resolver_estipulado, avaliacao_nota,
            avaliacao_observacao, primeira_interacao, ultima_alteracao,
            count_interacao_publico, count_interacao_interno,
            solicitante_id, responsavel_id, unidade_id, departamento_id, assunto_id
        ) VALUES (
            %(id)s, %(titulo)s, %(tipo)s, %(situacao)s, %(aberto)s, %(resolvido)s, %(concluido)s,
            %(resolverPlanejado)s, %(resolverEstipulado)s, %(avaliacaoNota)s,
            %(avaliacaoObservacao)s, %(primeiraInteracao)s, %(ultimaAlteracao)s,
            %(countInteracaoPublico)s, %(countInteracaoInterno)s,
            %(solicitante_id)s, %(responsavel_id)s, %(unidade_id)s, %(departamento_id)s, %(assunto_id)s
        )
        ON CONFLICT (id) DO NOTHING
    """, {
        "id": chamado["id"],
        "titulo": chamado.get("titulo"),
        "tipo": chamado.get("tipo"),
        "situacao": chamado.get("situacao"),
        "aberto": chamado.get("aberto"),
        "resolvido": chamado.get("resolvido"),
        "concluido": chamado.get("concluido"),
        "resolverPlanejado": chamado.get("resolverPlanejado"),
        "resolverEstipulado": chamado.get("resolverEstipulado"),
        "avaliacaoNota": chamado.get("avaliacaoNota"),
        "avaliacaoObservacao": chamado.get("avaliacaoObservacao", ""),
        "primeiraInteracao": chamado.get("primeiraInteracao"),
        "ultimaAlteracao": chamado.get("ultimaAlteracao"),
        "countInteracaoPublico": chamado.get("countInteracaoPublico", 0),
        "countInteracaoInterno": chamado.get("countInteracaoInterno", 0),
        "solicitante_id": chamado["solicitante"]["id"] if chamado.get("solicitante") else None,
        "responsavel_id": chamado["responsavel"]["id"] if chamado.get("responsavel") else None,
        "unidade_id": chamado["unidade"]["id"] if chamado.get("unidade") else None,
        "departamento_id": chamado["departamento"]["id"] if chamado.get("departamento") else None,
        "assunto_id": chamado["assunto"]["id"] if chamado.get("assunto") else None
    })


    # Inserir o chamado principal VERIFICAR
    cur.execute("""
        INSERT INTO chamados_sults (
            id, titulo, tipo, situacao, aberto, resolvido, concluido,
            resolver_planejado, resolver_estipulado, avaliacao_nota,
            avaliacao_observacao, primeira_interacao, ultima_alteracao,
            count_interacao_publico, count_interacao_interno,
            solicitante_id, responsavel_id, unidade_id, departamento_id, assunto_id
        ) VALUES (
            %(id)s, %(titulo)s, %(tipo)s, %(situacao)s, %(aberto)s, %(resolvido)s, %(concluido)s,
            %(resolverPlanejado)s, %(resolverEstipulado)s, %(avaliacaoNota)s,
            %(avaliacaoObservacao)s, %(primeiraInteracao)s, %(ultimaAlteracao)s,
            %(countInteracaoPublico)s, %(countInteracaoInterno)s,
            %(solicitante_id)s, %(responsavel_id)s, %(unidade_id)s, %(departamento_id)s, %(assunto_id)s
        )
        ON CONFLICT (id) DO NOTHING
    """, {
        "id": chamado["id"],
        "titulo": chamado["titulo"],
        "tipo": chamado["tipo"],
        "situacao": chamado["situacao"],
        "aberto": chamado["aberto"],
        "resolvido": chamado["resolvido"],
        "concluido": chamado["concluido"],
        "resolverPlanejado": chamado["resolverPlanejado"],
        "resolverEstipulado": chamado["resolverEstipulado"],
        "avaliacaoNota": chamado.get("avaliacaoNota", None),
        "avaliacaoObservacao": chamado.get("avaliacaoObservacao", ""),
        "primeiraInteracao": chamado["primeiraInteracao"],
        "ultimaAlteracao": chamado["ultimaAlteracao"],
        "countInteracaoPublico": chamado["countInteracaoPublico"],
        "countInteracaoInterno": chamado["countInteracaoInterno"],
        "solicitante_id": chamado["solicitante"]["id"],
        "responsavel_id": chamado["responsavel"]["id"],
        "unidade_id": chamado["unidade"]["id"],
        "departamento_id": chamado["departamento"]["id"],
        "assunto_id": chamado["assunto"]["id"]
    })

    # Inserir etiquetas
    for etiqueta in chamado.get("etiqueta", []):
        inserir_etiqueta(etiqueta)
        cur.execute("""
            INSERT INTO chamado_etiqueta_sults (chamado_id, etiqueta_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (chamado["id"], etiqueta["id"]))

    # Inserir apoios
    for apoio in chamado["apoio"] if isinstance(chamado.get("apoio"), list) else []:
        depto = apoio.get("departamento")
        if depto:
            inserir_simples("departamentos_sults", depto.get("id"), depto.get("nome"))

# Loop de paginação
start = 0
limit = 100

while True:
    params = {"start": start, "limit": limit}
    response = requests.get(url_base, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Erro na requisição: {response.status_code}")
        break

    data = response.json().get("data", [])
    if not data:
        print("Todos os dados foram processados.")
        break

    for chamado in data:
        if not chamado_existe(chamado["id"]):
            inserir_chamado(chamado)

    print(f"Processados {len(data)} registros na página {start}.")
    start += 1
    conn.commit()
    time.sleep(0.5)  # Evitar sobrecarregar a API

cur.close()
conn.close()
print("Finalizado com sucesso.")


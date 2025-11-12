import psycopg2

# Conecte-se ao PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="postgresql"
)
cur = conn.cursor()

# Criação das tabelas base
cur.execute("""
CREATE TABLE IF NOT EXISTS solicitantes_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT
);

CREATE TABLE IF NOT EXISTS responsaveis_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT
);

CREATE TABLE IF NOT EXISTS unidades_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT
);

CREATE TABLE IF NOT EXISTS departamentos_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT
);

CREATE TABLE IF NOT EXISTS assuntos_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT
);

CREATE TABLE IF NOT EXISTS etiquetas_sults (
    id INTEGER PRIMARY KEY,
    nome TEXT,
    cor TEXT
);
""")

# Criação das tabelas de relacionamento
cur.execute("""
CREATE TABLE IF NOT EXISTS chamados_sults (
    id INTEGER PRIMARY KEY,
    titulo TEXT,
    tipo INTEGER,
    situacao INTEGER,
    aberto TIMESTAMP,
    resolvido TIMESTAMP,
    concluido TIMESTAMP,
    resolver_planejado TIMESTAMP,
    resolver_estipulado TIMESTAMP,
    avaliacao_nota INTEGER,
    avaliacao_observacao TEXT,
    primeira_interacao TIMESTAMP,
    ultima_alteracao TIMESTAMP,
    count_interacao_publico INTEGER,
    count_interacao_interno INTEGER,

    solicitante_id INTEGER REFERENCES solicitantes_sults(id),
    responsavel_id INTEGER REFERENCES responsaveis_sults(id),
    unidade_id INTEGER REFERENCES unidades_sults(id),
    departamento_id INTEGER REFERENCES departamentos_sults(id),
    assunto_id INTEGER REFERENCES assuntos_sults(id)
);

CREATE TABLE IF NOT EXISTS apoios_sults (
    id SERIAL PRIMARY KEY,
    chamado_id INTEGER REFERENCES chamados_sults(id),
    pessoa_id INTEGER REFERENCES pessoas_sults(id),
    departamento_id INTEGER REFERENCES departamentos_sults(id),
    pessoa_unidade BOOLEAN
);

CREATE TABLE IF NOT EXISTS chamado_etiqueta_sults (
    chamado_id INTEGER REFERENCES chamados_sults(id),
    etiqueta_id INTEGER REFERENCES etiquetas_sults(id),
    PRIMARY KEY (chamado_id, etiqueta_id)
);
""")

conn.commit()
cur.close()
conn.close()

print("Todas as tabelas foram criadas com sucesso.")

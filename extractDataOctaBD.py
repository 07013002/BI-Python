import psycopg2

# === CONEXÃO COM O POSTGRES ===
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgresocta",
    user="postgres",
    password="postgresql"
)
cur = conn.cursor()

# === CRIAÇÃO DAS TABELAS ===

# Tabela principal de tickets
cur.execute("""
CREATE TABLE IF NOT EXISTS tickets_octa (
    id TEXT PRIMARY KEY,
    number INTEGER,
    summary TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    interactions_count INTEGER
)
""")

# Tabela de status
cur.execute("""
CREATE TABLE IF NOT EXISTS status_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    status_id TEXT,
    status_name TEXT,
    PRIMARY KEY (ticket_id, status_id)
)
""")

# Tabela de channel
cur.execute("""
CREATE TABLE IF NOT EXISTS channel_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    channel_id TEXT,
    channel_name TEXT,
    PRIMARY KEY (ticket_id, channel_id)
)
""")

# Tabela de tipo
cur.execute("""
CREATE TABLE IF NOT EXISTS type_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    type_id TEXT,
    type_name TEXT,
    PRIMARY KEY (ticket_id, type_id)
)
""")

# Tabela de requester
cur.execute("""
CREATE TABLE IF NOT EXISTS requester_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    requester_id TEXT,
    requester_name TEXT,
    requester_email TEXT,
    PRIMARY KEY (ticket_id, requester_id)
)
""")

# Tabela de grupo
cur.execute("""
CREATE TABLE IF NOT EXISTS group_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    group_id TEXT,
    group_name TEXT,
    PRIMARY KEY (ticket_id, group_id)
)
""")

# Tabela de prioridade
cur.execute("""
CREATE TABLE IF NOT EXISTS priority_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    priority_id TEXT,
    priority_name TEXT,
    PRIMARY KEY (ticket_id, priority_id)
)
""")

# Tabela de organização
cur.execute("""
CREATE TABLE IF NOT EXISTS organization_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    organization_id TEXT,
    organization_name TEXT,
    PRIMARY KEY (ticket_id, organization_id)
)
""")

# Tabela de atribuição
cur.execute("""
CREATE TABLE IF NOT EXISTS assigned_octa (
    ticket_id TEXT REFERENCES tickets_octa(id),
    assigned_id TEXT,
    assigned_name TEXT,
    assigned_email TEXT,
    PRIMARY KEY (ticket_id, assigned_id)
)
""")


# === COMMIT DAS ALTERAÇÕES ===
conn.commit()

print("Tabelas 'tickets_octa' e relacionadas criadas com sucesso.")

# === FINALIZA CONEXÃO ===
cur.close()
conn.close()

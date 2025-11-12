import requests
import psycopg2
import time
import os

# === CONFIGURAÇÃO DA API ===
# É uma boa prática carregar segredos de variáveis de ambiente
# Em vez de deixá-los diretamente no código.
API_KEY = os.getenv("OCTA_API_KEY", "token")
AGENT_EMAIL = os.getenv("OCTA_AGENT_EMAIL", "email@gmail.com")

headers = {
    "accept": "application/json",
    "octa-agent-email": AGENT_EMAIL,
    "X-API-KEY": API_KEY
}

# === CONEXÃO COM O POSTGRES ===
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgresocta",
        user="postgres",
        password="postgresql"
    )
    cur = conn.cursor()
    print("Conexão com o PostgreSQL estabelecida com sucesso.")
except psycopg2.OperationalError as e:
    print(f"Erro ao conectar ao PostgreSQL: {e}")
    exit()

def ticket_exists(ticket_id):
    """Verifica se um ticket já existe na tabela principal."""
    cur.execute("SELECT 1 FROM tickets_octa WHERE id = %s", (ticket_id,))
    return cur.fetchone() is not None

def inserir_ticket(ticket):
    """
    Insere um ticket e suas informações relacionadas de forma segura,
    verificando a existência de dados antes de tentar a inserção.
    """
    ticket_id = ticket.get("id")
    if not ticket_id:
        print("Ticket sem ID recebido, pulando.")
        return

    # 1. Inserção na tabela principal 'tickets_octa'
    try:
        cur.execute("""
            INSERT INTO tickets_octa (id, number, summary, created_at, updated_at, interactions_count)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            ticket_id,
            ticket.get("number"),
            ticket.get("summary"),
            ticket.get("createdAt"),
            ticket.get("updatedAt"),
            ticket.get("interactionsCount")
        ))
    except Exception as e:
        print(f"Erro ao inserir na tabela principal tickets_octa para o ticket {ticket_id}: {e}")
        conn.rollback() # Desfaz a transação parcial para este ticket
        return


    # 2. Inserção segura em tabelas relacionadas
    # Dicionário para mapear chaves da API para configurações do DB
    related_tables = {
        "status": {"table": "status_octa", "cols": ["ticket_id", "status_id", "status_name"]},
        "channel": {"table": "channel_octa", "cols": ["ticket_id", "channel_id", "channel_name"]},
        "type": {"table": "type_octa", "cols": ["ticket_id", "type_id", "type_name"]},
        "requester": {"table": "requester_octa", "cols": ["ticket_id", "requester_id", "requester_name", "requester_email"]},
        "group": {"table": "group_octa", "cols": ["ticket_id", "group_id", "group_name"]},
        "priority": {"table": "priority_octa", "cols": ["ticket_id", "priority_id", "priority_name"]},
        "organization": {"table": "organization_octa", "cols": ["ticket_id", "organization_id", "organization_name"]},
        "assigned": {"table": "assigned_octa", "cols": ["ticket_id", "assigned_id", "assigned_name", "assigned_email"]},
    }

    for key, config in related_tables.items():
        data_dict = ticket.get(key)
        
        # Pula se o dicionário aninhado não existir ou for nulo
        if not data_dict or not isinstance(data_dict, dict):
            continue

        # Extrai os valores baseados no nome da coluna (ex: status_id -> id)
        values = [ticket_id]
        for col_name in config["cols"][1:]: # Pula a primeira coluna (ticket_id)
            field_name = col_name.split('_')[1] # Extrai 'id', 'name', 'email' etc.
            values.append(data_dict.get(field_name))

        # A chave da entidade (ex: status_id) não pode ser nula por causa da PK composta.
        # O segundo item na lista 'values' é sempre o ID da entidade relacionada.
        if values[1] is None:
            # print(f"  -> ID nulo para '{key}' no ticket {ticket_id}. Pulando inserção em {config['table']}.")
            continue
            
        try:
            placeholders = ", ".join(["%s"] * len(config["cols"]))
            cols_str = ", ".join(config["cols"])
            sql = f"INSERT INTO {config['table']} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cur.execute(sql, tuple(values))
        except Exception as e:
            print(f"  -> ERRO ao inserir em {config['table']} para o ticket {ticket_id}: {e}")
            # Decide-se continuar o processamento das outras tabelas para o mesmo ticket

    print(f"Ticket {ticket_id} processado com sucesso.")


# === LOOP DE PAGINAÇÃO ===
def main():
    page = 1
    limit = 100
    while True:
        url = f"https://o205391-132.api004.octadesk.services/tickets?page={page}&limit={limit}"
        print(f"\nBuscando página {page}...")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Lança um erro para status HTTP 4xx/5xx
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição à API na página {page}: {e}")
            break

        data = response.json()

        if not data:
            print("Fim dos dados. Nenhuma informação retornada.")
            break

        for ticket in data:
            ticket_id = ticket.get("id")
            if not ticket_id:
                print("Ticket sem ID encontrado na resposta, pulando.")
                continue

            if not ticket_exists(ticket_id):
                try:
                    inserir_ticket(ticket)
                except Exception as e:
                    print(f"Erro inesperado ao processar o ticket {ticket_id}: {e}")
                    conn.rollback() # Garante que a transação para este ticket seja desfeita
            else:
                print(f"Ticket {ticket_id} já existe, pulando.")

        conn.commit()  # Salva as transações da página inteira
        page += 1
        time.sleep(0.5)

    # === FINALIZA CONEXÃO ===
    cur.close()
    conn.close()
    print("\nProcesso finalizado.")

if __name__ == "__main__":
    main()

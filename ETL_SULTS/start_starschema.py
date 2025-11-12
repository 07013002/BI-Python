import subprocess
import sys

# --- LISTA DE SCRIPTS PARA EXECUÇÃO (APENAS SULTS) ---
# A ordem é crucial: primeiro as dimensões, depois a tabela fato.

scripts_dimensoes_SULTS = [
    # Scripts de Dimensão para a fonte de dados SULTS
    "ETL_SULTS\data_hora_dim.py",
    "ETL_SULTS\\responsavel_dim.py",
    "ETL_SULTS\status_dim.py",
    "ETL_SULTS\interacao_dim.py"
]

scripts_fatos_SULTS = [
    # Script da Tabela Fato para a fonte de dados SULTS
    "ETL_SULTS\chamados_fato.py"
]

# Une as duas listas na ordem correta de execução
scripts_a_executar = scripts_dimensoes_SULTS + scripts_fatos_SULTS


def executar_script(nome_script):
    """
    Executa um único script Python e verifica se houve erros.
    Retorna True para sucesso, False para falha.
    """
    print("="*50)
    print(f"INICIANDO EXECUÇÃO DE: {nome_script}")
    print("="*50)
    
    try:
        # sys.executable garante que estamos usando o mesmo interpretador Python
        # que está executando este script orquestrador.
        # check=True faz com que o programa levante um erro se o script falhar.
        resultado = subprocess.run(
            [sys.executable, nome_script], 
            check=True, 
            capture_output=True, # Captura a saída do script
            text=True            # Converte a saída para texto
        )
        
        # Imprime a saída padrão do script que foi executado
        print(resultado.stdout)
        
        print("-" * 50)
        print(f"SUCESSO: O script {nome_script} foi concluído.")
        print("-" * 50 + "\n")
        return True
        
    except FileNotFoundError:
        print(f"\nERRO CRÍTICO: O arquivo '{nome_script}' não foi encontrado.")
        print("Verifique se o nome do arquivo está correto na lista e se ele está na mesma pasta.")
        return False
        
    except subprocess.CalledProcessError as e:
        # Este bloco é executado se check=True detectar um erro no script filho.
        print(f"\nERRO CRÍTICO: O script '{nome_script}' falhou durante a execução.")
        print(f"Código de retorno: {e.returncode}")
        
        # Imprime a saída padrão e a saída de erro do script que falhou para ajudar na depuração
        print("\n--- SAÍDA PADRÃO DO SCRIPT ---")
        print(e.stdout)
        print("\n--- SAÍDA DE ERRO DO SCRIPT ---")
        print(e.stderr)
        print("---------------------------------")
        
        return False


def main():
    """
    Função principal que itera sobre la lista de scripts e os executa em ordem.
    """
    print(">>> INICIANDO ORQUESTRADOR DE ETL (SOMENTE SULTS) <<<\n")
    
    for script in scripts_a_executar:
        if not executar_script(script):
            print("\n>>> PROCESSO DE ETL INTERROMPIDO DEVIDO A UM ERRO. <<<")
            # Encerra o programa principal se um dos scripts falhar
            sys.exit(1)
            
    print(">>> PROCESSO DE ETL (SULTS) CONCLUÍDO COM SUCESSO! <<<")


if __name__ == "__main__":
    main()

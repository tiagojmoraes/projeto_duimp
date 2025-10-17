import requests
from dotenv import load_dotenv
import os

# Carrega as variáveis do .env
load_dotenv()

def authenticate():
    # Obtém as credenciais do .env
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    role_type = os.getenv('ROLE_TYPE')
    base_url = os.getenv('PUCOMEX_BASE_URL')  

    # Define os headers para autenticação
    headers = {
        'Client-Id': client_id,
        'Client-Secret': client_secret,
        'Role-Type': role_type
    }

    response = None # Inicializa response para garantir que existe no escopo do except

    try:
        # Faz a requisição POST
        response = requests.post(base_url, headers=headers)
        response.raise_for_status()  # Levanta exceção se houver erro HTTP

        # Extrai os headers de retorno
        set_token = response.headers.get('set-token')           # ← minúsculo
        csrf_token = response.headers.get('x-csrf-token')       # ← minúsculo
        csrf_expiration = response.headers.get('x-csrf-expiration')  # ← minúsculo

        # Retorna os tokens como dicionário
        return {
            'set_token': set_token,
            'csrf_token': csrf_token
        }

    except requests.exceptions.RequestException as e:
        # Lógica para tratar erros de requisição, incluindo erros HTTP (4xx, 5xx)
        error_code = 'Unknown'
        error_message = str(e)

        if response is not None:
            # Se uma resposta foi recebida, tenta extrair informações
            error_code = response.status_code
            try:
                # Tenta decodificar a resposta como JSON para obter detalhes
                error_data = response.json()
                error_message = error_data.get('message', f"Erro HTTP {error_code}: {response.text}")
            except requests.exceptions.JSONDecodeError:
                # Se não for JSON, usa o texto da resposta
                error_message = f"Erro HTTP {error_code}: {response.text}"
            
        print(f"Erro de autenticação ({error_code}): {error_message}")
        return None

if __name__ == "__main__":
    authenticate()
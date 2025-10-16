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

    try:
        # Faz a requisição POST
        response = requests.post(base_url, headers=headers)
        response.raise_for_status()  # Levanta exceção se houver erro HTTP

        # Extrai os headers de retorno
        set_token = response.headers.get('Set-Token')
        csrf_token = response.headers.get('X-CSRF-Token')
        csrf_expiration = response.headers.get('X-CSRF-Expiration')

        # Exibe os valores no terminal
        print("Autenticação bem-sucedida!")
        print(f"Set-Token: {set_token}")
        print(f"X-CSRF-Token: {csrf_token}")
        print(f"X-CSRF-Expiration: {csrf_expiration}")

    except requests.exceptions.RequestException as e:
        error_code = response.json().get('code', 'Unknown') if 'response' in locals() else 'Unknown'
        error_message = response.json().get('message', str(e)) if 'response' in locals() else str(e)
        print(f"Erro de autenticação ({error_code}): {error_message}")

if __name__ == "__main__":
    authenticate()
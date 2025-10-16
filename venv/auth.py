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

    # Verifica se todas as variáveis estão presentes
    if not all([client_id, client_secret, role_type, base_url]):
        print("Erro: Variáveis de ambiente CLIENT_ID, CLIENT_SECRET, ROLE_TYPE ou PUCOMEX_BASE_URL ausentes.")
        return None
    
    # Define os headers para autenticação
    headers = {
        'Client-Id': client_id,
        'Client-Secret': client_secret,
        'Role-Type': role_type
    }
    
    response = None # Inicializa response para garantir que existe no escopo do except

    try:
        # Faz a requisição POST
        print(f"Tentando autenticar em: {base_url}")
        response = requests.post(base_url, headers=headers)
        response.raise_for_status()  # Levanta exceção se houver erro HTTP (4xx ou 5xx)

        # ----------------------------------------------------------------------
        # CORREÇÃO CRÍTICA: Lendo chaves de header com segurança (em minúsculas)
        # ----------------------------------------------------------------------
        lower_headers = {k.lower(): v for k, v in response.headers.items()}
        
        set_token = lower_headers.get('set-token')
        csrf_token = lower_headers.get('x-csrf-token')
        csrf_expiration = lower_headers.get('x-csrf-expiration')
        
        # Depuração: exibe todos os headers retornados
        print("\nHeaders retornados pela API (formato original):", dict(response.headers))

        # Exibe os valores no terminal
        print("\nAutenticação bem-sucedida! 🎉")
        print(f"Set-Token (capturado): {set_token[:30]}...") 
        print(f"X-CSRF-Token (capturado): {csrf_token[:30]}...")
        print(f"X-CSRF-Expiration: {csrf_expiration}")

        # Retorna os tokens como dicionário
        return {
            'set_token': set_token,
            'csrf_token': csrf_token
        }

    except requests.exceptions.RequestException as e:
        # Lógica para tratar erros de requisição
        error_code = 'No Response'
        error_message = str(e)

        if response is not None:
            error_code = response.status_code
            try:
                # Tenta decodificar a resposta como JSON
                error_data = response.json()
                error_message = error_data.get('message', f"Erro HTTP {error_code}: {response.text}")
            except requests.exceptions.JSONDecodeError:
                # Se não for JSON, usa o texto da resposta
                error_message = f"Erro HTTP {error_code}: {response.text}"
            
        print(f"\nErro de autenticação ({error_code}): {error_message}")
        return None

if __name__ == "__main__":
    authenticate()
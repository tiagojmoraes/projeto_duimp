import requests
import os
from dotenv import load_dotenv
import auth

# Carrega as variáveis do .env
load_dotenv()

def send_authenticated_request(tokens):
    if not tokens:
        print("Erro: Tokens de autenticação não fornecidos.")
        return None, None

    set_token = tokens['set_token']
    csrf_token = tokens['csrf_token']
    cookies = tokens.get('cookies')

    # Depuração: exibe os tokens obtidos
    print("--- Detalhes da Requisição ---")
    print(f"Set-Token usado: {set_token}")
    print(f"X-CSRF-Token usado: {csrf_token}")

    # Obtém as variáveis de endpoint
    end_point = os.getenv('END_POINT')
    duimp = os.getenv('DUIMP')
    duimp_version = os.getenv('DUIMP_VERSION')

    # Constrói a URL de consulta
    # Exemplo: https://api.exemplo.gov.br/duimp/v1/
    url = f"{end_point}/{duimp}/{duimp_version}"
    print(f"URL de destino: {url}")

    # Define os headers com os tokens
    headers = {
        'Authorization': f'Bearer {set_token}',
        'X-CSRF-Token': csrf_token
    }

    response = None
    new_tokens = None
    
    try:
        # Faz a requisição GET
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()  # Levanta exceção se houver erro HTTP

        # Verifica se a API retornou novos tokens (Renovação)
        new_set_token = response.headers.get('Set-Token')
        new_csrf_token = response.headers.get('X-CSRF-Token')
        
        if new_set_token and new_csrf_token:
            new_tokens = {
                'set_token': new_set_token,
                'csrf_token': new_csrf_token
            }
        
        # Exibe a resposta
        print("\nRequisição bem-sucedida! 🎉")
        print(f"Status Code: {response.status_code}")
        print(f"Resposta (parte): {response.text[:500]}...") # Limita a saída
        
        return response, new_tokens

    except requests.exceptions.RequestException as e:
        # Trata erros (conexão, timeout, HTTP 4xx/5xx)
        error_code = 'No Response'
        error_message = str(e)
        
        if response is not None:
            # Se uma resposta foi recebida (mesmo que erro HTTP)
            error_code = response.status_code
            try:
                # Tenta obter detalhes do erro do corpo JSON
                error_data = response.json()
                # A API retorna a mensagem de erro que vimos anteriormente
                error_message = error_data.get('message', f"Erro HTTP {error_code}: {response.text}")
            except requests.exceptions.JSONDecodeError:
                error_message = f"Erro HTTP {error_code}: {response.text}"

        print(f"\nErro na requisição ({error_code}): {error_message}")
        
        # Retorna None e None em caso de falha
        return None, None

if __name__ == "__main__":
    # 1. AUTENTICAÇÃO INICIAL
    print("Iniciando autenticação...")
    tokens = auth.authenticate()
    
    if tokens:
        # 2. FAZ A PRIMEIRA REQUISIÇÃO
        response, new_tokens = send_authenticated_request(tokens)
        
        if response is not None:
            # 3. EXIBE A RENOVAÇÃO DO TOKEN
            if new_tokens:
                print("\n✅ Tokens Renovados Encontrados na Resposta!")
                print("ESTES DEVEM SER USADOS NA PRÓXIMA CHAMADA:")
                print(f"Novo Set-Token: {new_tokens['set_token']}")
                print(f"Novo X-CSRF-Token: {new_tokens['csrf_token']}")
            else:
                print("\nℹ️ Não foram encontrados novos tokens de renovação nos headers.")
        else:
            print("\n🛑 A requisição falhou. Não é possível verificar a renovação do token.")
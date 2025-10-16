import requests
import os
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

def send_authenticated_request():
    # Obtém os tokens salvos
    set_token = os.getenv('SET_TOKEN')
    csrf_token = os.getenv('X_CSRF_TOKEN')

    # Obtém as variáveis de endpoint
    end_point = os.getenv('END_POINT')
    duimp = os.getenv('DUIMP')
    duimp_version = os.getenv('DUIMP_VERSION')

    # Constrói a URL de consulta
    url = f"{end_point}/{duimp}/{duimp_version}"

    # Define os headers com os tokens
    headers = {
        'Authorization': f'Bearer {set_token}',
        'X-CSRF-Token': csrf_token
    }

    try:
        # Faz a requisição POST (ajuste o body conforme necessário)
        response = requests.post(url, headers=headers, json={})
        response.raise_for_status()  # Levanta exceção se houver erro HTTP

        # Exibe a resposta
        print("Requisição bem-sucedida!")
        print(f"Resposta: {response.text}")

    except requests.exceptions.RequestException as e:
        error_code = response.json().get('code', 'Unknown') if 'response' in locals() else 'Unknown'
        error_message = response.json().get('message', str(e)) if 'response' in locals() else str(e)
        print(f"Erro na requisição ({error_code}): {error_message}")

if __name__ == "__main__":
    send_authenticated_request()
import requests
import os
from dotenv import load_dotenv
import auth
from urllib.parse import quote  # ← Adicione esta importação

load_dotenv()

def send_authenticated_request():
    tokens = auth.authenticate()
    if not tokens:
        print("Falha na autenticação. Verifique as credenciais.")
        return

    set_token = tokens['set_token']
    csrf_token = tokens['csrf_token']

    end_point = os.getenv('END_POINT')
    duimp = os.getenv('DUIMP')
    duimp_version = os.getenv('DUIMP_VERSION')
    # url = f"{end_point}/{duimp}/{duimp_version}"
    url = f"{end_point}/{duimp}/{duimp_version}/itens"

    headers = {
        'Authorization': set_token,
        'x-csrf-token': csrf_token
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        if 'response' in locals() and response is not None:
            try:
                error_data = response.json()
                error_code = error_data.get('code', 'Unknown')
                error_message = error_data.get('message', str(e))
            except requests.exceptions.JSONDecodeError:
                error_code = response.status_code
                error_message = f"Não foi possível decodificar JSON do erro. Texto da resposta: {response.text}"
        else:
            error_code = 'No Response'
            error_message = str(e)

        print(f"Erro na requisição ({error_code}): {error_message}")

if __name__ == "__main__":
    send_authenticated_request()
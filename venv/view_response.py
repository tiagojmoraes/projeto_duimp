import json
from send_request import send_authenticated_request

def save_response_to_file():
    # Modifica temporariamente para retornar a resposta
    import requests
    import os
    from dotenv import load_dotenv
    import auth
    
    load_dotenv()
    
    tokens = auth.authenticate()
    if not tokens:
        print("Falha na autenticação.")
        return
    
    set_token = tokens['set_token']
    csrf_token = tokens['csrf_token']
    
    end_point = os.getenv('END_POINT')
    duimp_number = os.getenv('DUIMP')
    duimp_version = os.getenv('DUIMP_VERSION')
    # url = f"{end_point}/{duimp_number}/{duimp_version}" # Consultar DUIMP já registrada - FUNCIONA
    # url = f"{end_point}/{duimp_number}/{duimp_version}/itens" # Consultar os itens de uma DUIMP já registrada - FUNCIONA
    # url = f"{end_point}/{duimp_number}/{duimp_version}/valores-calculados" # Consultar valores calculados da  - NÃO FUNCIONA
    # url = f"{end_point}/{duimp_number}/{duimp_version}/diagnosticos" # Obter dados do diagnostico da DUIMP - FUNCIONA
    # url = f"{end_point}/{duimp_number}/versoes" # Consultar a versão de uma DUIMP já  - FUNCIONA
    
    headers = {
        'Authorization': set_token,
        'x-csrf-token': csrf_token
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Salva em arquivo
        with open('api_response.json', 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, indent=2, ensure_ascii=False)
        
        print("✅ Resposta salva em 'api_response.json'")
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    save_response_to_file()
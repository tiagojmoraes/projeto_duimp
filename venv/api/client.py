import requests
from auth.oauth import PUCOMEXAuthenticator

class PUCOMEXClient:
    def __init__(self):
        self.auth = PUCOMEXAuthenticator()
        self.base_url = self.auth.base_url
        self.session = requests.Session()
        self._update_headers()

    def _update_headers(self):
        """Atualiza headers com tokens."""
        self.session.headers.update(self.auth.get_headers())

    def get_duimp_extrato(self, duimp_id: str):
        """Consulta o extrato da DUIMP pelo ID."""
        endpoint = f"{self.base_url}/duimp/v1/duimpas/{duimp_id}/extrato"
        try:
            response = self.session.get(endpoint)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_code = response.json().get('code', 'Unknown') if response else 'Unknown'
            error_message = response.json().get('message', str(e)) if response else str(e)
            raise Exception(f"Erro na consulta da DUIMP ({error_code}): {error_message}")
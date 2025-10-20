# database_handler.py
import sqlite3
import json
import os
from converter_table_json import get_table_json

class DatabaseHandler:
    def __init__(self, db_name='duimp_identificacao.db'):
        self.db_path = db_name
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_table()
    
    def create_table(self):
        create_query = """
        CREATE TABLE IF NOT EXISTS duimp_identificacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataInsercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            duimpNumero TEXT,
            duimpVersao TEXT,
            dataRegistro TEXT,
            chaveAcesso TEXT,
            cnpjImportador TEXT,
            canalParametrizacao TEXT,
            tipoIdentificacaoCarga TEXT,
            cargaIdentificacao TEXT,
            seguroMoedaNegociada TEXT,
            seguroValorNegociada REAL,
            freteMoedaNegociada TEXT,
            freteValorNegociada REAL,
            valorMercadoriaBRL REAL,
            codFilial TEXT,
            table_json JSON
        )
        """
        self.cursor.execute(create_query)
        self.conn.commit()
    
    def get_cod_filial(self, cnpj):
        if cnpj == "85090033001366":
            return "18"
        else:
            return "Cadastrar Código"
    
    def extract_specific_fields(self, data):
        items = {}
        
        identificacao = data.get('identificacao', {})
        if identificacao:
            items['duimpNumero'] = identificacao.get('numero')
            items['duimpVersao'] = identificacao.get('versao')
            items['dataRegistro'] = identificacao.get('dataRegistro')
            items['chaveAcesso'] = identificacao.get('chaveAcesso')
        
        importador = identificacao.get('importador', {})
        if importador:
            cnpj = importador.get('ni')
            items['cnpjImportador'] = cnpj
            items['codFilial'] = self.get_cod_filial(cnpj)
        
        resultado_analise = data.get('resultadoAnaliseRisco', {})
        if resultado_analise:
            items['canalParametrizacao'] = resultado_analise.get('canalConsolidado')
        
        carga = data.get('carga', {})
        if carga:
            items['tipoIdentificacaoCarga'] = carga.get('tipoIdentificacaoCarga')
            items['cargaIdentificacao'] = carga.get('identificacao')
            
            seguro = carga.get('seguro', {})
            if seguro:
                items['seguroMoedaNegociada'] = seguro.get('codigoMoedaNegociada')
                items['seguroValorNegociada'] = seguro.get('valorMoedaNegociada')
            
            frete = carga.get('frete', {})
            if frete:
                items['freteMoedaNegociada'] = frete.get('codigoMoedaNegociada')
                items['freteValorNegociada'] = frete.get('valorMoedaNegociada')
        
        tributos = data.get('tributos', {})
        if tributos:
            mercadoria = tributos.get('mercadoria', {})
            if mercadoria:
                items['valorMercadoriaBRL'] = mercadoria.get('valorTotalLocalEmbarqueBRL')
        
        return items
    
    def insert_extracted_data(self, extracted_data):
        try:
            columns = []
            placeholders = []
            values = []
            
            for key, value in extracted_data.items():
                columns.append(key)
                placeholders.append('?')
                values.append(value)
            
            query = f"INSERT INTO duimp_identificacao ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            self.cursor.execute(query, values)
            self.conn.commit()
            
            self.update_table_json()
            
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            return None
    
    def update_table_json(self):
        try:
            table_json = get_table_json(self.db_path, 'duimp_identificacao', exclude_columns=['table_json'])
            
            if table_json:
                self.cursor.execute("UPDATE duimp_identificacao SET table_json = ?", (table_json,))
                self.conn.commit()
        except Exception as e:
            pass
    
    def get_table_json_content(self):
        """Retorna o conteúdo do table_json do primeiro registro - APENAS OS PRINTS SOLICITADOS"""
        try:
            self.cursor.execute("SELECT table_json FROM duimp_identificacao LIMIT 1")
            result = self.cursor.fetchone()
            if result and result[0]:
                table_json_data = json.loads(result[0])
                
                # APENAS ESTES PRINTS:
                print("🗂️ ESTRUTURA DO TABLE_JSON:")
                print(f"   📋 Tabela: {table_json_data.get('metadata', {}).get('table_name')}")
                print()
                print("   📝 PRIMEIRO REGISTRO:")
                
                records = table_json_data.get('data', [])
                if records:
                    first_record = records[0]
                    
                    # Ordem específica dos campos como solicitado
                    fields_order = [
                        'id', 'dataInsercao', 'duimpNumero', 'duimpVersao', 
                        'dataRegistro', 'chaveAcesso', 'cnpjImportador', 'codFilial',
                        'canalParametrizacao', 'tipoIdentificacaoCarga', 'cargaIdentificacao',
                        'seguroMoedaNegociada', 'seguroValorNegociada', 'freteMoedaNegociada',
                        'freteValorNegociada', 'valorMercadoriaBRL'
                    ]
                    
                    for field in fields_order:
                        if field in first_record:
                            value = first_record[field]
                            icon = "🏷️" if field == 'codFilial' else "✅"
                            print(f"      {icon} {field}: {value}")
                
                return table_json_data
            return None
        except sqlite3.Error:
            return None
    
    def close(self):
        self.conn.close()

def find_json_file():
    current_dir = os.getcwd()
    possible_locations = [
        os.path.join(current_dir, 'api_response.json'),
        'api_response.json'
    ]
    
    for location in possible_locations:
        if os.path.exists(location):
            return location
    return None

def process_json_file(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        db = DatabaseHandler()
        extracted_data = db.extract_specific_fields(data)
        inserted_id = db.insert_extracted_data(extracted_data)
        
        if inserted_id:
            # APENAS ESTA CHAMADA PARA MOSTRAR OS PRINTS SOLICITADOS
            db.get_table_json_content()
        
        db.close()
        return extracted_data
                
    except Exception:
        return None

if __name__ == "__main__":
    json_file = find_json_file()
    
    if json_file:
        process_json_file(json_file)
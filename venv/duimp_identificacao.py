import sqlite3
import json
import os
from converter_table_json import get_table_json


class DatabaseHandler:
    def __init__(self, db_name='duimp_identificacao.db'):
        self.db_path = db_name
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # Não criar tabela automaticamente

    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            self.conn.close()

    def create_table(self):
        create_query = """
        CREATE TABLE IF NOT EXISTS duimp_identificacao (
            dataInsercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            duimpNumero TEXT,
            duimpVersao TEXT,
            dataRegistro TEXT,
            chaveAcesso TEXT,
            cnpjImportador TEXT,
            codFilial TEXT,
            canalParametrizacao TEXT,
            tipoIdentificacaoCarga TEXT,
            cargaIdentificacao TEXT,
            taxaSiscomex REAL,
            dataLibDuimp TEXT,
            pesoBruto REAL,
            table_json JSON
        )
        """
        self.cursor.execute(create_query)
        self.conn.commit()

    def format_cnpj(self, cnpj):
        if not cnpj:
            return cnpj

        # Remove tudo que não for dígito
        cnpj_clean = ''.join(filter(str.isdigit, str(cnpj)))

        # Garante 14 dígitos (completa com zeros à esquerda se necessário)
        if len(cnpj_clean) < 14:
            cnpj_clean = cnpj_clean.zfill(14)
        elif len(cnpj_clean) > 14:
            cnpj_clean = cnpj_clean[-14:]

        # Formatação padrão: XX.XXX.XXX/XXXX-XX
        return f"{cnpj_clean[:2]}.{cnpj_clean[2:5]}.{cnpj_clean[5:8]}/{cnpj_clean[8:12]}-{cnpj_clean[12:]}"

    def get_cod_filial(self, cnpj):
        cnpj_clean = ''.join(filter(str.isdigit, str(cnpj)))
        if cnpj_clean == "85090033001366":
            return "18"
        else:
            return "Cadastrar Código"

    def extract_taxa_siscomex(self, data):
        tributos_calculados = data.get('tributos', {}).get('tributosCalculados', [])

        for tributo in tributos_calculados:
            if tributo.get('tipo') == 'TAXA_UTILIZACAO':
                valores_brl = tributo.get('valoresBRL', {})
                return valores_brl.get('recolhido')

        return None

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
            # Aplica formatação ao CNPJ
            items['cnpjImportador'] = self.format_cnpj(cnpj)
            items['codFilial'] = self.get_cod_filial(cnpj)

        resultado_analise = data.get('resultadoAnaliseRisco', {})
        if resultado_analise:
            items['canalParametrizacao'] = resultado_analise.get('canalConsolidado')

        carga = data.get('carga', {})
        if carga:
            items['tipoIdentificacaoCarga'] = carga.get('tipoIdentificacaoCarga')
            items['cargaIdentificacao'] = carga.get('identificacao')

        # Extrair taxa Siscomex
        items['taxaSiscomex'] = self.extract_taxa_siscomex(data)

        return items

    def insert_extracted_data(self, extracted_data, manual_data=None):
        try:
            # Criar tabela apenas quando for inserir dados
            self.create_table()
            
            # Se manual_data não foi fornecido, usar dados vazios
            if manual_data is None:
                manual_data = {}
            
            # Combinar dados extraídos + manuais
            all_data = {**extracted_data, **manual_data}
            
            columns = []
            placeholders = []
            values = []

            for key, value in all_data.items():
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
            print(f"Erro ao inserir dados: {e}")
            return None

    def update_table_json(self):
        try:
            # Verificar se a tabela existe e tem dados
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='duimp_identificacao'")
            if not self.cursor.fetchone():
                return
                
            table_json = get_table_json(self.db_path, 'duimp_identificacao', exclude_columns=['table_json'])

            if table_json:
                self.cursor.execute("UPDATE duimp_identificacao SET table_json = ?", (table_json,))
                self.conn.commit()
        except Exception as e:
            print(f"Erro ao atualizar table_json: {e}")

    def get_table_json_content(self):
        try:
            self.cursor.execute("SELECT table_json FROM duimp_identificacao LIMIT 1")
            result = self.cursor.fetchone()
            if result and result[0]:
                table_json_data = json.loads(result[0])

                records = table_json_data.get('data', [])
                if records:
                    first_record = records[0]

                    # Ordem específica dos campos como solicitado
                    fields_order = [
                        'id', 'dataInsercao', 'duimpNumero', 'duimpVersao',
                        'dataRegistro', 'chaveAcesso', 'cnpjImportador', 'codFilial',
                        'canalParametrizacao', 'tipoIdentificacaoCarga', 'cargaIdentificacao',
                        'taxaSiscomex', 'dataLibDuimp', 'pesoBruto'
                    ]
                return table_json_data
            return None
        except sqlite3.Error as e:
            print(f"Erro ao ler table_json: {e}")
            return None

    def get_manual_fields(self):
        """Para uso via terminal - coleta campos manualmente"""
        print("\n" + "="*50)
        print("FORMULÁRIO DE DADOS ADICIONAIS")
        print("="*50)
        
        data_lib_duimp = input("Data Liberação DUIMP (YYYY-MM-DD): ").strip()
        
        peso_bruto = input("Peso Bruto (kg): ").strip()
        peso_bruto = float(peso_bruto) if peso_bruto and peso_bruto.replace('.', '').replace('-', '').isdigit() else None
    
        return {
            'dataLibDuimp': data_lib_duimp if data_lib_duimp else None,
            'pesoBruto': peso_bruto
        }


# FUNÇÕES FORA DA CLASSE
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
        db.close()
        
        # Verificar se extraiu dados essenciais
        if extracted_data and extracted_data.get('duimpNumero'):
            print("✅ Dados extraídos com sucesso!")
            return extracted_data
        else:
            print("❌ Dados extraídos estão incompletos")
            return None

    except Exception as e:
        print(f"❌ Erro ao processar JSON: {e}")
        return None


if __name__ == "__main__":
    json_file = find_json_file()

    if json_file:
        print(f"Arquivo encontrado: {json_file}")
        
        # Extrair dados do JSON
        extracted_data = process_json_file(json_file)
        
        if extracted_data:
            print("✅ Dados extraídos com sucesso!")
            print("🌐 Abrindo formulário web...")
            
            # Importar e executar o formulário web
            try:
                from formulario_web import mostrar_formulario
                mostrar_formulario(extracted_data)
            except ImportError as e:
                print(f"❌ Erro ao importar formulário web: {e}")
                print("📋 Voltando para formulário do terminal...")
                # Fallback para terminal
                db = DatabaseHandler()
                db.insert_extracted_data(extracted_data)
                db.close()
    else:
        print("Arquivo 'api_response.json' não encontrado.")
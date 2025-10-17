import sqlite3
import json
from datetime import datetime
import os
import sys

class DatabaseHandler:
    def __init__(self, db_name='duimp_data.db'):
        """Inicializa a conexão com o banco de dados SQLite"""
        self.db_path = db_name
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # print(f"📁 Banco de dados: {os.path.abspath(self.db_path)}")
    
    def extract_specific_fields(self, data):
        """
        Extrai APENAS as tags específicas solicitadas, ignorando todo o resto
        """
        items = {}
        
        # Identificação
        identificacao = data.get('identificacao', {})
        if identificacao:
            items['duimpNumero'] = identificacao.get('numero')
            items['duimpVersao'] = identificacao.get('versao')
            items['dataRegistro'] = identificacao.get('dataRegistro')
            items['chaveAcesso'] = identificacao.get('chaveAcesso')
        
        # Importador (dentro de identificacao)
        importador = identificacao.get('importador', {})
        if importador:
            items['cnpjImportador'] = importador.get('ni')
        
        # Resultado da Análise de Risco
        resultado_analise = data.get('resultadoAnaliseRisco', {})
        if resultado_analise:
            items['canalParametrizacao'] = resultado_analise.get('canalConsolidado')
        
        # Carga
        carga = data.get('carga', {})
        if carga:
            items['tipoIdentificacaoCarga'] = carga.get('tipoIdentificacaoCarga')
            items['cargaIdentificacao'] = carga.get('identificacao')
            
            # Seguro
            seguro = carga.get('seguro', {})
            if seguro:
                items['seguroMoedaNegociada'] = seguro.get('codigoMoedaNegociada')
                items['seguroValorNegociada'] = seguro.get('valorMoedaNegociada')
            
            # Frete
            frete = carga.get('frete', {})
            if frete:
                items['freteMoedaNegociada'] = frete.get('codigoMoedaNegociada')
                items['freteValorNegociada'] = frete.get('valorMoedaNegociada')
        
        # Tributos
        tributos = data.get('tributos', {})
        if tributos:
            # Mercadoria
            mercadoria = tributos.get('mercadoria', {})
            if mercadoria:
                items['valorMercadoriaBRL'] = mercadoria.get('valorTotalLocalEmbarqueBRL')
            
            # Tributos Calculados - campos dinâmicos por tipo
            tributos_calculados = tributos.get('tributosCalculados', [])
            for tributo in tributos_calculados:
                tipo = tributo.get('tipo')
                valores_brl = tributo.get('valoresBRL', {})
                recolhido = valores_brl.get('recolhido')
                
                if tipo:
                    # Cria campo específico para cada tipo de tributo
                    field_name = f"{tipo}_recolhido"
                    items[field_name] = recolhido
        
        return items
    
    def get_exclude_list(self):
        """
        Lista de exclusão para referência (não usada na extração, apenas para documentação)
        """
        return [
            'situacao',
            'equipesTrabalho',
            'resultadoAnaliseRisco_resultadoRFB',
            'resultadoAnaliseRisco_resultadoAnuente',
            'documentos',
            'adicoes',
            'pagamentos',
            'tratamentoAdministrativo',
            'quantidadeItens',
            'itens',
            'identificacao_importador_tipoImportador',
            'identificacao_responsavelRegistroNumero',
            'identificacao_informacaoComplementar',
            'carga_unidadeDeclarada',
            'carga_paisProcedencia',
            'carga_motivoSituacaoEspecial',
            'tributos_mercadoria_valorTotalLocalEmbarqueUSD'
        ]
    
    def create_table_from_data(self, flat_data):
        """Cria a tabela dinamicamente baseada apenas nos campos específicos"""
        # Adiciona campo de ID e timestamp
        columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 
                   'data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP']
        
        # Cria colunas para cada campo específico
        for key in flat_data.keys():
            # Define tipo de dado baseado no valor
            value = flat_data[key]
            if isinstance(value, (int, float)):
                col_type = 'REAL'
            else:
                col_type = 'TEXT'
            
            columns.append(f"{key} {col_type}")
        
        # Adiciona coluna para JSON completo (opcional)
        columns.append('json_completo TEXT')
        
        # Cria a tabela
        create_query = f"""
        CREATE TABLE IF NOT EXISTS duimp (
            {', '.join(columns)}
        )
        """
        
        self.cursor.execute(create_query)
        self.conn.commit()
        print("✅ Tabela 'duimp' criada/verificada com sucesso!")
        print(f"📊 Total de colunas: {len(columns)}")
    
    def insert_data(self, json_data):
        """Insere os dados do JSON na tabela usando apenas campos específicos"""
        # Extrai apenas os campos específicos
        specific_data = self.extract_specific_fields(json_data)
        
        # Mostra os campos que serão incluídos
        self.show_included_fields(specific_data)
        
        # Cria a tabela se não existir
        self.create_table_from_data(specific_data)
        
        # Prepara os dados para inserção
        columns = list(specific_data.keys())
        values = list(specific_data.values())
        
        # Adiciona o JSON completo
        columns.append('json_completo')
        values.append(json.dumps(json_data, ensure_ascii=False))
        
        # Cria a query de inserção
        placeholders = ', '.join(['?' for _ in values])
        columns_str = ', '.join(columns)
        
        insert_query = f"INSERT INTO duimp ({columns_str}) VALUES ({placeholders})"
        
        try:
            self.cursor.execute(insert_query, values)
            self.conn.commit()
            print("✅ Dados inseridos com sucesso!")
            print(f"📝 Registro ID: {self.cursor.lastrowid}")
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"❌ Erro ao inserir dados: {e}")
            return None
    
    def show_included_fields(self, specific_data):
        """Mostra quais campos estão sendo incluídos"""
        print(f"\n🎯 CAMPOS ESPECÍFICOS INCLUÍDOS ({len(specific_data)} campos):")
        print("=" * 70)
        
        # Agrupa por categoria para melhor visualização
        categories = {
            'Identificação': [],
            'Importador': [],
            'Análise de Risco': [],
            'Carga': [],
            'Tributos': [],
            'Tributos Calculados': []
        }
        
        for key, value in specific_data.items():
            if key.startswith('identificacao_') and not key.startswith('identificacao_importador'):
                categories['Identificação'].append((key, value))
            elif key.startswith('importador_'):
                categories['Importador'].append((key, value))
            elif key.startswith('resultadoAnaliseRisco_'):
                categories['Análise de Risco'].append((key, value))
            elif key.startswith('carga_'):
                categories['Carga'].append((key, value))
            elif key.startswith('tributos_mercadoria'):
                categories['Tributos'].append((key, value))
            elif key.startswith('tributo_'):
                categories['Tributos Calculados'].append((key, value))
        
        for category, fields in categories.items():
            if fields:
                print(f"\n📁 {category}:")
                for field, value in fields:
                    print(f"   ✅ {field:45} = {value}")
        
        print("=" * 70)
    
    def show_excluded_fields_info(self):
        """Mostra informações sobre campos excluídos"""
        exclude_list = self.get_exclude_list()
        print(f"\n📋 LISTA DE EXCLUSÃO ({len(exclude_list)} categorias):")
        print("-" * 50)
        for field in sorted(exclude_list):
            print(f"   ❌ {field}")
        print("-" * 50)
    
    def get_all_records(self):
        """Retorna todos os registros da tabela"""
        try:
            self.cursor.execute("SELECT * FROM duimp")
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Converte para lista de dicionários
            records = []
            for row in rows:
                records.append(dict(zip(columns, row)))
            
            return records
        except sqlite3.Error as e:
            print(f"❌ Erro ao buscar registros: {e}")
            return []
    
    def get_record_by_numero(self, numero_duimp):
        """Busca um registro específico pelo número da DUIMP"""
        try:
            self.cursor.execute(
                "SELECT * FROM duimp WHERE identificacao_numero = ?", 
                (numero_duimp,)
            )
            columns = [description[0] for description in self.cursor.description]
            row = self.cursor.fetchone()
            
            if row:
                return dict(zip(columns, row))
            return None
        except sqlite3.Error as e:
            print(f"❌ Erro ao buscar registro: {e}")
            return None
    
    def show_table_structure(self):
        """Mostra a estrutura da tabela"""
        try:
            self.cursor.execute("PRAGMA table_info(duimp)")
            columns = self.cursor.fetchall()
            
            print("\n📋 ESTRUTURA DA TABELA 'duimp':")
            print("=" * 60)
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                # Destaca colunas de tributos dinâmicos
                if col_name.startswith('tributo_'):
                    print(f"  🎯 {col_name:38} {col_type} (TRIBUTO DINÂMICO)")
                else:
                    print(f"  ✅ {col_name:38} {col_type}")
            print("=" * 60)
            print(f"Total de colunas: {len(columns)}\n")
            
            return columns
        except sqlite3.Error as e:
            print(f"❌ Erro ao obter estrutura: {e}")
            return []
    
    def close(self):
        """Fecha a conexão com o banco de dados"""
        self.conn.close()
        print("🔒 Conexão com banco de dados fechada.")


def find_json_file():
    """Procura o arquivo JSON em vários locais possíveis"""
    current_dir = os.getcwd()
    project_root = os.path.dirname(current_dir) if current_dir.endswith('/venv') else current_dir
    
    possible_locations = [
        os.path.join(project_root, 'api_response.json'),
        os.path.join(current_dir, 'api_response.json'),
        os.path.join(project_root, 'data', 'api_response.json'),
        os.path.join(project_root, 'json', 'api_response.json'),
    ]
    
    for location in possible_locations:
        if os.path.exists(location):
            return location
    
    return None


# Função auxiliar para processar o arquivo JSON
def process_json_file(json_file_path):
    """Lê o arquivo JSON e insere no banco de dados"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        db = DatabaseHandler()
        
        # Mostra informações sobre exclusões
        db.show_excluded_fields_info()
        
        # Insere os dados (apenas campos específicos)
        record_id = db.insert_data(data)
        
        if record_id:
            db.show_table_structure()
            print(f"\n✅ PROCESSAMENTO CONCLUÍDO!")
            print(f"📊 Número DUIMP: {data.get('identificacao', {}).get('numero', 'N/A')}")
            
            # Mostra totais de tributos
            specific_data = db.extract_specific_fields(data)
            tributos_total = sum(value for key, value in specific_data.items() 
                               if key.startswith('tributo_') and value is not None)
            print(f"💰 Total de tributos recolhidos: R$ {tributos_total:,.2f}")
        
        db.close()
        return record_id
        
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {json_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Erro ao decodificar JSON do arquivo: {json_file_path}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return None


if __name__ == "__main__":
    # Procura o arquivo JSON
    json_file = find_json_file()
    
    if json_file:
        print(f"🚀 Arquivo encontrado: {json_file}")
        process_json_file(json_file)
    else:
        print("❌ Arquivo api_response.json não encontrado!")
        
        # Diagnóstico
        current_dir = os.getcwd()
        project_root = os.path.dirname(current_dir) if current_dir.endswith('/venv') else current_dir
        
        print(f"\n🔍 Diagnóstico:")
        print(f"📂 Diretório atual: {current_dir}")
        print(f"📁 Raiz do projeto: {project_root}")
        
        # Tenta criar arquivo de exemplo
        try:
            sample_data = {
                "identificacao": {
                    "numero": "25BR00001883412",
                    "versao": 1,
                    "dataRegistro": "2025-10-09T08:43:49-0300",
                    "chaveAcesso": "25Nkq000700600",
                    "importador": {
                        "ni": "85090033001366"
                    }
                },
                "resultadoAnaliseRisco": {
                    "canalConsolidado": "VERDE"
                },
                "carga": {
                    "tipoIdentificacaoCarga": "RUC",
                    "identificacao": "5BRIMP26681388200010000000009972",
                    "seguro": {
                        "codigoMoedaNegociada": "BRL",
                        "valorMoedaNegociada": 86.97
                    },
                    "frete": {
                        "codigoMoedaNegociada": "EUR",
                        "valorMoedaNegociada": 387.6
                    }
                },
                "tributos": {
                    "mercadoria": {
                        "valorTotalLocalEmbarqueBRL": 100395.07
                    },
                    "tributosCalculados": [
                        {
                            "tipo": "II",
                            "valoresBRL": {
                                "recolhido": 16322.14
                            }
                        },
                        {
                            "tipo": "IPI", 
                            "valoresBRL": {
                                "recolhido": 8868.22
                            }
                        }
                    ]
                }
            }
            
            sample_file = os.path.join(project_root, 'api_response.json')
            with open(sample_file, 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, ensure_ascii=False, indent=2)
            print(f"✅ Arquivo de exemplo criado: {sample_file}")
            process_json_file(sample_file)
            
        except Exception as e:
            print(f"❌ Erro ao criar arquivo de exemplo: {e}")
            print(f"\n💡 Solução manual:")
            print(f"1. Execute: cd {project_root} && python view_response.py")
            print(f"2. Depois: python venv/database_handler.py")
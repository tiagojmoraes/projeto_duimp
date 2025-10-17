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
    
    def extract_adicoes_items(self, data):
        """
        Extrai as adições e itens em formato normalizado - uma linha por item
        O numeroItem é uma sequência numérica reiniciando a cada adição (1, 2, 3...)
        """
        adicoes_items = []
        
        adicoes = data.get('adicoes', [])
        for adicao in adicoes:
            numero_adicao = adicao.get('numero')
            itens = adicao.get('itens', [])
            
            # Para cada adição, cria itens sequenciais começando do 1
            for i, item_original in enumerate(itens, 1):
                item_data = {
                    'adiciaoNumero': numero_adicao,
                    'numeroItem': i  # Sequência: 1, 2, 3... para cada adição
                }
                adicoes_items.append(item_data)
        
        return adicoes_items
    
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
    
    def show_excluded_fields_info(self):
        """Mostra informações sobre campos excluídos"""
        exclude_list = self.get_exclude_list()
        print(f"\n📋 LISTA DE EXCLUSÃO ({len(exclude_list)} categorias):")
        print("-" * 50)
        for field in sorted(exclude_list):
            print(f"   ❌ {field}")
        print("-" * 50)
    
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
        
        # Adiciona colunas para adições e itens
        columns.append('adiciaoNumero INTEGER')
        columns.append('numeroItem INTEGER')
              
        # Cria a tabela
        create_query = f"""
        CREATE TABLE IF NOT EXISTS duimp (
            {', '.join(columns)}
        )
        """
        
        self.cursor.execute(create_query)
        self.conn.commit()
           
    def insert_data(self, json_data):
        """Insere os dados do JSON na tabela criando uma linha por item de adição"""
        # Extrai os campos específicos básicos
        specific_data = self.extract_specific_fields(json_data)
        
        # Extrai as adições e itens em formato normalizado
        adicoes_items = self.extract_adicoes_items(json_data)
        
        # Cria a tabela se não existir
        self.create_table_from_data(specific_data)
        
        inserted_ids = []
        
        if not adicoes_items:
            # Se não houver adições, insere pelo menos uma linha com os dados básicos
            combined_data = specific_data.copy()
            combined_data['adiciaoNumero'] = None
            combined_data['numeroItem'] = None
            
            # Prepara os dados para inserção
            columns = list(combined_data.keys())
            values = list(combined_data.values())
                       
            # Cria a query de inserção
            placeholders = ', '.join(['?' for _ in values])
            columns_str = ', '.join(columns)
            
            insert_query = f"INSERT INTO duimp ({columns_str}) VALUES ({placeholders})"
            
            try:
                self.cursor.execute(insert_query, values)
                inserted_ids.append(self.cursor.lastrowid)
            except sqlite3.Error as e:
                print(f"❌ Erro ao inserir dados: {e}")
                return None
        else:
            # Para cada item das adições, cria uma linha separada
            for item_data in adicoes_items:
                # Combina dados específicos com dados do item
                combined_data = specific_data.copy()
                combined_data.update(item_data)
                
                # Prepara os dados para inserção
                columns = list(combined_data.keys())
                values = list(combined_data.values())
                                
                # Cria a query de inserção
                placeholders = ', '.join(['?' for _ in values])
                columns_str = ', '.join(columns)
                
                insert_query = f"INSERT INTO duimp ({columns_str}) VALUES ({placeholders})"
                
                try:
                    self.cursor.execute(insert_query, values)
                    inserted_ids.append(self.cursor.lastrowid)
                except sqlite3.Error as e:
                    print(f"❌ Erro ao inserir dados do item {item_data.get('numeroItem')}: {e}")
                    return None
        
        self.conn.commit()
        return inserted_ids
    
    def show_table_structure(self):
        """Mostra a estrutura da tabela de forma limpa e organizada"""
        try:
            self.cursor.execute("PRAGMA table_info(duimp)")
            columns = self.cursor.fetchall()
            
            print("\n📊 ESTRUTURA DA TABELA 'duimp_data':")
            print("=" * 55)
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                # Define ícones específicos para cada tipo de coluna
                if col_name in ['adiciaoNumero', 'numeroItem']:
                    icon = "🔥"  # Novos campos
                elif col_name in ['id', 'data_insercao']:
                    icon = "⏰"  # Campos de sistema
                elif 'recolhido' in col_name:
                    icon = "💰"  # Campos de tributos
                elif 'duimp' in col_name.lower():
                    icon = "📝"  # Identificação da DUIMP
                elif 'cnpj' in col_name.lower():
                    icon = "🏢"  # Importador
                elif 'carga' in col_name.lower() or 'frete' in col_name.lower() or 'seguro' in col_name.lower():
                    icon = "📦"  # Dados de carga
                else:
                    icon = "✅"  # Demais campos
                
                print(f"  {icon} {col_name:35} {col_type}")
            
            print("=" * 55)
            print(f"🎯 Total de colunas: {len(columns)}")
            
            return columns
        except sqlite3.Error as e:
            print(f"❌ Erro ao obter estrutura: {e}")
            return []
    
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
                "SELECT * FROM duimp WHERE duimpNumero = ?", 
                (numero_duimp,)
            )
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Converte para lista de dicionários
            records = []
            for row in rows:
                records.append(dict(zip(columns, row)))
            
            return records
        except sqlite3.Error as e:
            print(f"❌ Erro ao buscar registro: {e}")
            return None
    
    def close(self):
        """Fecha a conexão com o banco de dados"""
        self.conn.close()


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


def process_json_file(json_file_path):
    """Lê o arquivo JSON e insere no banco de dados"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        db = DatabaseHandler()
        
        # Insere os dados (apenas campos específicos)
        record_ids = db.insert_data(data)
        
        if record_ids:
            db.show_table_structure()
            print(f"📈 Total de linhas inseridas: {len(record_ids)}")
        
        db.close()
        return record_ids
        
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
        process_json_file(json_file)
    else:
        print("❌ Arquivo api_response.json não encontrado!")

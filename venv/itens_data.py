import sqlite3
import json
import os
import sys
from datetime import datetime

class ItensDatabaseHandler:
    def __init__(self, db_name='itens_data.db'):
        """Inicializa a conexão com o banco de dados SQLite"""
        self.db_path = db_name
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"📁 Banco de dados: {os.path.abspath(self.db_path)}")
    
    def extract_fields(self, item_data):
        """
        Extrai os campos específicos conforme a lista fornecida
        """
        items = {}
        
        # Identificação - seguindo padrão da tabela duimp
        identificacao = item_data.get('identificacao', {})
        if identificacao:
            items['duimpNumero'] = identificacao.get('numero')
            items['duimpVersao'] = identificacao.get('versao')
            items['numeroItem'] = identificacao.get('numeroItem')
        
        # Produto
        produto = item_data.get('produto', {})
        if produto:
            items['codigoProduto'] = produto.get('codigo')
            items['versaoProduto'] = produto.get('versao')
            items['ncm'] = produto.get('ncm')
        
        # Exportador
        exportador = item_data.get('exportador', {})
        if exportador:
            items['codigoExportador'] = exportador.get('codigo')
        
        # Mercadoria
        mercadoria = item_data.get('mercadoria', {})
        if mercadoria:
            items['quantidadeComercial'] = mercadoria.get('quantidadeComercial')
            items['pesoLiquido'] = mercadoria.get('pesoLiquido')
            items['descricaoMercadoria'] = mercadoria.get('descricao')
            
            # Moeda negociada
            moeda_negociada = mercadoria.get('moedaNegociada', {})
            if moeda_negociada:
                items['moedaNegociada'] = moeda_negociada.get('codigo')
                items['valorUnitarioMoedaNegociada'] = mercadoria.get('valorUnitarioMoedaNegociada')
        
        # Condição de Venda
        condicao_venda = item_data.get('condicaoVenda', {})
        if condicao_venda:
            items['valorBRL'] = condicao_venda.get('valorBRL')
            items['valorMoedaNegociada'] = condicao_venda.get('valorMoedaNegociada')
            
            # Incoterm
            incoterm = condicao_venda.get('incoterm', {})
            if incoterm:
                items['incoterm'] = incoterm.get('codigo')
            
            # Frete
            frete = condicao_venda.get('frete', {})
            if frete:
                items['freteValorBRL'] = frete.get('valorBRL')
            
            # Seguro
            seguro = condicao_venda.get('seguro', {})
            if seguro:
                items['seguroValorBRL'] = seguro.get('valorBRL')
        
        # Dados Cambiais
        dados_cambiais = item_data.get('dadosCambiais', {})
        if dados_cambiais:
            items['numeroROF'] = dados_cambiais.get('numeroROF')
        
        # Tributos
        tributos = item_data.get('tributos', {})
        if tributos:
            # Mercadoria
            mercadoria_tributos = tributos.get('mercadoria', {})
            if mercadoria_tributos:
                items['valorLocalEmbarqueBRL'] = mercadoria_tributos.get('valorLocalEmbarqueBRL')
                items['valorAduaneiroBRL'] = mercadoria_tributos.get('valorAduaneiroBRL')
            
            # Tributos Calculados - CAMPOS DINÂMICOS (seguindo padrão II_recolhido, IPI_recolhido, etc.)
            tributos_calculados = tributos.get('tributosCalculados', [])
            for tributo in tributos_calculados:
                tipo = tributo.get('tipo')
                valores_brl = tributo.get('valoresBRL', {})
                memoria_calculo = tributo.get('memoriaCalculo', {})
                
                if tipo:
                    # Cria campos dinâmicos baseados no tipo de tributo (seguindo padrão da tabela duimp)
                    items[f'{tipo}_devido'] = valores_brl.get('devido')
                    items[f'{tipo}_baseCalculoBRL'] = memoria_calculo.get('baseCalculoBRL')
                    items[f'{tipo}_tipoAliquota'] = memoria_calculo.get('tipoAliquota')
                    items[f'{tipo}_valorAliquota'] = memoria_calculo.get('valorAliquota')
        
        return items
    
    def discover_tributo_columns(self, json_data):
        """
        Descobre todos os tipos de tributos presentes no JSON para criar colunas dinâmicas
        """
        tributo_types = set()
        
        for item in json_data:
            tributos = item.get('tributos', {})
            tributos_calculados = tributos.get('tributosCalculados', [])
            
            for tributo in tributos_calculados:
                tipo = tributo.get('tipo')
                if tipo:
                    tributo_types.add(tipo)
        
        return sorted(list(tributo_types))
    
    def create_table(self, tributo_types):
        """Cria a tabela com colunas fixas e colunas dinâmicas para tributos"""
        # Colunas fixas - seguindo padrão da tabela duimp
        columns = [
            'id INTEGER PRIMARY KEY AUTOINCREMENT',
            'data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'duimpNumero TEXT',
            'duimpVersao INTEGER',
            'numeroItem INTEGER',
            'codigoProduto TEXT',
            'versaoProduto TEXT',
            'ncm TEXT',
            'codigoExportador TEXT',
            'quantidadeComercial REAL',
            'pesoLiquido REAL',
            'moedaNegociada TEXT',
            'valorUnitarioMoedaNegociada REAL',
            'descricaoMercadoria TEXT',
            'incoterm TEXT',
            'valorBRL REAL',
            'valorMoedaNegociada REAL',
            'freteValorBRL REAL',
            'seguroValorBRL REAL',
            'numeroROF TEXT',
            'valorLocalEmbarqueBRL REAL',
            'valorAduaneiroBRL REAL'
        ]
        
        # Colunas dinâmicas para tributos - seguindo padrão II_recolhido, IPI_recolhido, etc.
        for tributo_type in tributo_types:
            columns.extend([
                f'{tributo_type}_devido REAL',
                f'{tributo_type}_baseCalculoBRL REAL',
                f'{tributo_type}_tipoAliquota TEXT',
                f'{tributo_type}_valorAliquota REAL'
            ])
        
        # Cria a tabela
        create_query = f"""
        CREATE TABLE IF NOT EXISTS itens (
            {', '.join(columns)}
        )
        """
        
        self.cursor.execute(create_query)
        self.conn.commit()
        print("✅ Tabela 'itens' criada/verificada com sucesso!")
        print(f"📊 Total de colunas: {len(columns)}")
        print(f"🎯 Tipos de tributos encontrados: {', '.join(tributo_types)}")
    
    def insert_data(self, json_data):
        """Insere todos os itens do JSON na tabela"""
        # Descobre os tipos de tributos
        tributo_types = self.discover_tributo_columns(json_data)
        
        # Cria a tabela
        self.create_table(tributo_types)
        
        # Insere cada item
        inserted_count = 0
        for i, item in enumerate(json_data, 1):
            extracted_data = self.extract_fields(item)
            
            # Prepara os dados para inserção
            columns = list(extracted_data.keys())
            values = list(extracted_data.values())
            
            # Cria a query de inserção
            placeholders = ', '.join(['?' for _ in values])
            columns_str = ', '.join(columns)
            
            insert_query = f"INSERT INTO itens ({columns_str}) VALUES ({placeholders})"

        self.conn.commit()
        print(f"🎉 {inserted_count} itens inseridos com sucesso!")
        return inserted_count
    
    def show_table_structure(self):
        """Mostra a estrutura da tabela"""
        try:
            self.cursor.execute("PRAGMA table_info(itens)")
            columns = self.cursor.fetchall()
            
            print("\n📋 ESTRUTURA DA TABELA 'itens':")
            print("=" * 60)
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                # Destaca colunas de tributos dinâmicos
                if any(tributo in col_name for tributo in ['II_', 'IPI_', 'PIS_', 'COFINS_', 'TAXA_']):
                    print(f"  🎯 {col_name:40} {col_type}")
                else:
                    print(f"  ✅ {col_name:40} {col_type}")
            print("=" * 60)
            print(f"Total de colunas: {len(columns)}\n")
            
            return columns
        except sqlite3.Error as e:
            print(f"❌ Erro ao obter estrutura: {e}")
            return []
    
    def show_sample_data(self, limit=5):
        """Mostra uma amostra dos dados inseridos"""
        try:
            self.cursor.execute(f"SELECT * FROM itens LIMIT {limit}")
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            print(f"\n📊 AMOSTRA DOS DADOS (primeiros {limit} registros):")
            print("=" * 80)
            for row in rows:
                print("\n--- REGISTRO ---")
                for col_name, value in zip(columns, row):
                    if value is not None and col_name not in ['id', 'data_insercao']:  # Mostra apenas campos com valores e exclui id/timestamp
                        print(f"  {col_name:30} = {value}")
            print("=" * 80)
            
        except sqlite3.Error as e:
            print(f"❌ Erro ao buscar dados: {e}")
    
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
        os.path.join(project_root, 'itens_response.json'),
        os.path.join(current_dir, 'itens_response.json'),
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
        
        # Verifica se é uma lista (array de itens)
        if not isinstance(data, list):
            print("❌ O JSON não é uma lista de itens!")
            return None
        
        print(f"📁 JSON carregado: {len(data)} itens encontrados")
        
        db = ItensDatabaseHandler('itens_data.db')
        
        # Insere os dados
        inserted_count = db.insert_data(data)
        
        if inserted_count > 0:
            db.show_table_structure()
            db.show_sample_data(3)
            
            # Estatísticas
            print(f"\n📈 ESTATÍSTICAS:")
            print(f"   ✅ Itens processados: {inserted_count}/{len(data)}")
            
            # Mostra tipos de tributos encontrados
            tributo_types = db.discover_tributo_columns(data)
            print(f"   🎯 Tipos de tributos: {', '.join(tributo_types)}")
            
            # Soma dos valores
            db.cursor.execute("SELECT SUM(valorBRL), SUM(valorAduaneiroBRL) FROM itens")
            total_valor_brl, total_valor_aduaneiro = db.cursor.fetchone()
            print(f"   💰 Total valor BRL: R$ {total_valor_brl:,.2f}")
            print(f"   💰 Total valor aduaneiro: R$ {total_valor_aduaneiro:,.2f}")
        
        db.close()
        return inserted_count
        
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
        print("❌ Arquivo JSON não encontrado!")
        
        # Diagnóstico simples
        current_dir = os.getcwd()
        project_root = os.path.dirname(current_dir) if current_dir.endswith('/venv') else current_dir
        
        print(f"\n🔍 Diagnóstico:")
        print(f"📂 Diretório atual: {current_dir}")
        print(f"📁 Raiz do projeto: {project_root}")
        
        print(f"\n💡 Solução:")
        print(f"1. Coloque o arquivo JSON na pasta: {project_root}")
        print(f"2. Certifique-se de que o arquivo tem um dos nomes:")
        print(f"   - api_response.json")
        print(f"   - itens_response.json")
        print(f"3. Execute novamente: python {sys.argv[0]}")
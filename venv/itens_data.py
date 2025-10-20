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
    
    def extract_fields(self, item_data):
        """
        Extrai os campos específicos conforme a lista fornecida
        """
        items = {}
        
        # Identificação
        identificacao = item_data.get('identificacao', {})
        if identificacao:
            items['duimpNumero'] = identificacao.get('numero')
            items['duimpVersao'] = identificacao.get('versao')
        
        # Produto
        produto = item_data.get('produto', {})
        if produto:
            items['codigoProduto'] = produto.get('codigo')
            items['versaoProduto'] = produto.get('versao')
            items['ncmProduto'] = produto.get('ncm')
        
        # Exportador
        exportador = item_data.get('exportador', {})
        if exportador:
            items['codigoExportador'] = exportador.get('codigo')
        
        # Mercadoria
        mercadoria = item_data.get('mercadoria', {})
        if mercadoria:
            items['quantidadeItem'] = mercadoria.get('quantidadeComercial')
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
            
            # Tributos Calculados - CAMPOS DINÂMICOS
            tributos_calculados = tributos.get('tributosCalculados', [])
            for tributo in tributos_calculados:
                tipo = tributo.get('tipo')
                valores_brl = tributo.get('valoresBRL', {})
                memoria_calculo = tributo.get('memoriaCalculo', {})
                
                if tipo:
                    # Cria campos dinâmicos baseados no tipo de tributo
                    items[f'{tipo}_devido'] = valores_brl.get('devido')
                    items[f'{tipo}_baseCalculoBRL'] = memoria_calculo.get('baseCalculoBRL')
                    items[f'{tipo}_valorAliquota'] = memoria_calculo.get('valorAliquota')
        
        return items
    
    def calculate_adicoes_fields(self, json_data):
        processed_items = []
        current_adicao = 1  # Começa em 1
        current_ncm = None
        item_count_in_adicao = 0
        total_item_count = 0
        
        for item in json_data:
            total_item_count += 1
            
            # Extrai o NCM atual
            ncm = item.get('produto', {}).get('ncm')
            
            # Se é o primeiro item ou o NCM mudou, incrementa a adição
            if current_ncm is None:
                # Primeiro item - inicia a primeira adição
                current_adicao = 1
                item_count_in_adicao = 0
                current_ncm = ncm
            elif ncm != current_ncm:
                # NCM mudou - incrementa adição e reseta contador
                current_adicao += 1
                item_count_in_adicao = 0
                current_ncm = ncm
            
            item_count_in_adicao += 1
            
            # Adiciona os campos calculados ao item
            item_with_adicoes = item.copy()
            item_with_adicoes['adicaoNumero'] = current_adicao
            item_with_adicoes['numeroItemAdicao'] = item_count_in_adicao
            item_with_adicoes['numeroItemDuimp'] = total_item_count
            
            processed_items.append(item_with_adicoes)
        
        return processed_items
    
    def discover_tributo_columns(self, json_data):
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
        # Colunas fixas
        columns = [
            'id INTEGER PRIMARY KEY AUTOINCREMENT',
            'data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'duimpNumero TEXT',
            'duimpVersao INTEGER',
            'codigoProduto TEXT',
            'versaoProduto TEXT',
            'ncmProduto TEXT',
            'codigoExportador TEXT',
            'quantidadeItem REAL',
            'pesoLiquido REAL',
            'pesoPercentual REAL',
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
            'valorAduaneiroBRL REAL',
            'adicaoNumero INTEGER',
            'numeroItemAdicao INTEGER',
            'numeroItemDuimp INTEGER'
        ]
        
        # Colunas dinâmicas para tributos
        for tributo_type in tributo_types:
            columns.extend([
                f'{tributo_type}_devido REAL',
                f'{tributo_type}_baseCalculoBRL REAL',
                f'{tributo_type}_valorAliquota REAL'
            ])
        
        # Cria a tabela
        create_query = f"""
        CREATE TABLE IF NOT EXISTS itens_data (
            {', '.join(columns)}
        )
        """
        
        self.cursor.execute(create_query)
        self.conn.commit()
    
    def calculate_peso_percentual(self):
        try:
            # Calcula o peso total de todos os itens
            self.cursor.execute("SELECT SUM(pesoLiquido) FROM itens_data")
            total_peso = self.cursor.fetchone()[0]            
           
            # Atualiza cada registro com o percentual
            self.cursor.execute("SELECT id, pesoLiquido FROM itens_data")
            registros = self.cursor.fetchall()
            
            for registro_id, peso in registros:
                if peso is not None and peso > 0:
                    percentual = (peso / total_peso) * 100
                    # Arredonda para 8 casas decimais
                    percentual_arredondado = round(percentual, 8)
                    
                    self.cursor.execute(
                        "UPDATE itens_data SET pesoPercentual = ? WHERE id = ?",
                        (percentual_arredondado, registro_id)
                    )
            
            self.conn.commit()
            
            # Verifica se a soma dos percentuais é 100%
            self.cursor.execute("SELECT SUM(pesoPercentual) FROM itens_data")
            soma_percentual = self.cursor.fetchone()[0]
            
            print(f"   📊 Peso total: {total_peso:.4f} kg")
            print(f"   📈 Soma dos percentuais: {soma_percentual:.8f}%")
            
        except sqlite3.Error as e:
            print(f"❌ Erro ao calcular peso percentual: {e}")
    
    def insert_data(self, json_data):
        """Insere todos os itens do JSON na tabela"""
        # Primeiro calcula os campos de adição
        processed_data = self.calculate_adicoes_fields(json_data)
        
        # Descobre os tipos de tributos
        tributo_types = self.discover_tributo_columns(processed_data)
        
        # Cria a tabela
        self.create_table(tributo_types)
        
        # Insere cada item
        inserted_count = 0
        for item in processed_data:
            extracted_data = self.extract_fields(item)
            
            # Adiciona os campos calculados de adição
            extracted_data['adicaoNumero'] = item.get('adicaoNumero')
            extracted_data['numeroItemAdicao'] = item.get('numeroItemAdicao')
            extracted_data['numeroItemDuimp'] = item.get('numeroItemDuimp')
            
            # Prepara os dados para inserção
            columns = list(extracted_data.keys())
            values = list(extracted_data.values())
            
            # Cria a query de inserção
            placeholders = ', '.join(['?' for _ in values])
            columns_str = ', '.join(columns)
            
            insert_query = f"INSERT INTO itens_data ({columns_str}) VALUES ({placeholders})"
            
            try:
                self.cursor.execute(insert_query, values)
                inserted_count += 1
            except sqlite3.Error as e:
                print(f"❌ Erro ao inserir item {item.get('identificacao', {}).get('numeroItem')}: {e}")
        
        self.conn.commit()
        
        # Calcula o peso percentual após inserir todos os dados
        if inserted_count > 0:
            self.calculate_peso_percentual()
        
        return inserted_count
    
    def show_table_structure(self):
        try:
            self.cursor.execute("PRAGMA table_info(itens_data)")
            columns = self.cursor.fetchall()
            
            print("\n📊 ESTRUTURA DA TABELA 'itens_data':")
            print("=" * 60)
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                # Define ícones específicos para cada tipo de coluna
                if col_name in ['adicaoNumero', 'numeroItemAdicao', 'numeroItemDuimp']:
                    icon = "🔥"  # Novos campos
                elif col_name in ['id', 'data_insercao']:
                    icon = "⏰"  # Campos de sistema
                elif any(tributo in col_name for tributo in ['II_', 'IPI_', 'PIS_', 'COFINS_', 'TAXA_']):
                    icon = "💰"  # Campos de tributos
                elif 'peso' in col_name.lower():
                    icon = "⚖️"  # Campos de peso
                elif 'valor' in col_name.lower() or 'brl' in col_name.lower():
                    icon = "💵"  # Campos monetários
                elif 'descricao' in col_name.lower():
                    icon = "📝"  # Descrições
                else:
                    icon = "✅"  # Demais campos
                
                print(f"  {icon} {col_name:38} {col_type}")
            
            print("=" * 60)
            print(f"🎯 Total de colunas: {len(columns)}")
            
            return columns
        except sqlite3.Error as e:
            print(f"❌ Erro ao obter estrutura: {e}")
            return []
    
    def get_statistics(self, json_data):
        """Retorna estatísticas dos dados processados"""
        tributo_types = self.discover_tributo_columns(json_data)
        
        # Calcula totais
        self.cursor.execute("SELECT SUM(valorBRL), SUM(valorAduaneiroBRL) FROM itens_data")
        total_valor_brl, total_valor_aduaneiro = self.cursor.fetchone()
        
        # Estatísticas das adições
        self.cursor.execute("SELECT COUNT(DISTINCT adicaoNumero), MAX(numeroItemAdicao) FROM itens_data")
        total_adicoes, max_itens_por_adicao = self.cursor.fetchone()
        
        return {
            'tributo_types': tributo_types,
            'total_valor_brl': total_valor_brl or 0,
            'total_valor_aduaneiro': total_valor_aduaneiro or 0,
            'total_adicoes': total_adicoes or 0,
            'max_itens_por_adicao': max_itens_por_adicao or 0
        }
    
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
       
        db = ItensDatabaseHandler('itens_data.db')
        
        # Insere os dados
        inserted_count = db.insert_data(data)
        
        if inserted_count > 0:
            # Mostra estrutura da tabela
            db.show_table_structure()
            
            # Estatísticas
            stats = db.get_statistics(data)
            print("\n📈 ESTATÍSTICAS:")
            print(f"   ✅ Itens processados: {inserted_count}/{len(data)}")
            print(f"   💰 Total valor BRL: R$ {stats['total_valor_brl']:,.2f}")
            print(f"   💰 Total valor aduaneiro: R$ {stats['total_valor_aduaneiro']:,.2f}")
        
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
        # Processa silenciosamente - sem mensagem de arquivo encontrado
        process_json_file(json_file)
    else:
        print("❌ Arquivo JSON não encontrido!")
        
        # Diagnóstico simples
        current_dir = os.getcwd()
        project_root = os.path.dirname(current_dir) if current_dir.endswith('/venv') else current_dir
        
        print(f"\n💡 Solução:")
        print(f"1. Coloque o arquivo JSON na pasta: {project_root}")
        print(f"2. Certifique-se de que o arquivo tem um dos nomes:")
        print(f"   - api_response.json")
        print(f"   - itens_response.json")
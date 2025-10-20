# converter_table_json.py
import sqlite3
import json
import os
from datetime import datetime

class TableToJsonConverter:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
    
    def convert_table_to_json(self, table_name, exclude_columns=None):
        if exclude_columns is None:
            # Exclui table_json por padrão
            exclude_columns = ['table_json']  
            
        try:
            # Obtém os dados da tabela
            self.cursor.execute(f"SELECT * FROM {table_name}")
            rows = self.cursor.fetchall()
            
            # Converte para lista de dicionários, excluindo colunas especificadas
            data = []
            for row in rows:
                record = {}
                for key in row.keys():
                    if key in exclude_columns:
                        continue  # Pula colunas excluídas
                    
                    value = row[key]
                    # Converte bytes para string (se necessário)
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='ignore')
                    # Converte datetime para string (se necessário)
                    elif isinstance(value, datetime):
                        value = value.isoformat()
                    record[key] = value
                data.append(record)
            
            # Monta o resultado com estrutura mais limpa
            result = {
                "metadata": {
                    "table_name": table_name
                },
                "data": data
            }
            
            return result
            
        except sqlite3.Error as e:
            print(f"❌ Erro ao converter tabela {table_name}: {e}")
            return None
    
    def convert_table_to_json_string(self, table_name, exclude_columns=None):
        json_data = self.convert_table_to_json(table_name, exclude_columns)
        if json_data:
            return json.dumps(json_data, ensure_ascii=False, indent=2)
        return None
    
    def close(self):
        self.conn.close()


# Função de conveniência para uso rápido
def get_table_json(db_path, table_name, exclude_columns=None):
    converter = TableToJsonConverter(db_path)
    json_string = converter.convert_table_to_json_string(table_name, exclude_columns)
    converter.close()
    return json_string
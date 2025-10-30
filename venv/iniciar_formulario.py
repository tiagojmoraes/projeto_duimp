#!/usr/bin/env python3
"""
Script para iniciar o formulário DUIMP automaticamente
"""

import subprocess
import sys
import os

def main():
    print("🚀 Iniciando DUIMP Formulário...")
    
    # Primeiro executa a extração dos dados
    from duimp_identificacao import find_json_file, process_json_file
    
    json_file = find_json_file()
    
    if json_file:
        print(f"📁 Arquivo JSON encontrado: {json_file}")
        extracted_data = process_json_file(json_file)
        
        if extracted_data:
            print("✅ Dados extraídos com sucesso!")
            print("🌐 Abrindo formulário web...")
            
            # Importa e executa o formulário
            from formulario_web import mostrar_formulario
            mostrar_formulario(extracted_data)
        else:
            print("❌ Erro ao extrair dados do JSON")
    else:
        print("❌ Arquivo 'api_response.json' não encontrado")

if __name__ == "__main__":
    main()
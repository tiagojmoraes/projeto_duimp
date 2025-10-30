import streamlit as st
import sys
import os
import json

# Adicionar o diretório atual ao path
sys.path.append(os.path.dirname(__file__))

from duimp_identificacao import find_json_file, process_json_file
from formulario_web import mostrar_formulario

def main():
    st.set_page_config(
        page_title="DUIMP - Sistema Completo",
        page_icon="📊",
        layout="centered"
    )
    
    st.title("🚀 DUIMP - Sistema Completo")
    
    # Buscar e processar JSON automaticamente
    json_file = find_json_file()
    
    if json_file:
        st.success(f"✅ Arquivo JSON encontrado: `{json_file}`")
        
        try:
            extracted_data = process_json_file(json_file)
            
            if extracted_data:
                st.success("✅ Dados extraídos com sucesso!")
                mostrar_formulario(extracted_data)
            else:
                st.warning("⚠️  Não foi possível extrair dados automáticos do JSON")
                st.info("💡 Vamos usar um formulário com dados básicos")
                
                # Criar dados básicos manualmente
                dados_basicos = {
                    'duimpNumero': 'A SER PREENCHIDO',
                    'duimpVersao': '1',
                    'dataRegistro': '2024-01-01',
                    'chaveAcesso': 'A SER PREENCHIDO',
                    'cnpjImportador': '00.000.000/0000-00',
                    'codFilial': '00',
                    'canalParametrizacao': 'A SER PREENCHIDO',
                    'tipoIdentificacaoCarga': 'A SER PREENCHIDO',
                    'cargaIdentificacao': 'A SER PREENCHIDO',
                    'taxaSiscomex': 0.0
                }
                mostrar_formulario(dados_basicos)
                
        except Exception as e:
            st.error(f"❌ Erro: {e}")
            st.info("💡 Vamos usar um formulário com dados básicos")
            
            # Criar dados básicos manualmente em caso de erro
            dados_basicos = {
                'duimpNumero': 'A SER PREENCHIDO',
                'duimpVersao': '1', 
                'dataRegistro': '2024-01-01',
                'chaveAcesso': 'A SER PREENCHIDO',
                'cnpjImportador': '00.000.000/0000-00',
                'codFilial': '00',
                'canalParametrizacao': 'A SER PREENCHIDO',
                'tipoIdentificacaoCarga': 'A SER PREENCHIDO', 
                'cargaIdentificacao': 'A SER PREENCHIDO',
                'taxaSiscomex': 0.0
            }
            mostrar_formulario(dados_basicos)
            
    else:
        st.error("❌ Arquivo 'api_response.json' não encontrado")
        st.info("💡 Vamos usar um formulário com dados básicos")
        
        # Criar dados básicos manualmente
        dados_basicos = {
            'duimpNumero': 'A SER PREENCHIDO',
            'duimpVersao': '1',
            'dataRegistro': '2024-01-01', 
            'chaveAcesso': 'A SER PREENCHIDO',
            'cnpjImportador': '00.000.000/0000-00',
            'codFilial': '00',
            'canalParametrizacao': 'A SER PREENCHIDO',
            'tipoIdentificacaoCarga': 'A SER PREENCHIDO',
            'cargaIdentificacao': 'A SER PREENCHIDO',
            'taxaSiscomex': 0.0
        }
        mostrar_formulario(dados_basicos)

if __name__ == "__main__":
    main()
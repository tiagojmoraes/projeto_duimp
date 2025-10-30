import streamlit as st
import sqlite3
import json
from datetime import datetime
import sys
import os

# Adicionar o diretório atual ao path para importar duimp_identificacao
sys.path.append(os.path.dirname(__file__))

def mostrar_formulario(extracted_data):
    st.set_page_config(
        page_title="Formulário DUIMP",
        page_icon="📋",
        layout="centered"
    )
    
    st.title("📋 Formulário DUIMP - Dados Adicionais")
    
    # Mostrar dados extraídos do JSON
    with st.expander("📊 Dados Extraídos do JSON", expanded=True):
        for key, value in extracted_data.items():
            if value:  # Só mostrar se não for None/vazio
                st.write(f"**{key}:** {value}")
    
    st.divider()
    
    with st.form("form_duimp"):
        st.subheader("Dados Adicionais")
        
        # Campo: Data Liberação DUIMP
        data_lib = st.date_input(
            "Data Liberação DUIMP*",
            value=datetime.now(),
            format="YYYY-MM-DD"
        )
        
        # Campo: Peso Bruto
        peso_bruto = st.number_input(
            "Peso Bruto (kg)*",
            min_value=0.0,
            value=0.0,
            step=0.1
        )
        
        # Botão de submit
        submitted = st.form_submit_button("💾 Salvar Dados")
        
        if submitted:
            if not peso_bruto:
                st.error("❌ Peso Bruto é obrigatório!")
            else:
                manual_data = {
                    'dataLibDuimp': data_lib.strftime("%Y-%m-%d"),
                    'pesoBruto': peso_bruto
                }
                
                try:
                    from duimp_identificacao import DatabaseHandler
                    db = DatabaseHandler()
                    
                    # Aceitar mesmo que dados estejam incompletos
                    # Chamar insert_extracted_data passando manual_data explicitamente
                    inserted_id = db.insert_extracted_data(extracted_data, manual_data)
                    
                    if inserted_id:
                        st.success(f"✅ Dados salvos com sucesso! ID: {inserted_id}")
                        st.balloons()
                        
                        # Mostrar dados salvos
                        with st.expander("📋 Ver Dados Salvos", expanded=True):
                            all_data = {**extracted_data, **manual_data}
                            for key, value in all_data.items():
                                if value and value != 'A SER PREENCHIDO':
                                    st.write(f"**{key}:** {value}")
                    else:
                        st.error("❌ Erro ao salvar dados!")
                            
                except Exception as e:
                    st.error(f"❌ Erro: {e}")
                finally:
                    # Fechar a conexão com o banco
                    if 'db' in locals():
                        db.close()

# Código para executar diretamente (para testes)
if __name__ == "__main__":
    dados_exemplo = {
        'duimpNumero': 'TESTE123',
        'duimpVersao': '1',
        'dataRegistro': '2024-01-01',
        'chaveAcesso': 'chave_teste',
        'cnpjImportador': '12.345.678/0001-90',
        'codFilial': '18',
        'canalParametrizacao': 'Verde',
        'tipoIdentificacaoCarga': 'CTE',
        'cargaIdentificacao': '123456',
        'taxaSiscomex': 150.75
    }
    mostrar_formulario(dados_exemplo)
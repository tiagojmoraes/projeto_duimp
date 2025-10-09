from api.client import PUCOMEXClient
from data.processor import json_to_tables

def main():
    duimp_id = input("nº da DUIMP: ").strip()
    
    client = PUCOMEXClient()
    print("Consultando extrato da DUIMP...")
    extrato_json = client.get_duimp_extrato(duimp_id)
    
    print("Convertendo JSON para tabelas...")
    tables = json_to_tables(extrato_json)
    
    for name, df in tables.items():
        print(f"\nTabela '{name}': {len(df)} registros")
        print(df.head())
    
    print("\nArquivos salvos em './tables/'!")

if __name__ == "__main__":
    main()
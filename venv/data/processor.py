import pandas as pd
import json

def json_to_tables(data: dict, output_dir: str = './tables/'):
    """Converte o JSON do extrato em DataFrames Pandas e salva como CSV."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    tables = {}
    
    if 'header' in data:
        df_header = pd.DataFrame([data['header']])
        df_header.to_csv(f"{output_dir}/duimp_header.csv", index=False)
        tables['header'] = df_header
    
    if 'items' in data:
        df_items = pd.DataFrame(data['items'])
        df_items.to_csv(f"{output_dir}/duimp_items.csv", index=False)
        tables['items'] = df_items
    
    if 'documents' in data:
        df_docs = pd.DataFrame(data['documents'])
        df_docs.to_csv(f"{output_dir}/duimp_documents.csv", index=False)
        tables['documents'] = df_docs
    
    if 'processes' in data:
        df_processes = pd.DataFrame(data['processes'])
        df_processes.to_csv(f"{output_dir}/duimp_processes.csv", index=False)
        tables['processes'] = df_processes
    
    with open(f"{output_dir}/extrato_completo.json", 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    return tables
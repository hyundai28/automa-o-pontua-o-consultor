import os
import pandas as pd
from datetime import datetime

# ---------------------------------------------------------
# Função para limpar CPF
# ---------------------------------------------------------
def clean_cpf(x):
    if pd.isna(x):
        return None
    x = str(x)
    x = "".join(filter(str.isdigit, x))
    if len(x) != 11:
        return None
    return x

# ---------------------------------------------------------
# Carrega o arquivo mais recente de um diretório
# ---------------------------------------------------------
def load_latest_file(folder):
    files = sorted([
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith((".xlsx", ".xls"))
    ])
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {folder}")
    return files[-1]

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():

    print("Lendo arquivos:")

    consultores_dir = "inputs/consultores"
    cadastros_dir = "inputs/cadastros"

    consultores_file = load_latest_file(consultores_dir)
    cadastros_file = load_latest_file(cadastros_dir)

    print(f"  ➜ Consultores: {consultores_file}")
    print(f"  ➜ Cadastros: {cadastros_file}")

    df_cons = pd.read_excel(consultores_file)
    df_cad = pd.read_excel(cadastros_file)

    # ---------------------------------------------------------
    # IDENTIFICAR COLUNAS DE CPF AUTOMATICAMENTE
    # ---------------------------------------------------------
    cpf_columns_cons = df_cons.columns[df_cons.columns.str.contains("cpf", case=False)]
    cpf_columns_cad  = df_cad.columns[df_cad.columns.str.contains("cpf", case=False)]

    if len(cpf_columns_cons) == 0:
        raise Exception("Nenhuma coluna de CPF encontrada no arquivo de consultores!")

    if len(cpf_columns_cad) == 0:
        raise Exception("Nenhuma coluna de CPF encontrada no arquivo de cadastros!")

    # Pega somente a PRIMEIRA coluna encontrada (garante Series)
    cpf_col_cons = cpf_columns_cons[0]
    cpf_col_cad  = cpf_columns_cad[0]

    # ---------------------------------------------------------
    # LIMPAR CPFs SEM ERROS
    # ---------------------------------------------------------
    df_cons["CPF_clean"] = df_cons[cpf_col_cons].apply(clean_cpf)
    df_cad["CPF_clean"]  = df_cad[cpf_col_cad].apply(clean_cpf)

    # ---------------------------------------------------------
    # REMOVER LINHAS SEM CPF VÁLIDO
    # ---------------------------------------------------------
    df_cons = df_cons.dropna(subset=["CPF_clean"])
    df_cad = df_cad.dropna(subset=["CPF_clean"])

    # ---------------------------------------------------------
    # MERGE
    # ---------------------------------------------------------
    df_merged = df_cons.merge(df_cad, on="CPF_clean", how="left", suffixes=("", "_cad"))

    # ---------------------------------------------------------
    # EXPORTAR RESULTADO
    # ---------------------------------------------------------
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")

    output_file = os.path.join(output_dir, f"resultado-{today}.xlsx")
    df_merged.to_excel(output_file, index=False)

    print(f"\nArquivo gerado com sucesso:")
    print(f"  ➜ {output_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro: {e}")
        raise

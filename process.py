import pandas as pd
import glob
from rapidfuzz import fuzz, process

# ---------------------------
# 1. Localizar arquivos automaticamente
# ---------------------------

# Procura qualquer arquivo que termine com "-cadastros.xlsx"
cadastros_files = glob.glob("*-cadastros.xlsx")
consultores_files = glob.glob("*-consultores.xlsx")

if not cadastros_files:
    raise FileNotFoundError("Nenhum arquivo *-cadastros.xlsx encontrado!")
if not consultores_files:
    raise FileNotFoundError("Nenhum arquivo *-consultores.xlsx encontrado!")

cadastros_file = cadastros_files[0]
consultores_file = consultores_files[0]

print("Arquivo de cadastros encontrado:", cadastros_file)
print("Arquivo de consultores encontrado:", consultores_file)

# ---------------------------
# 2. Carregar arquivos
# ---------------------------
cadastros = pd.read_excel(cadastros_file)
consultores = pd.read_excel(consultores_file)

# Padronização
cadastros['CPF'] = cadastros['CPF'].astype(str).str.replace(r'\D', '', regex=True)
consultores['CPF'] = consultores['CPF'].astype(str).str.replace(r'\D', '', regex=True)

cadastros['NOME'] = cadastros['NOME'].astype(str).str.upper().str.strip()
consultores['NOME'] = consultores['NOME'].astype(str).str.upper().str.strip()

# ---------------------------
# 3. Merge direto pelo CPF
# ---------------------------
merge_cpf = pd.merge(
    cadastros,
    consultores,
    on="CPF",
    how="left",
    suffixes=("_cad", "_con")
)

# ---------------------------
# 4. Fuzzy match 100%
# ---------------------------
def fuzzy_match(row):
    nome = row["NOME"]
    
    # Já encontrou pelo CPF?
    if pd.notna(row["NOME_con"]):
        return row["NOME_con"]

    # Fuzzy com exigência 100%
    result = process.extractOne(
        nome,
        consultores["NOME"],
        scorer=fuzz.WRatio
    )

    if result and result[1] == 100:
        return result[0]
    return None

merge_cpf["NOME_MATCH"] = merge_cpf.apply(fuzzy_match, axis=1)

# ---------------------------
# 5. Recuperar pontuação
# ---------------------------
consultores_dict = consultores.set_index("NOME").to_dict(orient="index")

def get_score(row):
    nome = row["NOME_MATCH"]
    if nome in consultores_dict:
        return consultores_dict[nome]["PONTUACAO"]
    return 0

merge_cpf["PONTUACAO"] = merge_cpf.apply(get_score, axis=1)

# ---------------------------
# 6. Resultado final
# ---------------------------
historico = merge_cpf[["CPF", "NOME", "PONTUAÇÃO"]]

output_file = "historico_final.xlsx"
historico.to_excel(output_file, index=False)

print("\nProcessamento finalizado!")
print("Arquivo gerado:", output_file)

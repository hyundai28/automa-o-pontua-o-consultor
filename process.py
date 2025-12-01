import pandas as pd
from rapidfuzz import fuzz, process

# ---------------------------
# 1. Carregar arquivos
# ---------------------------
cadastros = pd.read_excel("cadastros.xlsx")
consultores = pd.read_excel("consultores.xlsx")

# Padronização
cadastros['CPF'] = cadastros['CPF'].astype(str).str.replace(r'\D', '', regex=True)
consultores['CPF'] = consultores['CPF'].astype(str).str.replace(r'\D', '', regex=True)

cadastros['NOME'] = cadastros['NOME'].astype(str).str.upper().str.strip()
consultores['NOME'] = consultores['NOME'].astype(str).str.upper().str.strip()

# ---------------------------
# 2. Mapeamento de CPF exato
# ---------------------------

# Merge EXATO pelo CPF
merge_cpf = pd.merge(
    cadastros,
    consultores,
    on="CPF",
    how="left",
    suffixes=("_cad", "_con")
)

# ---------------------------
# 3. Fuzzy match somente 100%
# ---------------------------
def fuzzy_match(row):
    nome = row["NOME"]
    if pd.notna(row["NOME_con"]):  
        return row["NOME_con"]  

    result = process.extractOne(
        nome,
        consultores["NOME"],
        scorer=fuzz.WRatio
    )

    if result and result[1] == 100:
        return result[0]
    else:
        return None  


merge_cpf["NOME_MATCH"] = merge_cpf.apply(fuzzy_match, axis=1)

# ---------------------------
# 4. Resgatar pontuação do consultor encontrado
# ---------------------------
consultores_dict = consultores.set_index("NOME").to_dict(orient="index")

def get_score(row):
    nome = row["NOME_MATCH"]
    if nome in consultores_dict:
        return consultores_dict[nome]["PONTUACAO"]
    return 0  

merge_cpf["PONTUACAO"] = merge_cpf.apply(get_score, axis=1)

# ---------------------------
# 5. Resultado FINAL
# ---------------------------
historico = merge_cpf[[
    "CPF",
    "NOME",
    "PONTUACAO"
]]

historico.to_excel("historico_final.xlsx", index=False)

print("\nProcessamento finalizado!")
print("Arquivo gerado: historico_final.xlsx")

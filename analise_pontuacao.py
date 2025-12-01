#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from datetime import datetime
from unidecode import unidecode
import re

ROOT = Path(".")
CAD_FILE = next((ROOT / "inputs" / "cadastros").glob("*.xlsx"), None)
PONT_FILE = next((ROOT / "inputs" / "consultores").glob("*.*"), None)
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

def clean_cpf(cpf):
    return re.sub(r'\D', '', str(cpf)) if pd.notna(cpf) else ""

def normalize_name(name):
    if pd.isna(name):
        return ""
    return re.sub(r'\s+', ' ', unidecode(str(name)).strip().upper())

print("Lendo planilhas...")
df_cad = pd.read_excel(CAD_FILE)
df_pont = pd.read_excel(PONT_FILE)

print(f"Cadastros bruto: {len(df_cad)} linhas")
print(f"Pontuação: {len(df_pont)} linhas")

# FILTRA só "Cadastro Concluído"
df_cad = df_cad[df_cad["Status"].str.strip() == "Cadastro Concluído"].copy()
print(f"Após filtro 'Cadastro Concluído': {len(df_cad)} linhas")
if df_cad.empty:
    print("Nenhum consultor ativo encontrado.")
    exit()

# Normalização
df_cad["CPF_clean"] = df_cad["CPF"].apply(clean_cpf)
df_cad["Nome_clean"] = df_cad["Nome"].apply(normalize_name)
df_cad["Concessionaria_clean"] = df_cad["Concessionária"].astype(str).str.upper()

df_pont["CPF_clean"] = df_pont["CPF"].apply(clean_cpf)
df_pont["Nome_clean"] = df_pont["Nome"].apply(normalize_name)
df_pont["Concessionaria_clean"] = df_pont["Concessionária"].astype(str).str.upper()

# PROCURA A COLUNA Q.1.4 DE QUALQUER JEITO
nota_col = None
for col in df_pont.columns:
    if str(col).strip().startswith("Q.1.4"):
        nota_col = col
        print(f"Coluna encontrada: '{col}' → será usada como 'Nota Recomendação Consultor'")
        break

if nota_col is None:
    print("ERRO: Não encontrou nenhuma coluna que começa com 'Q.1.4'")
    print("Colunas disponíveis:", list(df_pont.columns))
    exit()

# Índices com prioridade
amostra_por_cpf = pd.Series(df_pont["Amostra"].values, index=df_pont["CPF_clean"]).to_dict()
nota_por_cpf    = pd.Series(df_pont[nota_col].values, index=df_pont["CPF_clean"]).to_dict()

df_pont["Chave"] = df_pont["Nome_clean"] + " | " + df_pont["Concessionaria_clean"]
amostra_por_chave = pd.Series(df_pont["Amostra"].values, index=df_pont["Chave"]).to_dict()
nota_por_chave    = pd.Series(df_pont[nota_col].values, index=df_pont["Chave"]).to_dict()

resultados = []

for _, row in df_cad.iterrows():
    cpf = row["CPF_clean"]
    nome = row["Nome_clean"]
    conc = row["Concessionaria_clean"]
    chave = f"{nome} | {conc}"

    amostra = 0
    nota_recomendacao = None

    if cpf and cpf in amostra_por_cpf:
        amostra = amostra_por_cpf[cpf]
        nota_recomendacao = nota_por_cpf.get(cpf)
    elif chave in amostra_por_chave:
        amostra = amostra_por_chave[chave]
        nota_recomendacao = nota_por_chave.get(chave)
    elif nome in df_pont["Nome_clean"].values:
        candidatos = df_pont[df_pont["Nome_clean"] == nome]
        if not candidatos.empty:
            melhor = candidatos.loc[candidatos["Amostra"].idxmax()]
            amostra = melhor["Amostra"]
            nota_recomendacao = melhor[nota_col]

    amostra_int = int(amostra) if pd.notna(amostra) else 0
    try:
        amostra_int = int(amostra_int)
    except:
        amostra_int = 0

    try:
        nota_final = round(float(nota_recomendacao), 2) if pd.notna(nota_recomendacao) else None
    except:
        nota_final = None

    resultados.append({
        "Concessionária": row["Concessionária"],
        "Consultor Regional": row["Consultor Regional"],
        "Consultor": row["Nome"],
        "CPF": row["CPF"],
        "Amostra": amostra_int,
        "Nota Recomendação Consultor": nota_final,
        "Status": "PONTUOU" if amostra_int > 0 else "NÃO PONTUOU"
    })

df_final = pd.DataFrame(resultados)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
arquivo = OUTPUT_DIR / f"RESULTADO_FINAL_{timestamp}.xlsx"

with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
    df_final.to_excel(writer, sheet_name="Resultado", index=False)
    resumo = pd.DataFrame({
        "Indicador": ["Total ativos", "Pontuaram", "Não pontuaram", "% pontuaram", "Média Nota Q.1.4"],
        "Valor": [
            len(df_final),
            (df_final["Amostra"] > 0).sum(),
            (df_final["Amostra"] == 0).sum(),
            f"{100*(df_final['Amostra']>0).sum()/len(df_final):.1f}%",
            f"{df_final['Nota Recomendação Consultor'].mean():.2f}" if df_final['Nota Recomendação Consultor'].notna().any() else "0.00"
        ]
    })
    resumo.to_excel(writer, sheet_name="Resumo", index=False)

print(f"\nSUCESSO TOTAL! Arquivo: {arquivo}")
print(f"Total: {len(df_final)} → {(df_final['Amostra']>0).sum()} pontuaram")

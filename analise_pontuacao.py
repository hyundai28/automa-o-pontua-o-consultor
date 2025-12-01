#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from datetime import datetime
from unidecode import unidecode
import re

ROOT = Path(".")
CADASTROS_FILE = next((ROOT / "inputs" / "cadastros").glob("*.xlsx"), None)
PONTUACAO_FILE = next((ROOT / "inputs" / "consultores").glob("*.*"), None)
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

def clean_cpf(cpf):
    return re.sub(r'\D', '', str(cpf)) if pd.notna(cpf) else ""

def normalize_name(name):
    return re.sub(r'\s+', ' ', unidecode(str(name)).strip().upper())

# Lendo as planilhas
print("Lendo planilhas...")
df_cad = pd.read_excel(CADASTROS_FILE)
df_pont = pd.read_excel(PONTUACAO_FILE)  # funciona .xls e .xlsx

print(f"Cadastros: {len(df_cad)} linhas")
print(f"Pontuação: {len(df_pont)} linhas")

# Forçando as colunas exatas que você me passou
df_cad["CPF_clean"] = df_cad["CPF"].apply(clean_cpf)
df_cad["Nome_clean"] = df_cad["Nome"].apply(normalize_name)

df_pont["CPF_clean"] = df_pont["CPF"].apply(clean_cpf)
df_pont["Nome_clean"] = df_pont["Nome"].apply(normalize_name)

# Criando dicionários de busca rápida
pontuacao_por_cpf = dict(zip(df_pont["CPF_clean"], df_pont["Amostra"]))
pontuacao_por_nome = dict(zip(df_pont["Nome_clean"], df_pont["Amostra"]))

# Processando cada cadastro
resultados = []
for _, row in df_cad.iterrows():
    cpf = row["CPF_clean"]
    nome = row["Nome_clean"]
    
    if cpf in pontuacao_por_cpf:
        pontos = pontuacao_por_cpf[cpf]
    elif nome in pontuacao_por_nome:
        pontos = pontuacao_por_nome[nome]
    else:
        pontos = 0
    
    resultados.append({
        "Concessionária": row["Concessionária"],
        "Consultor": row["Nome"],
        "CPF": row["CPF"],
        "Pontuação (Amostra)": int(pontos) if pd.notna(pontos) else 0,
        "Status": "PONTUOU" if (pd.notna(pontos) and pontos > 0) else "NÃO PONTUOU"
    })

df_final = pd.DataFrame(resultados)

# Salvando
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
arquivo_saida = OUTPUT_DIR / f"RESULTADO_PONTUACAO_{timestamp}.xlsx"

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    df_final.to_excel(writer, sheet_name="Resultado", index=False)
    
    # Aba de resumo
    resumo = pd.DataFrame({
        "Indicador": ["Total de consultores", "Pontuaram (Amostra > 0)", "Não pontuaram", "% que pontuaram"],
        "Valor": [
            len(df_final),
            len(df_final[df_final["Pontuação (Amostra)"] > 0]),
            len(df_final[df_final["Pontuação (Amostra)"] == 0]),
            f"{100 * len(df_final[df_final['Pontuação (Amostra)'] > 0]) / len(df_final):.1f}%"
        ]
    })
    resumo.to_excel(writer, sheet_name="Resumo", index=False)
    
print(arquivo_saida)
print(f"Total: {len(df_final)} consultores → {len(df_final[df_final['Status'] == 'PONTUOU'])} pontuaram")

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

print(f"Cadastros: {len(df_cad)} linhas}")
print(f"Pontuação: {len(df_pont)} linhas")

# COLUNAS CORRETAS DE VERDADE (conforme você me explicou agora)
# Cadastros → coluna "Nome" = consultor de pós-venda
# Pontuação → coluna "Nome" = consultor de pós-venda também
df_cad["CPF_clean"] = df_cad["CPF"].apply(clean_cpf)
df_cad["Nome_clean"] = df_cad["Nome"].apply(normalize_name)        # <- aqui é o consultor certo
df_cad["Concessionaria_clean"] = df_cad["Concessionária"].astype(str).str.upper()

df_pont["CPF_clean"] = df_pont["CPF"].apply(clean_cpf)
df_pont["Nome_clean"] = df_pont["Nome"].apply(normalize_name)
df_pont["Concessionaria_clean"] = df_pont["Concessionária"].astype(str).str.upper()

# Índices de busca (prioridade: CPF → Nome + Concessionária → Nome puro)
pontuacao_por_cpf = pd.Series(df_pont["Amostra"].values, index=df_pont["CPF_clean"]).to_dict()
hgsi_por_cpf     = pd.Series(df_pont["HGSI"].values, index=df_pont["CPF_clean"]).to_dict()

# Para evitar homônimos: chave = Nome + Concessionária
df_pont["Chave_Nome_Conc"] = df_pont["Nome_clean"] + " | " + df_pont["Concessionaria_clean"]
pontuacao_por_chave = pd.Series(df_pont["Amostra"].values, index=df_pont["Chave_Nome_Conc"]).to_dict()
hgsi_por_chave     = pd.Series(df_pont["HGSI"].values, index=df_pont["Chave_Nome_Conc"]).to_dict()

resultados = []

for _, row in df_cad.iterrows():
    cpf = row["CPF_clean"]
    nome = row["Nome_clean"]
    conc = row["Concessionaria_clean"]
    chave = f"{nome} | {conc}"

    amostra = 0
    hgsi = None

    # 1. Prioridade máxima: CPF
    if cpf and cpf in pontuacao_por_cpf:
        amostra = pontuacao_por_cpf[cpf]
        hgsi = hgs_por_cpf[cpf]
    # 2. Nome + Concessionária (evita pegar homônimo)
    elif chave in pontuacao_por_chave:
        amostra = pontuacao_por_chave[chave]
        hgsi = hgsi_por_chave[chave]
    # 3. Só nome (último recurso)
    elif nome in df_pont["Nome_clean"].values:
        candidatos = df_pont[df_pont["Nome_clean"] == nome]
        if not candidatos.empty:
            melhor = candidatos.loc[candidatos["Amostra"].idxmax()]
            amostra = melhor["Amostra"]
            hgsi = melhor["HGSI"]

    # Converte pra inteiro
    try:
        amostra_int = int(amostra) if pd.notna(amostra) else 0
    except:
        amostra_int = 0

    resultados.append({
        "Concessionária": row["Concessionária"],
        "Consultor Regional": row["Consultor Regional"],  # só pra referência da montadora
        "Consultor": row["Nome"],                         # esse é o que importa
        "CPF": row["CPF"],
        "Amostra": amostra_int,
        "HGSI": round(float(hgsi), 2) if pd.notna(hgsi) else None,
        "Status": "PONTUOU" if amostra_int > 0 else "NÃO PONTUOU"
    })

df_final = pd.DataFrame(resultados)

# Salva
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
arquivo = OUTPUT_DIR / f"RESULTADO_FINAL_{timestamp}.xlsx"

with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
    df_final.to_excel(writer, sheet_name="Resultado", index=False)
    resumo = pd.DataFrame({
        "Indicador": ["Total cadastrados", "Pontuaram", "Não pontuaram", "% pontuaram", "Média HGSI"],
        "Valor": [
            len(df_final),
            (df_final["Amostra"] > 0).sum(),
            (df_final["Amostra"] == 0).sum(),
            f"{100*(df_final['Amostra']>0).sum()/len(df_final):.1f}%",
            f"{df_final[df_final['Amostra']>0]['HGSI'].mean():.2f}" if (df_final['Amostra']>0).any() else "0.00"
        ]
    })
    resumo.to_excel(writer, sheet_name="Resumo", index=False)

print(f"\nPRONTO, CARALHO! Arquivo gerado: {arquivo}")
print(f"Total: {len(df_final)} → { (df_final['Amostra']>0).sum() } pontuaram")

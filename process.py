# === PROCESS.PY - Versão revisada solicitada ===
# Ajustado para:
# - Output único (CSV)
# - Fuzzy somente score 100
# - CPF sempre o cadastrado (não o do consultor)
# - Histórico com Dealer, Nome Consultor, CPF cadastrado
# - Correções estruturais

import pandas as pd
from rapidfuzz import fuzz
from datetime import datetime
import os

# Caminhos fixos
BASE = os.path.dirname(__file__)
INPUT_CONS = os.path.join(BASE, "inputs/consultores")
INPUT_CAD = os.path.join(BASE, "inputs/cadastros")
OUTPUT = os.path.join(BASE, "outputs")
HIST = os.path.join(BASE, "consultores_historico.csv")

os.makedirs(OUTPUT, exist_ok=True)

# ----------------------------------------------------------
# Função: Buscar cadastro por CPF ou Nome (score 100)
# ----------------------------------------------------------
def buscar_cadastro(consultor, df_cadastros):
    nome_consultor = str(consultor["Nome Consultor"]).strip().lower()
    cpf_consultor = str(consultor["CPF"]).zfill(11)

    # 1) Match direto por CPF
    match_cpf = df_cadastros[df_cadastros["CPF"] == cpf_consultor]
    if not match_cpf.empty:
        row = match_cpf.iloc[0]
        return row["CPF"], "CPF", 100

    # 2) Match por nome – somente se score = 100
    scores = df_cadastros["Nome"].apply(
        lambda x: fuzz.QRatio(str(x).strip().lower(), nome_consultor)
    )

    melhor_score = scores.max()

    if melhor_score == 100:
        matched_row = df_cadastros.iloc[scores.idxmax()]
        return matched_row["CPF"], "Nome", 100

    return None, None, 0

# ----------------------------------------------------------
# Carga dos arquivos de entrada
# ----------------------------------------------------------
def carregar_arquivos():
    # Consultores → pega sempre o último arquivo
    arquivos_cons = sorted([f for f in os.listdir(INPUT_CONS) if f.endswith(('.xls', '.xlsx'))])
    path_cons = os.path.join(INPUT_CONS, arquivos_cons[-1])

    df_cons = pd.read_excel(path_cons)

    # Renomear para novo formato
    df_cons.rename(columns={
        "Nome": "Dealer",
        "Consultor": "Nome Consultor"
    }, inplace=True)

    # Cadastros
    arquivos_cad = sorted([f for f in os.listdir(INPUT_CAD) if f.endswith(('.xls', '.xlsx'))])
    path_cad = os.path.join(INPUT_CAD, arquivos_cad[-1])

    df_cad = pd.read_excel(path_cad)

    # Normalizar nomes das colunas de cadastro
    df_cad.rename(columns={
        "nome": "Nome",
        "cpf": "CPF"
    }, inplace=True)

    df_cad["CPF"] = df_cad["CPF"].astype(str).str.replace("\D", "").str.zfill(11)

    return df_cons, df_cad

# ----------------------------------------------------------
# Carregar histórico antigo
# ----------------------------------------------------------
def carregar_historico():
    if os.path.exists(HIST):
        return pd.read_csv(HIST)
    else:
        return pd.DataFrame(columns=["Dealer", "Nome Consultor", "CPF Cadastrado", "Amostra", "data_import"])

# ----------------------------------------------------------
# Execução principal
# ----------------------------------------------------------
def main():
    print("Carregando arquivos...")
    df_cons, df_cad = carregar_arquivos()
    df_hist_antigo = carregar_historico()

    data_import = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    resultados = []

    print("Processando consultores...")
    for _, row in df_cons.iterrows():
        cpf_cad, matched_by, score = buscar_cadastro(row, df_cad)

        resultados.append({
            "Dealer": row.get("Dealer", ""),
            "Nome Consultor": row.get("Nome Consultor", ""),
            "CPF Cadastrado": cpf_cad,
            "Matched_by": matched_by,
            "Score_fuzzy": score,
            "Amostra": row.get("Amostra", ""),
            "data_import": data_import
        })

    df_final = pd.DataFrame(resultados)

    # Salvar output único em CSV
    output_name = f"resultados_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    full_output = os.path.join(OUTPUT, output_name)
    df_final.to_csv(full_output, index=False, encoding="utf-8-sig")

    print(f"Arquivo gerado: {full_output}")

    # Atualizar histórico
    df_hist_novo = df_final[[
        "Dealer", "Nome Consultor", "CPF Cadastrado", "Amostra", "data_import"
    ]]

    df_hist = pd.concat([df_hist_antigo, df_hist_novo], ignore_index=True)
    df_hist.to_csv(HIST, index=False, encoding="utf-8-sig")

    print("Histórico atualizado com sucesso.")


if __name__ == "__main__":
    main()

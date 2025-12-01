import os
import re
import sys
import traceback
from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz
from unidecode import unidecode
from datetime import datetime

ROOT = Path(__file__).parent
INPUTS_CONS = ROOT / "inputs" / "consultores"
INPUTS_CAD = ROOT / "inputs" / "cadastros"
OUTPUTS = ROOT / "outputs"

OUTPUTS.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------
# Funções utilitárias
# -----------------------------------------------------------

def clean_cpf(x):
    if pd.isna(x):
        return ""
    return re.sub(r"\D", "", str(x))

def norm_name(s):
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = unidecode(s).upper()
    s = re.sub(r"\s+", " ", s)
    return s

def find_file(folder: Path, keywords=("consultor",)):
    if not folder.exists():
        return None

    files = sorted([f for f in folder.glob("*") if f.is_file()])
    for f in files:
        name = f.name.lower()
        if any(k.lower() in name for k in keywords):
            return f
    return None

def read_maybe_excel(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não existe: {path}")

    ext = path.suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if ext == ".csv":
        try:
            return pd.read_csv(path)
        except:
            return pd.read_csv(path, sep=";")
    return pd.read_excel(path)

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------

def main():
    try:
        cons_file = find_file(INPUTS_CONS, keywords=("consultor","pontuacao","resultado"))
        cad_file = find_file(INPUTS_CAD, keywords=("cadastro","consultor"))

        print("Lendo arquivos:")
        print("  ➜ Consultores:", cons_file)
        print("  ➜ Cadastros:", cad_file)

        df_cons = read_maybe_excel(cons_file)
        df_cad = read_maybe_excel(cad_file)

        # -----------------------------------------------------------
        # Normalizar
        # -----------------------------------------------------------
        df_cad["CPF_clean"] = df_cad.iloc[:, df_cad.columns.str.contains("cpf", case=False)].apply(
            lambda col: clean_cpf(col), axis=1).iloc[:,0]

        df_cad["NOME_clean"] = df_cad.iloc[:, df_cad.columns.str.contains("nome", case=False)].apply(
            lambda col: norm_name(col), axis=1).iloc[:,0]

        df_cons["CPF_clean"] = df_cons.iloc[:, df_cons.columns.str.contains("cpf", case=False)].apply(
            lambda col: clean_cpf(col), axis=1).iloc[:,0]

        df_cons["NOME_clean"] = df_cons.iloc[:, df_cons.columns.str.contains("nome", case=False)].apply(
            lambda col: norm_name(col), axis=1).iloc[:,0]

        # Detectar coluna de pontuação automaticamente
        pont_col = None
        for c in df_cons.columns:
            if re.search(r"ponto|pontua|score", c.lower()):
                pont_col = c
                break

        if pont_col is None:
            print("ERRO: Nenhuma coluna de pontuação encontrada na planilha consultores.")
            sys.exit(1)

        # -----------------------------------------------------------
        # Índices de busca
        # -----------------------------------------------------------
        cons_by_cpf = df_cons.set_index("CPF_clean", drop=False).to_dict("index")

        cons_by_name = df_cons.set_index("NOME_clean", drop=False).to_dict("index")

        # Para fuzzy
        list_names = df_cons["NOME_clean"].tolist()
        list_rows = df_cons.to_dict("records")

        # -----------------------------------------------------------
        # RESULTADO FINAL (1 linha por cadastro!)
        # -----------------------------------------------------------

        results = []

        for _, cad in df_cad.iterrows():
            cpf = cad["CPF_clean"]
            nome = cad["NOME_clean"]

            matched = False
            pontos = 0.0

            # 1) Match exato por CPF
            if cpf and cpf in cons_by_cpf:
                matched = True
                pontos = cons_by_cpf[cpf][pont_col]

            # 2) Match exato por nome
            elif nome and nome in cons_by_name:
                matched = True
                pontos = cons_by_name[nome][pont_col]

            # 3) Fuzzy 100%
            else:
                best_score = -1
                best_idx = None

                for i, cand in enumerate(list_names):
                    score = fuzz.token_set_ratio(nome, cand)
                    if score > best_score:
                        best_score = score
                        best_idx = i

                if best_score == 100:
                    matched = True
                    pontos = list_rows[best_idx][pont_col]

            # garantir numeric
            try:
                pontos = float(pontos)
            except:
                pontos = 0.0

            results.append({
                "Nome Consultor": cad["NOME_clean"],
                "CPF": cpf,
                "Pontuacao": pontos,
                "Status": "PONTUOU" if matched else "NAO_PONTUOU"
            })

        df_out = pd.DataFrame(results)

        print("\nResumo:")
        print("Cadastros:", len(df_cad))
        print("Saída final:", len(df_out))

        # Aqui sempre será == cadastros
        assert len(df_out) == len(df_cad), "ERRO GRAVE: A saída não deveria ter linhas a mais!"

        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        file_out = OUTPUTS / f"resultado_{ts}.csv"
        df_out.to_csv(file_out, index=False)

        print("\nArquivo gerado:", file_out)

    except Exception as e:
        traceback.print_exc()
        print("Erro:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

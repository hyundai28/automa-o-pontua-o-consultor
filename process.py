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
HIST = ROOT / "historico" / "consultores_historico.csv"

OUTPUTS.mkdir(parents=True, exist_ok=True)
HIST.parent.mkdir(parents=True, exist_ok=True)

HIST_REQUIRED_COLS = ["Dealer", "Nome Consultor", "CPF", "Amostra", "data_import"]

def load_history_safe(path: Path):
    if not path.exists() or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=HIST_REQUIRED_COLS)
    try:
        df = pd.read_csv(path)
        for col in HIST_REQUIRED_COLS:
            if col not in df.columns:
                df[col] = ""
        return df[HIST_REQUIRED_COLS]
    except Exception:
        return pd.DataFrame(columns=HIST_REQUIRED_COLS)


def find_file(folder: Path, keywords=("consultor",)):
    if not folder.exists():
        return None
    for f in sorted(folder.glob("*")):
        name = f.name.lower()
        if any(k in name for k in keywords):
            return f
    return None


def read_maybe_excel(path: Path):
    if path is None or not path.exists():
        raise FileNotFoundError(f"Arquivo não existe: {path}")
    ext = path.suffix.lower()
    try:
        if ext == ".xls":
            return pd.read_excel(path, engine="xlrd")
        if ext == ".xlsx":
            return pd.read_excel(path, engine="openpyxl")
        if ext == ".csv":
            return pd.read_csv(path)
        return pd.read_excel(path)
    except Exception as e:
        raise RuntimeError(f"Erro lendo arquivo {path}: {e}")


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


def find_col(df, candidates):
    cols = df.columns.astype(str).tolist()
    cols_low = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in cols_low:
            return cols[cols_low.index(cand.lower())]
    for c in cols:
        if "cpf" in c.lower():
            return c
    for c in cols:
        if "nome" in c.lower() or "consultor" in c.lower():
            return c
    return None


def main():
    try:
        cons_file = find_file(INPUTS_CONS, keywords=("consultor", "consultores"))
        cad_file = find_file(INPUTS_CAD, keywords=("cadastro", "cadastros", "concessionaria", "concessionária"))

        if not cons_file or not cad_file:
            print("Erro: não encontrou os dois arquivos nas pastas inputs.")
            sys.exit(1)

        df_cons = read_maybe_excel(cons_file)
        df_cad = read_maybe_excel(cad_file)

        cpf_col_cons = find_col(df_cons, ["CPF"])
        nome_col_cons = find_col(df_cons, ["Nome"])
        amostra_col = find_col(df_cons, ["Amostra"])
        dealer_col = find_col(df_cons, ["Concessionária"])

        cpf_col_cad = find_col(df_cad, ["CPF"])
        nome_col_cad = find_col(df_cad, ["Nome"])

        df_cons["__CPF"] = df_cons[cpf_col_cons].map(clean_cpf)
        df_cons["__NOME_CONSULTOR"] = df_cons[nome_col_cons].map(norm_name)
        df_cons["__AMOSTRA"] = df_cons[amostra_col].fillna(0) if amostra_col else 1

        df_cad["__CPF"] = df_cad[cpf_col_cad].map(clean_cpf)
        df_cad["__NOME"] = df_cad[nome_col_cad].map(norm_name)

        df_cad = df_cad.sort_index().drop_duplicates(subset="__CPF", keep="last")
        cad_by_cpf = {r["__CPF"]: r for _, r in df_cad.iterrows() if r["__CPF"]}

        matched = []
        unmatched = []

        cad_names = df_cad["__NOME"].tolist()
        cad_rows = df_cad.to_dict('records')

        for _, row in df_cons.iterrows():
            cpf = row["__CPF"]
            nome = row["__NOME_CONSULTOR"]

            # MATCH POR CPF
            if cpf and cpf in cad_by_cpf:
                out = row.to_dict()
                out["_match_type"] = "CPF_EXATO"
                out["_matched_name"] = cad_by_cpf[cpf]["__NOME"]
                matched.append(out)
                continue

            # MATCH POR NOME 100%
            best_score = -1
            best_idx = None
            for i, cand in enumerate(cad_names):
                score = fuzz.token_set_ratio(nome, cand)
                if score > best_score:
                    best_score = score
                    best_idx = i

            if best_score == 100:
                cad_row = cad_rows[best_idx]
                out = row.to_dict()
                out["_match_type"] = "NOME_FUZZY_100"
                out["_matched_name"] = cad_row["__NOME"]
                matched.append(out)
            else:
                row["_match_type"] = "NAO_CADASTRADO"
                unmatched.append(row.to_dict())

        # REMOVER DUPLICIDADE POR CPF
        df_matched = pd.DataFrame(matched)
        if "__CPF" in df_matched.columns:
            df_matched = df_matched.drop_duplicates(subset="__CPF", keep="first")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        df_matched.to_csv(OUTPUTS / f"cadastrados_{timestamp}.csv", index=False)
        pd.DataFrame(unmatched).to_csv(OUTPUTS / f"nao_cadastrados_{timestamp}.csv", index=False)

        print("Arquivo único gerado:", OUTPUTS / f"cadastrados_{timestamp}.csv")

        # -------------------------
        # HISTÓRICO
        # -------------------------
        df_hist_existing = load_history_safe(HIST)
        df_hist_new = pd.DataFrame({
            "Dealer": df_cons[dealer_col],
            "Nome Consultor": df_cons[nome_col_cons],
            "CPF": df_cons["__CPF"],
            "Amostra": df_cons["__AMOSTRA"],
            "data_import": datetime.utcnow().isoformat()
        })

        df_hist = pd.concat([df_hist_existing, df_hist_new], ignore_index=True)
        df_hist = df_hist.sort_values("data_import").drop_duplicates(subset=["CPF"], keep="last")
        df_hist.to_csv(HIST, index=False)

        print("Histórico atualizado:", HIST)

    except Exception as e:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

# process.py
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

def find_file(folder: Path, keywords=("consultor",)):
    """Retorna o primeiro arquivo cujo nome contenha qualquer keyword (case-insensitive)."""
    if not folder.exists():
        return None
    for f in sorted(folder.glob("*")):
        name = f.name.lower()
        if any(k in name for k in keywords):
            return f
    return None

def read_maybe_excel(path: Path):
    """Lê xls/xlsx/csv definindo engine adequado e tratando erros comuns."""
    if path is None:
        return None
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não existe: {path}")
    ext = path.suffix.lower()
    try:
        if ext == ".xls":
            # pandas precisa do xlrd para .xls
            return pd.read_excel(path, engine="xlrd")
        if ext == ".xlsx":
            # .xlsx com openpyxl
            return pd.read_excel(path, engine="openpyxl")
        if ext == ".csv":
            return pd.read_csv(path)
        # tentar leitura automática se extensão estranha
        return pd.read_excel(path)
    except Exception as e:
        raise RuntimeError(f"Erro lendo arquivo {path}: {e}")

def clean_cpf(x):
    if pd.isna(x):
        return ""
    s = re.sub(r"\D", "", str(x))
    return s

def norm_name(s):
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = unidecode(s).upper()
    s = re.sub(r"\s+", " ", s)
    return s

def find_col(df, candidates):
    """Tenta encontrar uma coluna no DataFrame a partir de uma lista de candidatos.
       Se não achar, usa heurística por 'cpf' e 'nome' no header."""
    cols = df.columns.astype(str).tolist()
    cols_low = [c.lower() for c in cols]
    for cand in candidates:
        cand_low = cand.lower()
        if cand_low in cols_low:
            return cols[cols_low.index(cand_low)]
    # heurística
    for c in cols:
        if "cpf" in c.lower():
            return c
    for c in cols:
        if "nome" in c.lower() or "consultor" in c.lower():
            return c
    return None

def load_history_safe(path: Path):
    """Carrega o histórico com proteção: se não existir/estiver vazio/corrompido, retorna df vazio com colunas esperadas."""
    required = ["Nome", "CPF", "Amostra", "data_import"]
    if not path.exists() or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=required)
    try:
        df = pd.read_csv(path)
        # garante colunas mínimas
        for r in required:
            if r not in df.columns:
                df[r] = ""
        return df[required]
    except Exception:
        return pd.DataFrame(columns=required)

def safe_to_excel(df, path: Path):
    """Garante que sempre será gravado um excel, mesmo DataFrame vazio."""
    try:
        df.to_excel(path, index=False)
    except Exception:
        # fallback para csv se excel falhar por algum motivo
        df.to_csv(str(path.with_suffix(".csv")), index=False)

def main():
    try:
        cons_file = find_file(INPUTS_CONS, keywords=("consultor","consultores"))
        cad_file = find_file(INPUTS_CAD, keywords=("cadastro","cadastros","concessionaria","concessionária"))
        if not cons_file or not cad_file:
            print("Erro: não encontrou os dois arquivos nas pastas inputs. Verifique o upload.")
            print(f"consultores encontrado: {cons_file}")
            print(f"cadastros encontrado: {cad_file}")
            sys.exit(1)

        print("Lendo:", cons_file, cad_file)
        df_cons = read_maybe_excel(cons_file)
        df_cad = read_maybe_excel(cad_file)

        # detectar colunas
        cpf_col_cons = find_col(df_cons, ["CPF"])
        nome_col_cons = find_col(df_cons, ["Nome", "NOME", "nome"])
        amostra_col = find_col(df_cons, ["Amostra", "AMOSTRA", "Amostras", "Amostras"])
        cpf_col_cad = find_col(df_cad, ["CPF"])
        nome_col_cad = find_col(df_cad, ["Nome", "NOME", "nome"])

        if not cpf_col_cons or not nome_col_cons:
            print("Colunas CPF/Nome não encontradas na planilha de consultores.")
            print("Headers encontrados:", df_cons.columns.tolist())
            sys.exit(1)
        if not cpf_col_cad or not nome_col_cad:
            print("Colunas CPF/Nome não encontradas na planilha de cadastros.")
            print("Headers encontrados:", df_cad.columns.tolist())
            sys.exit(1)

        # normalizar
        df_cons["__CPF"] = df_cons[cpf_col_cons].map(clean_cpf)
        df_cons["__NOME"] = df_cons[nome_col_cons].map(norm_name)
        if amostra_col:
            df_cons["__AMOSTRA"] = df_cons[amostra_col].fillna(0)
        else:
            df_cons["__AMOSTRA"] = 1

        df_cad["__CPF"] = df_cad[cpf_col_cad].map(clean_cpf)
        df_cad["__NOME"] = df_cad[nome_col_cad].map(norm_name)

        # garantir que CPFs vazios fiquem como string vazia
        df_cons["__CPF"] = df_cons["__CPF"].fillna("").astype(str)
        df_cad["__CPF"] = df_cad["__CPF"].fillna("").astype(str)

        # remover duplicatas no cadastro mantendo o último (mais recente)
        if "__CPF" in df_cad.columns:
            df_cad = df_cad.sort_index().drop_duplicates(subset="__CPF", keep="last").reset_index(drop=True)

        # criar dicionário por CPF (índice único agora)
        cad_by_cpf = {}
        if "__CPF" in df_cad.columns:
            for _, r in df_cad.iterrows():
                cpf = r.get("__CPF", "")
                if cpf:
                    # manter última ocorrência (já fiz drop_duplicates)
                    cad_by_cpf[cpf] = r.to_dict()

        matched_by_cpf = []
        matched_by_name = []
        unmatched = []

        # preparar fuzzy (nomes e índices)
        cad_names = df_cad["__NOME"].fillna("").tolist()
        cad_rows = df_cad.to_dict('records')  # alinhado com cad_names por índice

        for idx, row in df_cons.iterrows():
            cpf = str(row.get("__CPF", "") or "")
            nome = str(row.get("__NOME", "") or "")
            matched = False

            # MATCH 1: CPF exato
            if cpf and cpf in cad_by_cpf:
                out = row.to_dict()
                out['_match_type'] = 'CPF_EXATO'
                out['_matched_cpf'] = cpf
                out['_matched_name'] = cad_by_cpf[cpf].get("__NOME", "")
                matched_by_cpf.append(out)
                matched = True
            else:
                # MATCH 2: fuzzy name
                best_score = 0
                best_idx = None
                for i, cand_name in enumerate(cad_names):
                    if not cand_name:
                        continue
                    score = fuzz.token_set_ratio(nome, cand_name)
                    if score > best_score:
                        best_score = score
                        best_idx = i
                if best_idx is not None and best_score >= 90:
                    cad_row = cad_rows[best_idx]
                    out = row.to_dict()
                    out['_match_type'] = 'NOME_FUZZY_ALTO'
                    out['_score'] = best_score
                    out['_matched_name'] = cad_row.get("__NOME", "")
                    out['_matched_cpf'] = cad_row.get("__CPF", "")
                    matched_by_name.append(out)
                    matched = True
                elif best_idx is not None and best_score >= 75:
                    cad_row = cad_rows[best_idx]
                    out = row.to_dict()
                    out['_match_type'] = 'NOME_FUZZY_POSSIVEL'
                    out['_score'] = best_score
                    out['_matched_name'] = cad_row.get("__NOME", "")
                    out['_matched_cpf'] = cad_row.get("__CPF", "")
                    matched_by_name.append(out)
                    matched = True

            if not matched:
                out = row.to_dict()
                out['_match_type'] = 'NAO_CADASTRADO'
                unmatched.append(out)

        # salvar saídas (mesmo se vazias)
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        out_cpf = OUTPUTS / f"cadastrados_por_cpf_{timestamp}.xlsx"
        out_name = OUTPUTS / f"cadastrados_por_nome_{timestamp}.xlsx"
        out_un = OUTPUTS / f"nao_cadastrados_{timestamp}.xlsx"

        safe_to_excel(pd.DataFrame(matched_by_cpf), out_cpf)
        safe_to_excel(pd.DataFrame(matched_by_name), out_name)
        safe_to_excel(pd.DataFrame(unmatched), out_un)

        print("Gerados:", out_cpf, out_name, out_un)

        # atualizar historico
        df_new = df_cons.copy()
        if "data_import" not in df_new.columns:
            df_new["data_import"] = datetime.utcnow().isoformat()

        hist_cols = ["__NOME", "__CPF", "__AMOSTRA", "data_import"]
        hist_df = df_new[hist_cols].rename(columns={"__NOME": "Nome", "__CPF": "CPF", "__AMOSTRA": "Amostra"})

        df_hist = load_history_safe(HIST)
        df_comb = pd.concat([df_hist, hist_df], ignore_index=True)

        # normalizar data_import para ordenação segura (se tiver strings diferentes)
        # assume ISO format; se não for, será ordenado lexicograficamente o que também mantém última inserção por timestamp
        if "data_import" not in df_comb.columns:
            df_comb["data_import"] = datetime.utcnow().isoformat()

        df_comb = df_comb.sort_values("data_import").drop_duplicates(subset=["CPF"], keep="last")
        df_comb = df_comb.sort_values("data_import").drop_duplicates(subset=["Nome"], keep="last")

        df_comb.to_csv(HIST, index=False)
        print("Histórico atualizado em", HIST)

    except Exception as e:
        # imprimir traceback completo para debug no GitHub Actions
        print("Traceback (most recent call last):")
        traceback.print_exc()
        print(f"Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

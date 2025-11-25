# process.py
import os
import re
import sys
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
    # retorna o primeiro arquivo cujo nome contenha qualquer keyword (case-insensitive)
    for f in sorted(folder.glob("*")):
        name = f.name.lower()
        if any(k in name for k in keywords):
            return f
    return None

def read_maybe_excel(path: Path):
    if path is None:
        return None
    ext = path.suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    elif ext == ".csv":
        return pd.read_csv(path)
    else:
        raise ValueError(f"Formato não suportado: {path}")

def clean_cpf(x):
    if pd.isna(x): return ""
    s = re.sub(r"\D", "", str(x))
    # não forçar zeros; retornamos como string de dígitos
    return s

def norm_name(s):
    if pd.isna(s): return ""
    s = str(s).strip()
    s = unidecode(s).upper()
    s = re.sub(r"\s+", " ", s)
    return s

def main():
    cons_file = find_file(INPUTS_CONS, keywords=("consultor","consultores"))
    cad_file = find_file(INPUTS_CAD, keywords=("cadastro","cadastros","concessionaria"))
    if not cons_file or not cad_file:
        print("Erro: não encontrou os dois arquivos nas pastas inputs. Verifique o upload.")
        print(f"consultores encontrado: {cons_file}")
        print(f"cadastros encontrado: {cad_file}")
        sys.exit(1)

    print("Lendo:", cons_file, cad_file)
    df_cons = read_maybe_excel(cons_file)
    df_cad = read_maybe_excel(cad_file)

    # Ajustar nomes de colunas que vêm do usuário (você pode adaptar se as colunas forem diferentes)
    # Procurar colunas: CPF, Nome, Amostra (Amostra -> quantidade)
    def find_col(df, candidates):
        cols = df.columns.astype(str).tolist()
        cols_low = [c.lower() for c in cols]
        for cand in candidates:
            if cand.lower() in cols_low:
                return cols[cols_low.index(cand.lower())]
        # fallback: heurística
        for c in cols:
            if "cpf" in c.lower(): return c
        for c in cols:
            if "nome" in c.lower(): return c
        return None

    # Colunas consultores
    cpf_col_cons = find_col(df_cons, ["CPF"])
    nome_col_cons = find_col(df_cons, ["Nome"])
    amostra_col = find_col(df_cons, ["Amostra","AMOSTRA"])
    # Colunas cadastros
    cpf_col_cad = find_col(df_cad, ["CPF"])
    nome_col_cad = find_col(df_cad, ["Nome"])

    if not cpf_col_cons or not nome_col_cons:
        print("Colunas CPF/Nome não encontradas na planilha de consultores.")
        sys.exit(1)
    if not cpf_col_cad or not nome_col_cad:
        print("Colunas CPF/Nome não encontradas na planilha de cadastros.")
        sys.exit(1)

    # Normalizar
    df_cons["__CPF"] = df_cons[cpf_col_cons].map(clean_cpf)
    df_cons["__NOME"] = df_cons[nome_col_cons].map(norm_name)
    if amostra_col:
        df_cons["__AMOSTRA"] = df_cons[amostra_col]
    else:
        df_cons["__AMOSTRA"] = 1

    df_cad["__CPF"] = df_cad[cpf_col_cad].map(clean_cpf)
    df_cad["__NOME"] = df_cad[nome_col_cad].map(norm_name)

    # Map CPF exato
    # Remove linhas com CPF duplicado, mantendo somente a última
    df_cad = df_cad.drop_duplicates(subset="__CPF", keep="last")

    # Agora cria o dict normalmente
    cad_by_cpf = df_cad.set_index("__CPF").to_dict('index')

    matched_by_cpf = []
    matched_by_name = []
    unmatched = []

    # Precompute list of cadastros names for fuzzy matching
    cad_names = df_cad["__NOME"].fillna("").tolist()
    cad_indices = df_cad.index.tolist()

    for idx, row in df_cons.iterrows():
        cpf = row["__CPF"]
        nome = row["__NOME"]
        matched = False
        if cpf and cpf in cad_by_cpf:
            # encontrado por cpf
            out = row.to_dict()
            out['_match_type'] = 'CPF_EXATO'
            out['_matched_cpf'] = cpf
            out['_matched_name'] = cad_by_cpf[cpf].get("__NOME", "")
            matched_by_cpf.append(out)
            matched = True
        else:
            # fuzzy name matching: procurar melhor score
            best_score = 0
            best_idx = None
            for i, cand_name in enumerate(cad_names):
                if not cand_name: continue
                score = fuzz.token_set_ratio(nome, cand_name)
                if score > best_score:
                    best_score = score
                    best_idx = i
            # thresholds: >= 90 forte, 75-89 provável, <75 ignorar
            if best_score >= 90:
                cad_row = df_cad.iloc[cad_indices[best_idx]]
                out = row.to_dict()
                out['_match_type'] = 'NOME_FUZZY_ALTO'
                out['_score'] = best_score
                out['_matched_name'] = cad_row["__NOME"]
                out['_matched_cpf'] = cad_row["__CPF"]
                matched_by_name.append(out)
                matched = True
            elif best_score >= 75:
                cad_row = df_cad.iloc[cad_indices[best_idx]]
                out = row.to_dict()
                out['_match_type'] = 'NOME_FUZZY_POSSIVEL'
                out['_score'] = best_score
                out['_matched_name'] = cad_row["__NOME"]
                out['_matched_cpf'] = cad_row["__CPF"]
                matched_by_name.append(out)
                matched = True

        if not matched:
            out = row.to_dict()
            out['_match_type'] = 'NAO_CADASTRADO'
            unmatched.append(out)

    # salvar saídas
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_cpf = OUTPUTS / f"cadastrados_por_cpf_{timestamp}.xlsx"
    out_name = OUTPUTS / f"cadastrados_por_nome_{timestamp}.xlsx"
    out_un = OUTPUTS / f"nao_cadastrados_{timestamp}.xlsx"

    pd.DataFrame(matched_by_cpf).to_excel(out_cpf, index=False)
    pd.DataFrame(matched_by_name).to_excel(out_name, index=False)
    pd.DataFrame(unmatched).to_excel(out_un, index=False)

    print("Gerados:", out_cpf, out_name, out_un)

 # Atualizar historico: uniremos os consultores do mês ao historico evitando duplicatas
    df_new = df_cons.copy()

    if "data_import" not in df_new.columns:
        df_new["data_import"] = datetime.utcnow().isoformat()

    hist_cols = ["__NOME", "__CPF", "__AMOSTRA", "data_import"]
    hist_df = df_new[hist_cols].rename(columns={
        "__NOME": "Nome",
        "__CPF": "CPF",
        "__AMOSTRA": "Amostra"
    })

    # Se o arquivo existir mas estiver vazio, cria um histórico vazio corretamente formatado
    if HIST.exists() and os.path.getsize(HIST) > 0:
        df_hist = pd.read_csv(HIST)
    else:
        df_hist = pd.DataFrame(columns=["Nome", "CPF", "Amostra", "data_import"])

    df_comb = pd.concat([df_hist, hist_df], ignore_index=True)

    # remover duplicatas por CPF (mantendo o mais recente)
    df_comb = df_comb.sort_values("data_import").drop_duplicates(subset=["CPF"], keep="last")

    # garantir nomes únicos quando CPF estiver vazio
    df_comb = df_comb.sort_values("data_import").drop_duplicates(subset=["Nome"], keep="last")

    df_comb.to_csv(HIST, index=False)
    print("Histórico atualizado em", HIST)

if __name__ == "__main__":
    main()

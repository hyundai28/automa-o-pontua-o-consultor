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

# -----------------------------------------------------------
# Histórico
# -----------------------------------------------------------
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

# -----------------------------------------------------------
# Utilitárias de arquivo / leitura
# -----------------------------------------------------------
def find_file(folder: Path, keywords=("consultor",), date_like_prefix=True):
    """
    Procura um arquivo na pasta `folder` que contenha qualquer keyword OR
    que comece com padrão YYYY-MM- (quando date_like_prefix=True).
    Retorna o primeiro arquivo encontrado (ordenado por name).
    """
    if not folder.exists():
        return None

    # procura arquivos contendo as keywords
    files = sorted([f for f in folder.glob("*") if f.is_file()])
    # 1) procura por keywords no nome
    for f in files:
        name = f.name.lower()
        if any(k.lower() in name for k in keywords):
            return f

    # 2) se não encontrou por keyword, procurar por padrão YYYY-MM- no começo
    if date_like_prefix:
        for f in files:
            name = f.name
            if re.match(r"^\d{4}-\d{2}[-_].*", name):
                # ex: 2025-10-cadastros.xlsx ou 2025-10_cadastros.csv
                # preferir arquivos que também contenham a palavra chave 'cadast' ou 'consult'
                lname = name.lower()
                if any(k.lower() in lname for k in keywords):
                    return f
        # se ainda não encontrou, retorna o primeiro que contenha '-cad' / '-cons' heurístico
        for f in files:
            lname = f.name.lower()
            if any(k.lower()[:4] in lname for k in keywords):
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
            # tenta detectar separador; se falhar, fallback para ,
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.read_csv(path, sep=";")
        # fallback
        return pd.read_excel(path)
    except Exception as e:
        raise RuntimeError(f"Erro lendo arquivo {path}: {e}")

# -----------------------------------------------------------
# Normalização
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

def find_col(df, candidates):
    cols = df.columns.astype(str).tolist()
    cols_low = [c.lower() for c in cols]

    for cand in candidates:
        cand_low = cand.lower()
        if cand_low in cols_low:
            return cols[cols_low.index(cand_low)]

    for c in cols:
        if "cpf" in c.lower():
            return c
    for c in cols:
        if "nome" in c.lower() or "consultor" in c.lower():
            return c
    return None

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
def main():
    try:
        # encontrar arquivos (mesma lógica de pastas como antes)
        cons_file = find_file(INPUTS_CONS, keywords=("consultor", "consultores", "consultas"))
        cad_file = find_file(INPUTS_CAD, keywords=("cadastro", "cadastros", "concessionaria", "concessionária"))

        if not cons_file or not cad_file:
            print("Erro: não encontrou os dois arquivos nas pastas inputs.")
            print(f"consultores encontrado: {cons_file}")
            print(f"cadastros encontrado: {cad_file}")
            sys.exit(1)

        print("Lendo:", cons_file, cad_file)
        df_cons = read_maybe_excel(cons_file)
        df_cad = read_maybe_excel(cad_file)

        # Identificar colunas principais
        cpf_col_cons = find_col(df_cons, ["CPF"])
        nome_col_cons = find_col(df_cons, ["Nome", "Nome Consultor", "Consultor", "NOME"])
        amostra_col_cons = find_col(df_cons, ["Amostra", "AMOSTRA"])
        dealer_col_cons = find_col(df_cons, ["Concessionária", "Dealer", "Loja", "Concessionaria"])

        cpf_col_cad = find_col(df_cad, ["CPF"])
        nome_col_cad = find_col(df_cad, ["Nome", "NOME", "Nome Completo"])

        if not cpf_col_cad or not nome_col_cad:
            print("Colunas CPF/Nome não encontradas na planilha de cadastros.")
            print(df_cad.columns.tolist())
            sys.exit(1)

        if not nome_col_cons:
            # consultores pode não ter coluna nome igual; aceitamos se tiver por CPF só
            print("Aviso: não foi encontrada coluna de Nome na planilha de consultores. Prosseguindo com CPF apenas.")

        # Normalizar cadastros (base)
        df_cad["__CPF"] = df_cad[cpf_col_cad].map(clean_cpf)
        df_cad["__NOME_CAD"] = df_cad[nome_col_cad].map(norm_name)

        # Remover duplicados por CPF no cadastro (keep last)
        df_cad = df_cad.sort_index().drop_duplicates(subset="__CPF", keep="last")

        # Normalizar consultores (onde estão as pontuações)
        if cpf_col_cons:
            df_cons["__CPF"] = df_cons[cpf_col_cons].map(clean_cpf)
        else:
            df_cons["__CPF"] = ""

        if nome_col_cons:
            df_cons["__NOME_CONS"] = df_cons[nome_col_cons].map(norm_name)
        else:
            df_cons["__NOME_CONS"] = ""

        if amostra_col_cons:
            df_cons["__AMOSTRA"] = df_cons[amostra_col_cons].fillna(0)
        else:
            df_cons["__AMOSTRA"] = 1

        # Mapas auxiliares para lookup
        # 1) por CPF (consultores)
        cons_by_cpf = {r["__CPF"]: r for _, r in df_cons.iterrows() if r["__CPF"]}
        # 2) por nome (consultores) -> dict de row por nome
        #    caso haja múltiplas linhas com mesmo nome, keep last
        cons_by_name = {}
        for _, r in df_cons.iterrows():
            name = r["__NOME_CONS"]
            if name:
                cons_by_name[name] = r

        # Preparar resultados: vamos iterar sobre CADASTROS (base)
        results = []
        unmatched_cad = []  # cadastros sem match (não pontuaram)
        cad_names = df_cons["__NOME_CONS"].tolist()
        cad_rows_list = df_cons.to_dict("records")

        for _, cad in df_cad.iterrows():
            cpf_cad = cad["__CPF"]
            nome_cad = cad["__NOME_CAD"]
            out = {
                **{k: cad.get(k, "") for k in cad.index},  # mantém colunas originais do cadastro
            }

            matched = False
            matched_source = None
            matched_row = None

            # 1) tenta CPF exato nas pontuações (se consultores tiverem CPF)
            if cpf_cad and cpf_cad in cons_by_cpf:
                matched_row = cons_by_cpf[cpf_cad]
                matched = True
                matched_source = "CPF_EXATO"

            # 2) se não, tenta encontrar por nome exato (ex.: quando consultor não tem CPF ou CPF diferente)
            if not matched and nome_cad:
                if nome_cad in cons_by_name:
                    matched_row = cons_by_name[nome_cad]
                    matched = True
                    matched_source = "NOME_EXATO"

            # 3) se ainda não achou, tenta fuzzy 100% sobre nomes (somente se consultores tiverem nomes)
            if not matched and nome_cad and cad_names:
                best_score = -1
                best_idx = None
                for i, cand_name in enumerate(cad_names):
                    score = fuzz.token_set_ratio(nome_cad, cand_name)
                    if score > best_score:
                        best_score = score
                        best_idx = i
                if best_score == 100:
                    matched_row = cad_rows_list[best_idx]
                    matched = True
                    matched_source = "NOME_FUZZY_100"

            # Construir saída
            if matched and matched_row is not None:
                # pegar CPF oficial do cadastro (sempre)
                cpf_oficial = cpf_cad if cpf_cad else ""
                # pegar nome apontado no cadastro (mantemos)
                nome_oficial = nome_cad

                # tentar obter pontuação da planilha de consultores
                # busca por colunas heurísticas: 'pontu', 'pontos', 'score', 'pontuação'
                pont_cols = [c for c in matched_row.index if re.search(r"pontu|ponto|score|pontuação|pontos", str(c).lower())]
                pontos_val = None
                if pont_cols:
                    # pegar a primeira coluna que pareça ser pontuação
                    pontos_val = matched_row[pont_cols[0]]
                else:
                    # se não achou coluna, tenta col 'PONTUACAO' exato
                    if "PONTUACAO" in matched_row:
                        pontos_val = matched_row["PONTUACAO"]

                try:
                    pontos_val = float(pontos_val) if pontos_val not in (None, "", float("nan")) else 0.0
                except Exception:
                    pontos_val = 0.0

                # Amostra: preferir valor vindo da planilha de consultores se existir, senão 0/1
                amostra_val = matched_row.get("__AMOSTRA", None)
                if amostra_val is None or (isinstance(amostra_val, float) and pd.isna(amostra_val)):
                    amostra_val = cad.get("__AMOSTRA", 1)

                out_record = {
                    "Dealer": cad.get(dealer_col_cons) if 'dealer_col_cons' in locals() else cad.get(dealer_col_cons, ""),
                    "Nome Consultor": cad.get(nome_col_cad, nome_oficial) if nome_col_cad in cad.index else nome_oficial,
                    "CPF": cpf_oficial,
                    "Amostra": amostra_val,
                    "PONTUACAO": pontos_val,
                    "_match_type": matched_source
                }
                # adicionar campos extras do matched_row para debug se quiser
                results.append(out_record)
            else:
                # cadastrado mas sem pontuação: incluímos com pontuação 0 (manter histórico)
                cpf_oficial = cpf_cad if cpf_cad else ""
                nome_oficial = nome_cad
                out_record = {
                    "Dealer": cad.get(dealer_col_cons) if 'dealer_col_cons' in locals() else cad.get(dealer_col_cons, ""),
                    "Nome Consultor": cad.get(nome_col_cad, nome_oficial) if nome_col_cad in cad.index else nome_oficial,
                    "CPF": cpf_oficial,
                    "Amostra": cad.get("__AMOSTRA", 0),
                    "PONTUACAO": 0.0,
                    "_match_type": "CADASTRADO_NAO_PONTUOU"
                }
                results.append(out_record)

        # -----------------------------------------------------------
        # SALVAR OUTPUTS
        # -----------------------------------------------------------
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        df_results = pd.DataFrame(results)

        # separar cadastrados (pontuaram ou tiveram match) dos realmente "não cadastrados" (pessoas da planilha consultores que não existem no cadastro)
        # Mas conforme regra: histórico e arquivo principal baseiam-se em cadastros, então aqui df_results contém todos os cadastros, alguns com pontuação 0.
        df_results.to_csv(OUTPUTS / f"cadastrados_{timestamp}.csv", index=False)
        print("Arquivo gerado:", OUTPUTS / f"cadastrados_{timestamp}.csv")

        # Ainda geramos um arquivo com os consultores que existem na planilha de consultores mas NÃO tem cadastro (opcional para auditoria)
        # Encontrar consultores sem cadastro (consultores com CPF não presente no cadastro e nomes não casaram)
        consultores_sem_cad = []
        cad_cpfs = set(df_cad["__CPF"].tolist())
        cad_nomes = set(df_cad["__NOME_CAD"].tolist())

        for _, c in df_cons.iterrows():
            cpf_c = c.get("__CPF", "")
            nome_c = c.get("__NOME_CONS", "")
            if cpf_c and cpf_c in cad_cpfs:
                continue
            if nome_c and nome_c in cad_nomes:
                continue
            # se chegou aqui, consultor não encontrado no cadastro
            consultores_sem_cad.append({
                "Nome Consultor": c.get(nome_col_cons, nome_c) if nome_col_cons in df_cons.columns else nome_c,
                "CPF_consultor": cpf_c,
                "_note": "NAO_CADASTRADO"
            })

        if consultores_sem_cad:
            pd.DataFrame(consultores_sem_cad).to_csv(OUTPUTS / f"nao_cadastrados_{timestamp}.csv", index=False)
            print("Arquivo de consultores sem cadastro gerado:", OUTPUTS / f"nao_cadastrados_{timestamp}.csv")
        else:
            # se não houver, criar arquivo vazio para consistência (opcional)
            pd.DataFrame(columns=["Nome Consultor", "CPF_consultor", "_note"]).to_csv(OUTPUTS / f"nao_cadastrados_{timestamp}.csv", index=False)
            print("Nenhum consultor fora do cadastro encontrado — gerado arquivo vazio.")

        # -----------------------------------------------------------
        # HISTÓRICO (somente CADASTROS — todos cadastros, mesmo que 0 pontos)
        # -----------------------------------------------------------
        df_hist_old = load_history_safe(HIST)

        # Preparar novo histórico apenas com cadastros (df_results)
        df_hist_new = pd.DataFrame({
            "Dealer": df_results.get("Dealer", ""),
            "Nome Consultor": df_results.get("Nome Consultor", ""),
            "CPF": df_results.get("CPF", ""),
            "Amostra": df_results.get("Amostra", 0),
            "data_import": datetime.utcnow().isoformat()
        })

        df_hist = pd.concat([df_hist_old, df_hist_new], ignore_index=True)
        # mantém última importação por CPF
        if "CPF" in df_hist.columns:
            df_hist = df_hist.sort_values("data_import").drop_duplicates(subset=["CPF"], keep="last")
        df_hist.to_csv(HIST, index=False)
        print("Histórico atualizado:", HIST)

    except Exception as e:
        traceback.print_exc()
        print("Erro:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

import os
import re
import sys
import traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
from rapidfuzz import process, fuzz
from unidecode import unidecode

ROOT = Path(".")
INPUTS_CAD = ROOT / "inputs" / "cadastros"
INPUTS_PONT = ROOT / "inputs" / "pontuacao"
OUTPUTS = ROOT / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------
# Utilitários
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

def find_file(folder: Path):
    if not folder.exists():
        return None
    files = list(folder.glob("*"))
    return files[0] if files else None

def detect_column(df, patterns, name=""):
    for col in df.columns:
        if any(re.search(p, str(col), re.I) for p in patterns):
            print(f"  Detectada coluna de {name}: {col}")
            return col
    return None

# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
def main():
    try:
        cad_file = find_file(INPUTS_CAD)
        pont_file = find_file(INPUTS_PONT)

        if not cad_file or not pont_file:
            print("ERRO: Uma das planilhas não foi encontrada!")
            print(f"Cadastros: {cad_file}")
            print(f"Pontuação: {pont_file}")
            sys.exit(1)

        print("Lendo planilhas...")
        df_cad = pd.read_excel(cad_file) if cad_file.suffix.lower() in ['.xlsx', '.xls'] else pd.read_csv(cad_file)
        df_pont = pd.read_excel(pont_file) if pont_file.suffix.lower() in ['.xlsx', '.xls'] else pd.read_csv(pont_file)

        print(f"  Cadastros: {len(df_cad)} linhas")
        print(f"  Pontuação: {len(df_pont)} linhas")

        # Detectar colunas
        cpf_cad_col = detect_column(df_cad, ["cpf"], "CPF (cadastros)") or df_cad.columns[0]
        nome_cad_col = detect_column(df_cad, ["nome", "consultor"], "Nome (cadastros)") or df_cad.columns[1]

        cpf_pont_col = detect_column(df_pont, ["cpf"], "CPF (pontuação)")
        nome_pont_col = detect_column(df_pont, ["nome", "consultor"], "Nome (pontuação)")
        pontuacao_col = detect_column(df_pont, ["pont", "score", "total", "resultado"], "Pontuação")
        
        if not pontuacao_col:
            print("ERRO: Não encontrada coluna de pontuação!")
            sys.exit(1)

        # Normalizar
        df_cad["CPF_clean"] = df_cad[cpf_cad_col].apply(clean_cpf)
        df_cad["NOME_clean"] = df_cad[nome_cad_col].apply(norm_name)

        df_pont["CPF_clean"] = df_pont[cpf_pont_col].apply(clean_cpf) if cpf_pont_col else ""
        df_pont["NOME_clean"] = df_pont[nome_pont_col].apply(norm_name) if nome_pont_col else ""

        # Índices rápidos
        pont_by_cpf = pd.Series(df_pont["CPF_clean"], df_pont[pontuacao_col]).to_dict()
        pont_by_name = pd.Series(df_pont["NOME_clean"], df_pont[pontuacao_col]).to_dict()

        # Para fuzzy (mais rápido que loop manual)
        choices = df_pont["NOME_clean].tolist()
        pont_by_row = df_pont.set_index("NOME_clean")[pontuacao_col].to_dict()

        results = []
        for _, row in df_cad.iterrows():
            cpf = row["CPF_clean"]
            nome = row["NOME_clean"]
            pontos = 0.0
            status = "NAO_PONTUOU"

            if cpf and cpf in pont_by_cpf:
                pontos = pont_by_cpf[cpf]
                status = "PONTUOU"
            elif nome and nome in pont_by_name:
                pontos = pont_by_name[nome]
                status = "PONTUOU"
            else:
                # Fuzzy matching
                match = process.extractOne(nome, choices, scorer=fuzz.token_set_ratio)
                if match and match[1] == 100:
                    matched_name = match[0]
                    pontos = pont_by_row.get(matched_name, 0.0)
                    status = "PONTUOU"

            try:
                pontos = float(pontos)
            except:
                pontos = 0.0

            results.append({
                "Nome": row[nome_cad_col],
                "CPF": row[cpf_cad_col],
                "Pontuacao": pontos,
                "Status": status
            })

        df_out = pd.DataFrame(results)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUTS / f"resultado_pontuacao_{ts}.xlsx"
        df_out.to_excel(output_file, index=False)

        print("\nResumo:")
        print(f"  Total de cadastros: {len(df_cad)}")
        print(f"  Pontuaram: {len(df_out[df_out['Status'] == 'PONTUOU'])}")
        print(f"  Não pontuaram: {len(df_out[df_out['Status'] == 'NAO_PONTUOU'])}")
        print(f"\nArquivo gerado: {output_file}")

    except Exception as e:
        traceback.print_exc()
        print(f"Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

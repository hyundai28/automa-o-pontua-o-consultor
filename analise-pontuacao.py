import os
import re
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
    files = list(folder.glob("*"))
    return files[0] if files else None

def detect_column(df, patterns):
    for col in df.columns:
        if any(re.search(p, str(col), re.I) for p in patterns):
            return col
    return None

def main():
    try:
        cad_file = find_file(INPUTS_CAD)
        pont_file = find_file(INPUTS_PONT)

        if not cad_file or not pont_file:
            print("ERRO: Uma das planilhas não foi enviada corretamente!")
            exit(1)

        print(f"Lendo: {cad_file.name} e {pont_file.name}")
        df_cad = pd.read_excel(cad_file)
        df_pont = pd.read_excel(pont_file)

        # Detectar colunas automaticamente
        nome_cad_col = detect_column(df_cad, ["nome", "consultor"]) or df_cad.columns[1]
        cpf_cad_col = detect_column(df_cad, ["cpf"]) or df_cad.columns[0]

        nome_pont_col = detect_column(df_pont, ["nome", "consultor"])
        cpf_pont_col = detect_column(df_pont, ["cpf"])
        pontuacao_col = detect_column(df_pont, ["pont", "score", "total", "resultado", "pontuação"])

        if not pontuacao_col:
            print("ERRO: Coluna de pontuação não encontrada!")
            exit(1)

        print(f"Colunas detectadas → Nome: {nome_cad_col} | CPF: {cpf_cad_col} | Pontuação: {pontuacao_col}")

        # Normalizar
        df_cad["CPF_clean"] = df_cad[cpf_cad_col].apply(clean_cpf)
        df_cad["NOME_clean"] = df_cad[nome_cad_col].apply(norm_name)
        df_pont["CPF_clean"] = df_pont[cpf_pont_col].apply(clean_cpf) if cpf_pont_col else pd.Series([""] * len(df_pont))
        df_pont["NOME_clean"] = df_pont[nome_pont_col].apply(norm_name) if nome_pont_col else pd.Series([""] * len(df_pont))

        # Índices rápidos
        pont_by_cpf = df_pont.set_index("CPF_clean")[pontuacao_col].to_dict()
        pont_by_name = df_pont.set_index("NOME_clean")[pontuacao_col].to_dict()
        choices = df_pont["NOME_clean"].tolist()

        results = []
        for _, row in df_cad.iterrows():
            cpf = row["CPF_clean"]
            nome = row["NOME_clean"]
            pontos = 0.0
            status = "NÃO PONTUOU"

            if cpf and cpf in pont_by_cpf:
                pontos = pont_by_cpf.get(cpf, 0)
                status = "PONTUOU"
            elif nome and nome in pont_by_name:
                pontos = pont_by_name.get(nome, 0)
                status = "PONTUOU"
            else:
 furthermore                match = process.extractOne(nome, choices, scorer=fuzz.token_set_ratio)
                if match and match[1] == 100:
                    pontos = pont_by_name.get(match[0], 0)
                    status = "PONTUOU"

            try:
                pontos = float(pontos)
            except:
                pontos = 0.0

            results.append({
                "Nome Consultor": row[nome_cad_col],
                "CPF": row[cpf_cad_col],
                "Pontuação": pontos,
                "Status": status
            })

        df_out = pd.DataFrame(results)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUTS / f"resultado_pontuacao_{ts}.xlsx"
        df_out.to_excel(output_file, index=False)

        # Salvar resumo para o log final
        total = len(df_cad)
        pontuaram = len(df_out[df_out['Status'] == 'PONTUOU'])
        nao_pontuaram = total - pontuaram
        summary = f"Total de cadastros: {total}\nPontuaram: {pontuaram}\nNão pontuaram: {nao_pontuaram}"
        print("\n" + "="*50)
        print("RESUMO DA ANÁLISE")
        print("="*50)
        print(summary)
        print("="*50)
        with open(OUTPUTS / "last_summary.txt", "w") as f:
            f.write(summary)

        print(f"\nArquivo gerado: {output_file.name}")
        print("Tudo pronto! Baixe o Excel nos Artifacts.")

    except Exception as e:
        print("ERRO CRÍTICO:", e)
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()

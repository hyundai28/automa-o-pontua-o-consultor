#!/usr/bin/env python3
"""
Análise de Pontuação Consultores - GitHub Action
Compara cadastros vs pontuação e gera relatório completo
"""

import os
import re
import sys
import traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
from rapidfuzz import process, fuzz
from unidecode import unidecode

# Configuração de pastas
ROOT = Path(".")
INPUTS_CAD = ROOT / "inputs" / "cadastros"
INPUTS_PONT = ROOT / "inputs" / "pontuacao"
OUTPUTS = ROOT / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------
# Funções utilitárias
# -----------------------------------------------------------
def print_step(title: str):
    """Imprime um passo bonito no log"""
    print(f"\n{'='*60}")
    print(f"📋 {title}")
    print(f"{'='*60}")

def clean_cpf(x):
    """Limpa CPF removendo caracteres não numéricos"""
    if pd.isna(x):
        return ""
    return re.sub(r"\D", "", str(x))

def norm_name(s):
    """Normaliza nome para comparação"""
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = unidecode(s).upper()
    s = re.sub(r"\s+", " ", s)
    return s

def find_file(folder: Path, keywords: tuple = ()) -> Path | None:
    """Encontra o primeiro arquivo na pasta"""
    if not folder.exists():
        print(f"❌ Pasta não encontrada: {folder}")
        return None
    
    files = list(folder.glob("*"))
    if not files:
        print(f"❌ Nenhum arquivo encontrado em: {folder}")
        return None
    
    # Se tem keywords, filtra
    if keywords:
        for f in files:
            if any(k.lower() in f.name.lower() for k in keywords):
                return f
    
    # Retorna o primeiro arquivo
    return files[0]

def detect_column(df: pd.DataFrame, patterns: list, name: str = "") -> str | None:
    """Detecta coluna baseada em padrões de nome"""
    for col in df.columns:
        col_lower = str(col).lower()
        if any(re.search(p, col_lower) for p in patterns):
            print(f"  ✅ {name}: '{col}'")
            return col
    print(f"  ⚠️  Coluna de {name} não encontrada - usando primeira disponível")
    return None

def read_excel_safe(path: Path) -> pd.DataFrame:
    """Lê Excel com tratamento de erros"""
    try:
        if path.suffix.lower() in ['.xlsx', '.xls']:
            return pd.read_excel(path)
        elif path.suffix.lower() == '.csv':
            try:
                return pd.read_csv(path)
            except:
                return pd.read_csv(path, sep=";")
        else:
            # Tenta como Excel por padrão
            return pd.read_excel(path)
    except Exception as e:
        print(f"❌ Erro ao ler {path}: {e}")
        raise

# -----------------------------------------------------------
# Função principal
# -----------------------------------------------------------
def main():
    print("🚀 Iniciando Análise de Pontuação Consultores")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Encontrar arquivos
        print_step("1. LOCALIZANDO ARQUIVOS")
        cad_file = find_file(INPUTS_CAD, ("cadastro", "cadastros"))
        pont_file = find_file(INPUTS_PONT, ("pontuacao", "pontuação", "consultor"))
        
        if not cad_file:
            print("❌ ERRO: Planilha de cadastros não encontrada!")
            sys.exit(1)
        if not pont_file:
            print("❌ ERRO: Planilha de pontuação não encontrada!")
            sys.exit(1)
        
        print(f"  📄 Cadastros: {cad_file.name}")
        print(f"  🏆 Pontuação: {pont_file.name}")

        # 2. Ler planilhas
        print_step("2. LENDO PLANILHAS")
        df_cad = read_excel_safe(cad_file)
        df_pont = read_excel_safe(pont_file)
        
        print(f"  📊 Cadastros carregados: {len(df_cad)} linhas, {len(df_cad.columns)} colunas")
        print(f"  🏆 Pontuações carregadas: {len(df_pont)} linhas, {len(df_pont.columns)} colunas")
        
        # Mostrar primeiras colunas para debug
        print(f"  Colunas cadastros: {list(df_cad.columns[:5])}...")
        print(f"  Colunas pontuação: {list(df_pont.columns[:5])}...")

        # 3. Detectar colunas
        print_step("3. IDENTIFICANDO COLUNAS")
        cpf_cad_col = detect_column(df_cad, ["cpf", "CPF"], "CPF (cadastros)") or df_cad.columns[0]
        nome_cad_col = detect_column(df_cad, ["nome", "consultor", "funcionario"], "Nome (cadastros)") or df_cad.columns[1]
        
        cpf_pont_col = detect_column(df_pont, ["cpf", "CPF"], "CPF (pontuação)")
        nome_pont_col = detect_column(df_pont, ["nome", "consultor", "funcionario"], "Nome (pontuação)")
        pont_col = detect_column(df_pont, ["pont", "score", "total", "resultado", "pontuação"], "Pontuação")
        
        if not pont_col:
            print("❌ ERRO: Não foi possível identificar coluna de pontuação!")
            print(f"  Colunas disponíveis: {list(df_pont.columns)}")
            sys.exit(1)

        # 4. Normalizar dados
        print_step("4. NORMALIZANDO DADOS")
        df_cad["CPF_clean"] = df_cad[cpf_cad_col].apply(clean_cpf)
        df_cad["NOME_clean"] = df_cad[nome_cad_col].apply(norm_name)
        
        df_pont["CPF_clean"] = df_pont[cpf_pont_col].apply(clean_cpf) if cpf_pont_col else ""
        df_pont["NOME_clean"] = df_pont[nome_pont_col].apply(norm_name) if nome_pont_col else ""

        # 5. Criar índices de busca
        print_step("5. CRIANDO ÍNDICES DE BUSCA")
        pont_by_cpf = dict(zip(df_pont["CPF_clean"], df_pont[pont_col]))
        pont_by_name = dict(zip(df_pont["NOME_clean"], df_pont[pont_col]))
        
        # Para fuzzy matching
        names_for_fuzzy = df_pont["NOME_clean"].dropna().tolist()
        pont_by_fuzzy_name = dict(zip(df_pont["NOME_clean"], df_pont[pont_col]))
        
        print(f"  🔍 CPFs únicos na pontuação: {len([c for c in pont_by_cpf if c])}")
        print(f"  🔍 Nomes únicos na pontuação: {len(pont_by_name)}")

        # 6. Processar cada cadastro
        print_step("6. ANALISANDO CADA CONSULTOR")
        results = []
        pontuaram = 0
        nao_pontuaram = 0
        
        for idx, row in df_cad.iterrows():
            if idx % 100 == 0 and idx > 0:
                print(f"  ⏳ Processados {idx}/{len(df_cad)} consultores...")
            
            cpf = row["CPF_clean"]
            nome = row["NOME_clean"]
            pontos = 0.0
            status = "NAO_PONTUOU"
            match_type = "Nenhum"
            
            # 1. Match exato por CPF (prioridade máxima)
            if cpf and cpf in pont_by_cpf:
                pontos = pont_by_cpf[cpf]
                status = "PONTUOU"
                match_type = "CPF_EXATO"
                pontuaram += 1
            # 2. Match exato por nome
            elif nome and nome in pont_by_name:
                pontos = pont_by_name[nome]
                status = "PONTUOU"
                match_type = "NOME_EXATO"
                pontuaram += 1
            # 3. Fuzzy matching (100% de similaridade)
            else:
                if nome and names_for_fuzzy:
                    match = process.extractOne(nome, names_for_fuzzy, scorer=fuzz.token_set_ratio)
                    if match and match[1] == 100:
                        matched_name = match[0]
                        pontos = pont_by_fuzzy_name.get(matched_name, 0.0)
                        status = "PONTUOU"
                        match_type = "FUZZY_100"
                        pontuaram += 1
            
            # Garantir que pontos seja numérico
            try:
                pontos = float(pontos) if pd.notna(pontos) else 0.0
            except (ValueError, TypeError):
                pontos = 0.0
            
            nao_pontuaram += 1 if status == "NAO_PONTUOU" else 0
            
            results.append({
                "Consultor": row[nome_cad_col],
                "CPF": row[cpf_cad_col],
                "Pontuacao": pontos,
                "Status": status,
                "Tipo_Match": match_type
            })
        
        # 7. Gerar resultado
        print_step("7. GERANDO RESULTADO FINAL")
        df_out = pd.DataFrame(results)
        
        # Estatísticas
        total_cadastros = len(df_out)
        total_pontuaram = len(df_out[df_out['Status'] == 'PONTUOU'])
        total_nao_pontuaram = len(df_out[df_out['Status'] == 'NAO_PONTUOU'])
        pontuacao_media = df_out[df_out['Pontuacao'] > 0]['Pontuacao'].mean()
        
        print(f"  📈 Total de consultores: {total_cadastros:,}")
        print(f"  🏆 Pontuaram: {total_pontuaram:,} ({total_pontuaram/total_cadastros*100:.1f}%)")
        print(f"  ❌ Não pontuaram: {total_nao_pontuaram:,} ({total_nao_pontuaram/total_cadastros*100:.1f}%)")
        print(f"  📊 Pontuação média: {pontuacao_media:.2f}" if pd.notna(pontuacao_media) else "  📊 Pontuação média: N/A")
        
        # 8. Salvar arquivo
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUTS / f"resultado_pontuacao_{ts}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Planilha principal
            df_out.to_excel(writer, sheet_name='Resultado', index=False)
            
            # Resumo estatístico
            resumo_data = {
                'Métrica': ['Total Consultores', 'Pontuaram', 'Não Pontuaram', '% Pontuaram', 'Pontuação Média'],
                'Valor': [
                    total_cadastros,
                    total_pontuaram,
                    total_nao_pontuaram,
                    f"{total_pontuaram/total_cadastros*100:.1f}%",
                    f"{pontuacao_media:.2f}" if pd.notna(pontuacao_media) else "0.00"
                ]
            }
            pd.DataFrame(resumo_data).to_excel(writer, sheet_name='Resumo', index=False)
        
        print(f"  💾 Arquivo salvo: {output_file}")
        print(f"  📏 Tamanho: {output_file.stat().st_size / 1024:.1f} KB")
        
        print_step("✅ ANÁLISE CONCLUÍDA")
        print(f"🎯 Resultado disponível em: outputs/resultado_pontuacao_{ts}.xlsx")
        
    except Exception as e:
        print_step("💥 ERRO CRÍTICO")
        traceback.print_exc()
        print(f"❌ Falha na análise: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

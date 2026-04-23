import pandas as pd
import os
from datetime import datetime

# ======================================================
# CONFIGURAÇÕES
# ======================================================
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')
NOME_ARQUIVO = f"df_agenda_{DATA_HOJE}.csv"
URL_PARQUET = "https://github.com/gatodegravata/vulp-stats/raw/refs/heads/main/base/db_matches.parquet"

def atualizar_agenda_do_dia():
    # 1. Verifica se o arquivo do dia existe
    if not os.path.exists(NOME_ARQUIVO):
        print(f"⚠️ {NOME_ARQUIVO} ainda não existe no repositório. Pulando...")
        return

    df_agenda = pd.read_csv(NOME_ARQUIVO)
    
    # 2. Verifica se há jogos pendentes
    pendentes = df_agenda[df_agenda['Status'] != 'Full'].copy()

    if pendentes.empty:
        print("✅ Todos os jogos da agenda já estão com status 'Full'.")
        return

    # 3. Carregar a base principal (apenas se houver pendências)
    print("🔍 Buscando atualizações na base principal...")
    try:
        df_principal = pd.read_parquet(URL_PARQUET)
    except Exception as e:
        print(f"❌ Erro ao carregar base principal: {e}")
        return

    # 4. Cruzar os dados usando 'Match ID'
    df_principal.set_index('Match ID', inplace=True)
    
    houve_alteracao = False
    for idx, row in pendentes.iterrows():
        match_id = row['Match ID']
        
        if match_id in df_principal.index:
            jogo_base = df_principal.loc[match_id]
            
            # Se na base principal estiver Full, atualizamos a agenda
            if jogo_base['Status'] == 'Full':
                df_agenda.at[idx, 'Goals_H_HT'] = jogo_base['Goals_H_HT']
                df_agenda.at[idx, 'Goals_A_HT'] = jogo_base['Goals_A_HT']
                df_agenda.at[idx, 'Goals_H_FT'] = jogo_base['Goals_H_FT']
                df_agenda.at[idx, 'Goals_A_FT'] = jogo_base['Goals_A_FT']
                df_agenda.at[idx, 'Status'] = 'Full'
                houve_alteracao = True
    
    # 5. Salvar se houver mudanças
    if houve_alteracao:
        df_agenda.to_csv(NOME_ARQUIVO, index=False)
        print(f"✅ Agenda {NOME_ARQUIVO} atualizada com novos placares.")
    else:
        print("ℹ️ Nenhum placar novo encontrado na base principal para os jogos de hoje.")

# EXECUÇÃO (Obrigatório para o GitHub Actions rodar)
if __name__ == "__main__":
    atualizar_agenda_do_dia()

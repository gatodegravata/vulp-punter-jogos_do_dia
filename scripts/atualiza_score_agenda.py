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
    if not os.path.exists(NOME_ARQUIVO):
        print(f"⚠️ {NOME_ARQUIVO} não encontrado.")
        return

    # FORÇANDO O TIPO: dtype={'Match ID': str} garante que seja lido como texto
    df_agenda = pd.read_csv(NOME_ARQUIVO, sep=';', dtype={'Match ID': str})
    
    if 'Status' not in df_agenda.columns:
        print("❌ Coluna 'Status' não encontrada.")
        return

    pendentes = df_agenda[df_agenda['Status'] != 'Full'].copy()

    if pendentes.empty:
        print("✅ Tudo 'Full'.")
        return

    print("🔍 Buscando na base principal...")
    try:
        df_principal = pd.read_parquet(URL_PARQUET)
        # FORÇANDO O TIPO: Garante que a base principal também tenha IDs em texto
        df_principal['Match ID'] = df_principal['Match ID'].astype(str)
    except Exception as e:
        print(f"❌ Erro na base: {e}")
        return

    df_principal.set_index('Match ID', inplace=True)
    
    houve_alteracao = False
    for idx, row in pendentes.iterrows():
        # Aqui o match_id já vai vir como string garantida
        match_id = str(row['Match ID'])
        
        if match_id in df_principal.index:
            jogo_base = df_principal.loc[match_id]
            
            # Se a base principal devolver mais de uma linha (raro), pegamos a primeira
            if isinstance(jogo_base, pd.DataFrame):
                jogo_base = jogo_base.iloc[0]

            if jogo_base['Status'] == 'Full':
                df_agenda.at[idx, 'Goals_H_HT'] = jogo_base['Goals_H_HT']
                df_agenda.at[idx, 'Goals_A_HT'] = jogo_base['Goals_A_HT']
                df_agenda.at[idx, 'Goals_H_FT'] = jogo_base['Goals_H_FT']
                df_agenda.at[idx, 'Goals_A_FT'] = jogo_base['Goals_A_FT']
                df_agenda.at[idx, 'Status'] = 'Full'
                houve_alteracao = True
    
    if houve_alteracao:
        df_agenda.to_csv(NOME_ARQUIVO, index=False, sep=';')
        print(f"✅ Agenda {NOME_ARQUIVO} atualizada.")
    else:
        print("ℹ️ Nada novo para atualizar.")

if __name__ == "__main__":
    atualizar_agenda_do_dia()

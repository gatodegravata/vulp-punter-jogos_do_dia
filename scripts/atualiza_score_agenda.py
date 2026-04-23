import pandas as pd
import os
import requests
import io
from datetime import datetime

# ======================================================
# CONFIGURAÇÕES
# ======================================================
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')
NOME_ARQUIVO = f"df_agenda_{DATA_HOJE}.csv"
# URL do repositório PRIVADO
URL_PARQUET = "https://raw.githubusercontent.com/gatodegravata/vulp-db-enriched/main/db_matches_enriched.parquet"
# O token virá de forma segura via variável de ambiente
GITHUB_TOKEN = os.getenv('VULP_TOKEN') 

def atualizar_agenda_do_dia():
    if not os.path.exists(NOME_ARQUIVO):
        print(f"⚠️ {NOME_ARQUIVO} não encontrado localmente.")
        return

    df_agenda = pd.read_csv(NOME_ARQUIVO, sep=';', dtype={'Match ID': str})
    
    #pendentes = df_agenda[df_agenda['Status'] != 'Full'].copy()
    pendentes = df_agenda.copy()
    if pendentes.empty:
        print("✅ Tudo 'Full' ou agenda vazia.")
        return

    print("🔍 Acessando base privada enriquecida...")
    try:
        # Baixando o arquivo com o token de autenticação
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        response = requests.get(URL_PARQUET, headers=headers)
        response.raise_for_status() # Explode se der erro de permissão
        
        # Lê o conteúdo binário baixado
        df_principal = pd.read_parquet(io.BytesIO(response.content))
        df_principal['Match ID'] = df_principal['Match ID'].astype(str)
    except Exception as e:
        print(f"❌ Erro ao carregar base privada: {e}")
        return

    df_principal.set_index('Match ID', inplace=True)
    
    houve_alteracao = False
    for idx, row in pendentes.iterrows():
        match_id = str(row['Match ID'])
        if match_id in df_principal.index:
            jogo_base = df_principal.loc[match_id]
            if isinstance(jogo_base, pd.DataFrame): jogo_base = jogo_base.iloc[0]

            if jogo_base['Status'] == 'Full':
                df_agenda.at[idx, 'Goals_H_HT'] = jogo_base['Goals_H_HT']
                df_agenda.at[idx, 'Goals_A_HT'] = jogo_base['Goals_A_HT']
                df_agenda.at[idx, 'Goals_H_FT'] = jogo_base['Goals_H_FT']
                df_agenda.at[idx, 'Goals_A_FT'] = jogo_base['Goals_A_FT']
                df_agenda.at[idx, 'Score'] = f"{int(jogo_base['Goals_H_FT'])} - {int(jogo_base['Goals_A_FT'])}"
                df_agenda.at[idx, 'Status'] = 'Full'
                houve_alteracao = True
    
    if houve_alteracao:

        # Converte para inteiro (tratando possíveis valores nulos)
        colunas_gols = ['Goals_H_HT', 'Goals_A_HT', 'Goals_H_FT', 'Goals_A_FT']
        for col in colunas_gols:
            df_agenda[col] = pd.to_numeric(df_agenda[col], errors='coerce').fillna(0).astype(int)
        
        df_agenda.to_csv(NOME_ARQUIVO, index=False, sep=';')
        print(f"✅ Agenda {NOME_ARQUIVO} atualizada com base privada!")
    else:
        print("ℹ️ Nenhum placar novo na base privada.")

if __name__ == "__main__":
    atualizar_agenda_do_dia()

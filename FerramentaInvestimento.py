# ======================================================= WEBSCRAPPING INVESTIMENTOS =======================================================
# ------------------------------------------------------ MAIS RETORNO & SUPERCARTEIRA ------------------------------------------------------



## Contexto
# SuperCarteira é uma plataforma de análise de investimentos que classifica produtos financeiros em categorias de acordo com o perfil de 
# risco e retorno. Usa-la para escolher investimentos para compor uma carteira de investimentos facilita a escolha de produtos financeiros
# dado que a partir dela basta comparar alguns parâmetros sobre os ativos disponíveis na maisretorno.com.

## Propósito deste código
# Criar uma plataforma para facilitar a visualização das informações dos investimentos disponíveis na SuperCarteira e maisreotrno.com.

## Sumário
# 01. Introdução
# 02. Cockpit
# 03. Funções
# 04. Manipulação de Dados
# 05. Componentes de Filtros


# ------------------------------------------------------------- 01. INTRODUÇÃO -------------------------------------------------------------



# Importa bibliotecas
import os                                                                                                                                   # Biblioteca para manuseio de arquivos
import sys
import numpy as np                                                                                                                          # Biblioteca para manuseio de dados em matriz e distribuições
import pandas as pd                                                                                                                         # Biblioteca para manuseio de dados em DataFrame
from pandas import json_normalize

# Bibliotecas para criar web apps
import io
from io import BytesIO                                                                                                                      # Biblioteca para manuseio de dados binários
import streamlit as st                                                                                                                      # Biblioteca para criar web apps

# Bibliotecas de manipulação de dados e webscrapping
import http.client
import requests
import json

# Bibliotecas para manipulação de datas e horas
import time                                                                                                                                 # Biblioteca que permite realizar operações relacionadas com tempo 
from   datetime import timedelta                                                                                                            # Biblioteca para calcular duração de trechos do código
from   datetime import datetime                                                                                                             # Biblioteca para dizer data de hoje

# Biblioteca para criar gráficos
import plotly.express as px
import plotly.io as pio

# Adiciona o caminho para a pasta principal ao caminho do sistema
sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

# Clock inicio código
Start_Time = time.monotonic()

# Define o diretório onde está salvo os arquivos que serão utilizados
wdir = os.getcwd()                                                                                                                          # Guarda a localização do diretório do arquivo
wdir = wdir.replace("\\", "/")                                                                                                              # Troca o padrão de localização da Microsoft Windows para o padrão universal
os.chdir(wdir)                                                                                                                              # Define esse como o diretório padrão para esse algoritimo

# Cria um caminho para puxar os dados brutos e outro para o armazenamento dos resultados
input_path = wdir + "/01. Input"
output_path = wdir + "/02. Output"


# --------------------------------------------------------------- 02. COCKPIT --------------------------------------------------------------



# Setup para conexão com API
conn = http.client.HTTPSConnection("api.maisretorno.com")
payload = ""
headers = { 'User-Agent': "insomnia/8.6.1" }

# Nome das categorias da SuperCarteira
list_categorias = ["ANTIFRAGILIDADE", "DIVERSIFICACAO", "ESTABILIDADE", "VALORIZACAO", "OUTROS"]

# Tempo de espera entre requisições
SLEEP_SECONDS = 2

# Define as opções de score que podem ser escolhidas
SCORE_OPTIONS = ["12m", "36m", "60m", "begin"]
TOOLTIP_SIMPLE_COLUMNS = ["name", "profitability", "volatility", "categoria"]

# Parametros para webscrapping do BTG
SIZE_PER_PAGE_BTG = 150
MAX_PRODUCTS_BTG = 750
MAX_PAGES_BTG = int(MAX_PRODUCTS_BTG / SIZE_PER_PAGE_BTG)



# -------------------------------------------------------------- 03. FUNCTIONS -------------------------------------------------------------



######### Webscrapping ########
# Função que pega cnpj de gestora de fundos de investimento desta categoria e adiciona ao df de ativos
def get_cnpj(
    dm_ativos: pd.DataFrame,
    json_path: str,
    categoria: str
):
    """
    Função que pega cnpj de gestora de fundos de investimento desta categoria e adiciona ao df de ativos

    Args:
        dm_ativos (pd.DataFrame): DataFrame com CNPJ de gestoras de fundos de investimento
        json_path (str): Caminho do arquivo json com informações sobre fundos de investimento
        categoria (str): Categoria de ativos

    Returns:
        pd.DataFrame: DataFrame com CNPJ de gestoras de fundos de investimento

    """
    # Le json
    super_carteira = pd.read_json(json_path)

    # Normaliza json
    super_carteira_norm = json_normalize(super_carteira[categoria])

    # Faz com que cnpj tenha comente numeros
    cnpj = (
        super_carteira_norm["cnpj"]
        .str.replace("/", "", regex = False)
        .str.replace(".", "", regex = False)
        .str.replace("-", "", regex = False)
    )

    # Cria df temporario com coluna de cnpj e coluna com categoria de ativo
    df_temp = pd.DataFrame({
        "cnpj": cnpj,
        "categoria": categoria
    })

    # Append o df temporario no df de ativos
    dm_ativos = pd.concat([dm_ativos, df_temp])
    
    return dm_ativos


# Function to calculate mean ignoring null values  
def calculate_mean(
    row
    ):  
    """
    Função que calcula a média igrnorando valores nulos

    Args:
        row (pd.DataFrame): DataFrame com CNPJ de gestoras de fundos de invest

    Returns:
        np.mean: Média dos scores
    """

    # Utiliza as colunas com scores de 12m, 36m, 60m e begin para fazer a média
    scores = [row['score_12m'], row['score_36m'], row['score_60m'], row['score_begin']]

    # Lista comprehension para pegar apenas valores não nulos
    scores = [score for score in scores if pd.notnull(score)]  

    # Retorna a média dos scores
    return np.mean(scores)  


# Function to calculate std ignoring null values  
def calculate_std(
    row
    ):  
    """
    Função que calcula o desvio padrão ignorando valores nulos

    Args:
        row (pd.DataFrame): DataFrame com CNPJ de gestoras de fundos de invest

    Returns:
        np.std: Desvio padrão dos scores
    """

    # Utiliza as colunas com scores de 12m, 36m, 60m e begin para fazer o desvio padrão
    scores = [row['score_12m'], row['score_36m'], row['score_60m'], row['score_begin']]  

    # Conta quantos valores nulos tem
    null_count = sum(pd.isnull(score) for score in scores)

    # Se tiver 3 valores nulos, então retorna o score de begin
    if null_count == 3:  
        return row['score_begin']

    # Caso contrário, pega apenas valores não nulos
    else:  
        scores = [score for score in scores if pd.notnull(score)]  
        return np.std(scores)


# Função que faz com que itens string dentro de uma lista se fornem integer
def string_to_int(
    string_list
    ):
    """
    Função que faz com que itens string dentro de uma lista se fornem integer

    Args:
        string_list (list): Lista de strings

    Returns:
        list: Lista de inteiros
    """
    return [int(item) for item in string_list]


# Função que executa o webscrapping
def webscrapping_maisretorno_old(
    list_categorias: list,
    tab_webscrapping: st.tabs
):
    """
    Função que executa o webscrapping

    Args:
        list_categorias (list): Lista de categorias de ativos

    Returns:
        df_result: DataFrame com dados limpos para criar gráficos
        dm_ativos: DataFrame com CNPJ de gestoras de fundos de investimento
        dm_ativos_not_found: Lista de ativos não encontrados
        list_not_found: Lista de ativos não encontrados
    """
    # Abertura de listas vazias e contadores para o Loop
    dm_ativos = pd.DataFrame()
    list_name = []
    list_profitability_12m = []
    list_volatility_12m = []
    list_profitability_36m = []
    list_volatility_36m = []
    list_profitability_60m = []
    list_volatility_60m = []
    list_profitability_begin = []
    list_volatility_begin = []
    list_sharpe_ratio_begin = []
    list_not_found = []
    count = 0

    # Cria barra de progresso e percentual do progresso do webscrapping
    progress_bar = tab_webscrapping.progress(0)

    # Para cada categoria de ativos da SuperCarteira, pega cnpj de gestora de fundos de investimento desta categoria
    for categoria in list_categorias:
        dm_ativos = get_cnpj(dm_ativos, f"{input_path}/SuperCarteira_{categoria}.json", categoria).reset_index(drop = True)
        # dm_ativos = dm_ativos.assign(Index = lambda _: _.index + 1).query("Index < 10").drop(columns = "Index")
        
    # Para cada ativo da SuperCarteira, pega rentabilidade e volatividade
    for ativo in dm_ativos.cnpj:

        # try, se nao funcionar, então guarda mensagem de erro e contagem de erro
        try:

            # Espera x segundos para não sobrecarregar o servidor
            time.sleep(SLEEP_SECONDS)
            count += 1
            get_string = "/v3/funds/stats/" + str(ativo) + "/details?="
            # conn.request("GET", "/v3/funds/stats/35940266000107/details?=", payload, headers)
            conn.request("GET", get_string, payload, headers)

            res = conn.getresponse()
            data = res.read()

            # Captura informações sobre da gestora
            company_profile = (pd.read_json(data.decode("utf-8"))
                    .reset_index()
                    .rename(columns={"index": "time_period"})
                    )
            
            # Captura nome da empresa
            name = company_profile["nicename"][0]
            
            # Contabiliza progresso em barra e um texto com o percentual de progresso
            print(f"count = {count}/{len(dm_ativos.cnpj)}", end = "\r")
            progress_bar.empty()
            progress_bar.progress(value = count / len(dm_ativos.cnpj), text = f"{count} dos {len(dm_ativos.cnpj)} ativos analisados")
            
            # Captura informações sobre produto
            product_info = (company_profile
                .copy()
                .query("time_period == 'timeframe'")
                    .stats
                    .values
                    [0]
            )

            # Captira as informações de rentabilidade e volatilidade
            profitability_12m = product_info["last_12_months"]["profitability"]
            volatility_12m = product_info["last_12_months"]["volatility"]
            profitability_36m = product_info["last_36_months"]["profitability"]
            volatility_36m = product_info["last_36_months"]["volatility"]
            profitability_60m = product_info["last_60_months"]["profitability"]
            volatility_60m = product_info["last_60_months"]["volatility"]
            profitability_begin = product_info["begin"]["profitability"]
            volatility_begin = product_info["begin"]["volatility"]
            sharpe_ratio_begin = product_info["begin"]["sharpe_ratio"]

            # Coloca informações deste produto na lista
            list_name.append(name)
            list_profitability_12m.append(profitability_12m)
            list_volatility_12m.append(volatility_12m)
            list_profitability_36m.append(profitability_36m)
            list_volatility_36m.append(volatility_36m)
            list_profitability_60m.append(profitability_60m)
            list_volatility_60m.append(volatility_60m)
            list_profitability_begin.append(profitability_begin)
            list_volatility_begin.append(volatility_begin)
            list_sharpe_ratio_begin.append(sharpe_ratio_begin)

        # Caso não seja possível puxar algum dos ativos
        except Exception as e:

            # Adicionar o ativo a lista de ativos não encontrados
            list_not_found.append(ativo)

            print(f"Erro: {e}")
            print(f"Erro no cnpj: {ativo}")
            print(f"Erro no count: {count}")

    # Transforma itens da lista de ativos não encontrados em inteiros
    list_not_found = string_to_int(list_not_found)

    # Cria DataFrame com ativos não encontrados
    dm_ativos_not_found = dm_ativos.query("cnpj == @list_not_found")

    # Remover o ativo da lista de ativos
    dm_ativos = dm_ativos.query("cnpj != @list_not_found")

    # Adiciona valores ao resultado
    df_result = (
        dm_ativos
        .copy()
        .assign(name = list_name)
        .assign(profitability_12m = list_profitability_12m)
        .assign(volatility_12m = list_volatility_12m)
        .assign(profitability_36m = list_profitability_36m)
        .assign(volatility_36m = list_volatility_36m)
        .assign(profitability_60m = list_profitability_60m)
        .assign(volatility_60m = list_volatility_60m)
        .assign(profitability_begin = list_profitability_begin)
        .assign(volatility_begin = list_volatility_begin)
        .assign(sharpe_ratio_begin = list_sharpe_ratio_begin)
        # .drop(columns = "cnpj")
        .assign(score_12m = lambda _: _.profitability_12m / _.volatility_12m)
        .assign(score_36m = lambda _: _.profitability_36m / _.volatility_36m)
        .assign(score_60m = lambda _: _.profitability_60m / _.volatility_60m)
        .assign(score_begin = lambda _: _.profitability_begin / _.volatility_begin)
        # .assign(score_mean = lambda _: np.mean([_.score_12m, _.score_36m, _.score_60m, _.score_begin], axis = 0))
        # .assign(score_std = lambda _: np.std([_.score_12m, _.score_36m, _.score_60m, _.score_begin], axis = 0))
        .assign(score_mean = lambda _: _.apply(calculate_mean, axis=1))  
        .assign(score_std = lambda _: _.apply(calculate_std, axis=1))  
        .assign(score_all = lambda _: _.score_mean / _.score_std)
        .sort_values(by = "score_all", ascending = False)
        # .sort_values(by = "score_begin", ascending = False)
    )

    # Retorna quantos ativos foram encontrados e quantos não foram
    tab_webscrapping.write(f"Quantidade de ativos encontrados: {len(dm_ativos)}")
    tab_webscrapping.write(f"Quantidade de ativos não encontrados: {len(dm_ativos_not_found)}")

    # Nomeia quais ativos não foram encontrados
    tab_webscrapping.write(f"Ativos não encontrados: {list_not_found}")

    return df_result, dm_ativos, dm_ativos_not_found, list_not_found


# Função que executa o webscrapping
def webscrapping_maisretorno(
    list_categorias: list,
    tab_webscrapping: st.tabs
):
    """
    Função que executa o webscrapping

    Args:
        list_categorias (list): Lista de categorias de ativos

    Returns:
        df_result: DataFrame com dados limpos para criar gráficos
        dm_ativos: DataFrame com CNPJ de gestoras de fundos de investimento
        dm_ativos_not_found: Lista de ativos não encontrados
        list_not_found: Lista de ativos não encontrados
    """
    # Abertura de listas vazias e contadores para o Loop
    dm_ativos = pd.DataFrame()
    df_result = pd.DataFrame()
    list_not_found = []
    count = 0

    # Dicionário que ajuda a renomear as colunas
    dict_rename_maisretorno = {
        "nicename": "name",
        "cnpj": "cnpj",
        "stats.positive_months": "positive_months",
        "stats.negative_months": "negative_months",
        "stats.timeframe.last_12_months.profitability": "profitability_12m",
        "stats.timeframe.last_12_months.volatility": "volatility_12m",
        "stats.timeframe.last_36_months.profitability": "profitability_36m",
        "stats.timeframe.last_36_months.volatility": "volatility_36m",
        "stats.timeframe.last_60_months.profitability": "profitability_60m",
        "stats.timeframe.last_60_months.volatility": "volatility_60m",
        "stats.timeframe.begin.profitability": "profitability_begin",
        "stats.timeframe.begin.volatility": "volatility_begin",
        "stats.timeframe.begin.sharpe_ratio": "sharpe_ratio_begin"
    }

    # Cria barra de progresso e percentual do progresso do webscrapping
    progress_bar = tab_webscrapping.progress(0)

    # Para cada categoria de ativos da SuperCarteira, pega cnpj de gestora de fundos de investimento desta categoria
    for categoria in list_categorias:
        dm_ativos = get_cnpj(dm_ativos, f"{input_path}/SuperCarteira_{categoria}.json", categoria).reset_index(drop = True)
        # dm_ativos = dm_ativos.assign(Index = lambda _: _.index + 1).query("Index < 3").drop(columns = "Index")
        
    # Para cada ativo da SuperCarteira, pega rentabilidade e volatividade
    for ativo in dm_ativos.cnpj:

        # try, se nao funcionar, então guarda mensagem de erro e contagem de erro
        try:

            # Espera x segundos para não sobrecarregar o servidor
            time.sleep(SLEEP_SECONDS)
            count += 1

            # Parametros para realizar o webcrawling
            url = f"https://api.maisretorno.com/v3/general/stats/{ativo}:fi"
            querystring = {"format_decimal":"false"}
            payload = ""
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://maisretorno.com/",
                "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
            }

            # Executa a busca da informação
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

            # Contabiliza progresso em barra e um texto com o percentual de progresso
            print(f"count = {count}/{len(dm_ativos.cnpj)}", end = "\r")
            progress_bar.empty()
            progress_bar.progress(value = count / len(dm_ativos.cnpj), text = f"{count} dos {len(dm_ativos.cnpj)} ativos analisados")

            # Captura a informação desse ativo e ajusta o DataFrame
            df_result_temp = (pd.json_normalize(response.json())
                    .reset_index()
                    .assign(cnpj = lambda _: ativo)
                    .rename(columns = dict_rename_maisretorno)
                    .filter(list(dict_rename_maisretorno.values()))
                    .assign(score_12m = lambda _: _.profitability_12m / _.volatility_12m)
                    .assign(score_36m = lambda _: _.profitability_36m / _.volatility_36m)
                    .assign(score_60m = lambda _: _.profitability_60m / _.volatility_60m)
                    .assign(score_begin = lambda _: _.profitability_begin / _.volatility_begin)
                    .assign(score_mean = lambda _: _.apply(calculate_mean, axis=1))  
                    .assign(score_std = lambda _: _.apply(calculate_std, axis=1))  
                    .assign(score_all = lambda _: _.score_mean / _.score_std)
                    .sort_values(by = "score_all", ascending = False)
                    .assign(total_months = lambda _: _.positive_months + _.negative_months)
                    .assign(perc_positive_months = lambda _: _.positive_months / _.total_months)
                    .assign(perc_negative_months = lambda _: _.negative_months / _.total_months)
            )

            # Junta ao DataFrame de resultados
            df_result = pd.concat([df_result, df_result_temp], ignore_index=True)

        # Caso não seja possível puxar algum dos ativos
        except Exception as e:

            # Adicionar o ativo a lista de ativos não encontrados
            list_not_found.append(ativo)

            print(f"Erro: {e}")
            print(f"Erro no cnpj: {ativo}")
            print(f"Erro no count: {count}")

    # Transforma itens da lista de ativos não encontrados em inteiros
    list_not_found = string_to_int(list_not_found)

    # Cria DataFrame com ativos não encontrados
    dm_ativos_not_found = dm_ativos.query("cnpj == @list_not_found")

    # Remover o ativo da lista de ativos
    dm_ativos = dm_ativos.query("cnpj != @list_not_found")

    # Retorna quantos ativos foram encontrados e quantos não foram
    tab_webscrapping.write(f"Quantidade de ativos encontrados: {len(dm_ativos)}")
    tab_webscrapping.write(f"Quantidade de ativos não encontrados: {len(dm_ativos_not_found)}")

    # Nomeia quais ativos não foram encontrados
    tab_webscrapping.write(f"Ativos não encontrados: {list_not_found}")

    return df_result, dm_ativos, dm_ativos_not_found, list_not_found


# Função que executa o webscrapping para descobrir os ativos disponíveis no BTG
def webscrapping_btg(
    MAX_PRODUCTS_BTG: int,
    SIZE_PER_PAGE_BTG: int
):

    """
    Função que executa o webscrapping para descobrir os ativos disponíveis no BTG

    Args:
        list_categorias (list): Lista de categorias de ativos

    Returns:
        df_result: DataFrame com dados limpos para criar gráficos
        dm_ativos: DataFrame com CNPJ de gestoras de fundos de investimento
        dm_ativos_not_found: Lista de ativos não encontrados
        list_not_found: Lista de ativos não encontrados
    """
    webscrapping_btg_result = pd.DataFrame()
    range_pages = range(1, int(MAX_PAGES_BTG) + 1)
    print(f"range_pages = {range_pages}")

    # Para cada página de produtos disponíveis no BTG
    for page in range_pages:

        # # Espera x segundos para não sobrecarregar o servidor
        # time.sleep(SLEEP_SECONDS)

        url = "https://investimentos.btgpactual.com/services/api/funds-public/public/funds/list"

        querystring = {"page": page, "size": SIZE_PER_PAGE_BTG, "sortByName": "FUND_NAME", "sortingDirection": "ASC"}

        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-language': "en-US,en;q=0.9",
            'Cookie': '_ga=GA1.1.1635766482.1724939877; _vis_opt_test_cookie=1; _vwo_uuid=DEE327AC8E341418F8C2E57E190E39C1F; rdtrk=%7B%22id%22%3A%22c13c71a9-c43f-4d74-8af2-78e9a024f52b%22%7D; _ga_9VMTRRBLL7=GS1.1.1724939876.1.1.1724939961.45.0.0; _vwo_uuid_v2=D45D5C870D65C04741508D801377C9E0E|d1fc097af737999902c0c700c77ccd01; __utmc=123482451; _tt_enable_cookie=1; _ttp=X09vOc9y2DDmLJujEcEn3jV4QJi.tt.1; _wingify_pc_uuid=584986f0ed1946baabedd2c37433030d; wingify_donot_track_actions=0; _gcl_au=1.1.1945979203.1741987652; _fbp=fb.1.1741987652271.117273033882608948; __utmz=123482451.1741987652.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); _vwo_ds=3%3Aa_0%2Ct_0%3A0%241741987652%3A24.62303202%3A%3A%3A172_0%3A1; __utmc=195331455; cebs=1; _ce.clock_data=76%2C187.35.190.234%2C1%2Cb977e10d1cb26107909e97d51a688323%2CChrome%2CBR; AWSALB=1txt7EhHlTEYeX24YL85ibuXEflxCsoELi6M5qoM8lfB6xX/DQkyGLvDQyJ4pq76SgjynVLP0lBmHH/5BgtATY/BQ+lcTgGbdGFs5qcuFh3983oK0mpVsM4uO6ih; AWSALBCORS=1txt7EhHlTEYeX24YL85ibuXEflxCsoELi6M5qoM8lfB6xX/DQkyGLvDQyJ4pq76SgjynVLP0lBmHH/5BgtATY/BQ+lcTgGbdGFs5qcuFh3983oK0mpVsM4uO6ih; efs-webapi-proxy-service=1745538992.087.127.386025|316369e728ea72285cab1efcf8e12bfb; _clck=1bti8oo%7C2%7Cfvd%7C0%7C1702; bm_sz=8759200CDC7314997DBBBC10F186ABF0~YAAQ1kLbF1JSQWeWAQAAaSSCahvt735rDwf7bXB0Vz74jDqXBB5G2KXUSTIyZq7tGSVWq9z0j/bpyjZuhVjxJfct+R7wlluHmHxdLUS7/FHNFrd5hF0RplIF2zCeI+RGkM+TrLdpaBlTD2HqUasb8/8T0L2Y4BaKod15xZE1ixsG1LMD49Nei+U3gUo//DGrlm+72dZnSI/C5UlGT//evmZCkF+dMjpgb7X1d4S3pV5S1Tq6F+HB1mVtS+3yTr95qKE9dgwrDGIoXs0org0wDmmNxs03D0I8kaKgnz5EGvkOgxpxYIZ6P53R226s8H3pq4RurkTVn3Xvu4YYgkTbRMVRT55INNG96O/TpL3RmFIa+Z7jOaUPxRYCOlI9YX8LU/jq7w5+tyhPuqzrnCv0LI0stzsCFMIdbcWYs82IqJV5qNL74viGG9b1xI4AWgKL~3556932~3294273; _abck=F9EC3C09A3D6A9FDC12928F362E94A2C~0~YAAQ1kLbF8lSQWeWAQAAdyWCag0eE86KeDxRO97HpQnFLfPWnCn2NIcdSQLeFWm8g2co/fEAr8PiViBUmVOGi4Z1uGTJc42UrCvDJckTYSNvqMI3VH+MWldBmUi1tgU5Puqd4Cqtnkegu6Zacd9ex8teGyxp2VyMLSsWju3NaFj3UYDaZgy/kLlRnlxl9B94zrsUxNbCa6Rh83LCU4J9EPkD+JXV9PN/WpUjUkpwlY3ugizI/y5KdoQ1UUXANoZ4CT3oAlh6NxZPFDW03RNUn0M2Qe38keRMDtobYBvxNKaclrFYuVKFhcxAT3RrAIqy0S4x/wI5vy660ERw6Jm/vTSr1nCeT86NcbUy5hdCWrlhdcmkVH9KFuzGieOI4SMEcCOdGDC9RnZbnVzP2clxkJP6JuI20sRCJTUW5ZBdAgMaX4dkYowA5aVSe4fy6I+9K+FbjF3CchXsodi2Men0506jHj82B1AYFsFOoNTHxrgjNPK7cqX0zwNE4j/yv79CZPgOEYYMXGy8rquHs6t+kfhNCHLoqAJd5l2eGzlnUgOQPINI7e8/eM3ALhzPtNVUgQn3qieT3krTb0ElNh23M50JQyV2uG8uJ3LgJuZyuJo9sudLNx4TJB8JyKKg3jf7UR5fCpz/EdpVdC8EVbL8ZEhJ4UuCqEj+/wLhuOCGz7IiYDkCyqZ6TTrpOaDYmIXCPA==~-1~-1~-1; __utma=195331455.1635766482.1724939877.1741987669.1745543637.2; __utmz=195331455.1745543637.2.2.utmcsr=investimentos.btgpactual.com|utmccn=(referral)|utmcmd=referral|utmcct=/; nvg70002=11c413eae3287f97897b7c60fe10|0_115; ak_bmsc=D5BC590B913430034AFBF839C34CC58A~000000000000000000000000000000~YAAQ1kLbFwRYQWeWAQAAhzCCahsS4Y2UxZKLzI01aUYMdR2mXQZa/x1u9SfxjnGVDKCIRrM9Wpj6Jt4MH6VU5DU6CD9+rEst/vq8Bcd2u34F/YI4b/sydWrxZOUi2V0h/KSmVCOBvH15upROLFaxdjjDeK2fE8nD4UsWjeHI1VcH2qk4iDIcDFYGSzvc8+hQHC0qIkMggMBOWsK4Srgl5FvPLO4dfg/Q2DVzXv/pcFTMrud3vAx0QyItR0Lopb0C/tkriVxNbeEbISdXzrnWE2kV8aON1g3rfbVxcrA0C39R8Jbg2kUcIUCjOzJxvKcqQ41712yAnefr0WtYb4smY8pAqmEdJCRr+vl7aFMkqMTSPlxFgVLPhLGo5Y4dHwb0F8heS/PO4rozDGzNJjX3iADdFumciVnLrdRgSyXG5aebLX9++RaF6eX09lx0lTPmOa+yKKi+wz6KzxiWFAUK+x6qVhCnIxrSrYjNtRE=; cebsp_=11; __utma=123482451.1635766482.1724939877.1745543640.1745541132.6; BTGPRLEMODAL=true%2Ffundos-de-investimento%2Fabsolute-atenas-p-ficfirf-crpr; _ce.s=v~d0eaedf20bd8a18203691e1b77850da82765ba23~lcw~1745545849834~vir~returning~lva~1745543650933~vpv~0~as~false~v11.cs~442038~v11.s~98079ef0-2172-11f0-80c7-ab0d53c266d6~v11.sla~1745545850056~v11.send~1745545849834~lcw~1745545850056; _ga_SYLSJKSZVZ=GS1.1.1745545746.8.1.1745545856.54.0.1959685439; _ga_L71GTFBDS4=GS1.1.1745545746.8.1.1745545856.54.0.1329494482; _uetsid=5afaec70216711f0a4468379bb9676f4; _uetvid=729b7c50a77011efb38b49ac0f388941; _vwo_sn=3558085%3A3; __trf.src=encoded_eyJmaXJzdF9zZXNzaW9uIjp7InZhbHVlIjoiKG5vbmUpIiwiZXh0cmFfcGFyYW1zIjp7fX0sImN1cnJlbnRfc2Vzc2lvbiI6eyJ2YWx1ZSI6Iihub25lKSIsImV4dHJhX3BhcmFtcyI6e319LCJjcmVhdGVkX2F0IjoxNzQ1NTQ1ODU3OTAzfQ==; ttcsid_CNH4HU3C77U1PP7E2CP0=1745545745111::nKHJPKXDl_SveeBi2E3X.4.1745545858346; ttcsid=1745545745111::11NLGZriFZVSpY1JtVS9.4.1745545858346; _clsk=1gbhs9t%7C1745545858638%7C10%7C1%7Cp.clarity.ms%2Fcollect; __utmb=123482451.0.10.1745545859892; _ga_43W2WYML5H=GS1.1.1745545746.9.1.1745545859.0.0.0; _ga_9JPZP9B352=GS1.1.1745543622.7.1.1745545859.37.0.1370457545; _dd_s=rum=2&id=86d8cf9d-17f3-41e2-aad1-dcb217edb960&created=1745545742744&expire=1745546756613; BTGNAV-INVEST=["/fundos-de-investimento/produtos","/fundos-de-investimento/produtos","/fundos-de-investimento/produtos","/fundos-de-investimento/produtos","/fundos-de-investimento/produtos"]',
            'if-none-match': 'W/"65827-B7etjPBMOBFfdJRitv0irULXt0E"',
            'priority': "u=1, i",
            'referer': "https://investimentos.btgpactual.com/fundos-de-investimento/produtos",
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': "?0",
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': "empty",
            'sec-fetch-mode': "cors",
            'sec-fetch-site': "same-origin",
            'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        }

        response = requests.get(url, headers=headers, params=querystring)

        # Print status
        print(f"---")
        print(f"BTG Page {page}")
        print(f"Status code: {response.status_code}")
        print(f"SIZE_PER_PAGE_BTG: {SIZE_PER_PAGE_BTG}")

        # Verifica se a resposta está vazia
        data = pd.json_normalize(response.json())
        if data.empty == True:
            print("Data is empty")
        else:
            print("Data is not empty")
            print(f"Data keys: {data.keys()}")

        # Caso o status code esteja ok, adiciona os dados ao DataFrame
        if response.status_code == 200 and data.empty == False:

            # Parse response to JSON
            data = response.json()

            # Flatten the JSON data
            webscrapping_btg_temp = pd.json_normalize(data['items']).rename(columns = {"CNPJ": "cnpj"})

            # Junta com o resto dos dados
            webscrapping_btg_result = pd.concat([webscrapping_btg_result, webscrapping_btg_temp], ignore_index=True)

        else:
            print("Request failed with status code:", response.status_code)

    return webscrapping_btg_result


# Função que junta os dados do webscrapping do BTG com os dados do webscrapping do Mais Retorno
def webscrapping_join(
    webscrapping_maisretorno_result: pd.DataFrame,
    webscrapping_btg_result: pd.DataFrame
):
    """
    Função que junta os dados do webscrapping do BTG com os dados do webscrapping do Mais Retorno
    Args:
        webscrapping_maisretorno_result (pd.DataFrame): DataFrame com dados do webscrapping do Mais Retorno
        webscrapping_btg_result (pd.DataFrame): DataFrame com dados do webscrapping do BTG
    Returns:
        webscrapping_join_result: DataFrame com dados do webscrapping do BTG e do Mais Retorno
    """
    # Garante que o cnpj está no formato correto
    webscrapping_btg_result = (webscrapping_btg_result
                                .copy()
                                .assign(cnpj = lambda _: _.cnpj.astype(str))
                                # .assign(cnpj = lambda _: _.cnpj.str.replace(".", "", regex = True))
    )

    # Garante que o cnpj está no formato correto
    webscrapping_maisretorno_result = (webscrapping_maisretorno_result
                        .copy()
                        .assign(cnpj = lambda _: _.cnpj.astype(str))
                        # .assign(cnpj = lambda _: _.cnpj.str.replace(".", "", regex = True))
    )

    # Lista de cnpj no BTG
    list_cnpj_btg = list(webscrapping_btg_result.cnpj.unique())

    print(f"len(webscrapping_btg_result) = {len(webscrapping_btg_result)}")
    print(f"len(webscrapping_maisretorno_result) = {len(webscrapping_maisretorno_result)}")

    # Realiza join por cnpj
    webscrapping_join_result = (webscrapping_maisretorno_result
                                .copy()
                                .assign(disponibilidade_btg = lambda _: _.cnpj.isin(list_cnpj_btg))
    )

    print(f"webscrapping_join_result = {webscrapping_join_result.head(5)}")

    return webscrapping_join_result


# Realiza ambos webscrappings e prepara os dados
def webscrapping_all(
    list_categorias: list,
    tab_webscrapping: st.tabs,
    MAX_PRODUCTS_BTG: int,
    SIZE_PER_PAGE_BTG: int
    ):
    """
    Função que executa ambos webscrappings e prepara os dados
    Args:
        list_categorias (list): Lista de categorias de ativos
        tab_webscrapping (st.tabs): Aba de webscrapping
        MAX_PRODUCTS_BTG (int): Número máximo de produtos do BTG
        SIZE_PER_PAGE_BTG (int): Número de produtos por página do BTG
    Returns:
        webscrapping_join_result: DataFrame com dados do webscrapping do BTG e do Mais Retorno
        webscrapping_btg_result: 
        webscrapping_maisretorno_result:
        dm_ativos:
        dm_ativos_not_found:
        list_not_found:
    """
    # Mostra status Progresso
    tab_webscrapping.write(f"--- Webscrapping MaisRetorno ---")

    # Executa o webscrapping do Mais Retorno
    webscrapping_maisretorno_result, dm_ativos, dm_ativos_not_found, list_not_found = webscrapping_maisretorno(
        list_categorias,
        tab_webscrapping
    )

    # Mostra status Progresso
    tab_webscrapping.write(f"--- Webscrapping BTG ---")

    # Executa o webscrapping do BTG
    webscrapping_btg_result = webscrapping_btg(
        MAX_PRODUCTS_BTG,
        SIZE_PER_PAGE_BTG
    )

    # Mostra status Progresso
    tab_webscrapping.write(f"--- Webscrapping Join ---")
    print(f"webscrapping_maisretorno_result = {webscrapping_maisretorno_result.dtypes}")
    print(f"webscrapping_btg_result = {webscrapping_btg_result.dtypes}")

    # Junta os dados do webscrapping do BTG com os dados do webscrapping do Mais Retorno
    webscrapping_join_result = webscrapping_join(
        webscrapping_maisretorno_result,
        webscrapping_btg_result
    )

    return webscrapping_join_result, webscrapping_btg_result, webscrapping_maisretorno_result, dm_ativos, dm_ativos_not_found, list_not_found


######## Data Analysis ########
# Função que prepara os dados para criar os gráficos
def clean_to_chart(
    df_result: pd.DataFrame,
    SCORE_TYPE: str = "60m",
    MAX_PROFITABILITY: float = 999999,
    MAX_VOLATILITY: float = 100,
    MIN_PROFITABILITY: float = 0,
    MIN_VOLATILITY: float = 0
):
    """
    Função que prepara os dados para criar os gráficos

    Args:
        df_result (pd.DataFrame): DataFrame com dados limpos para criar gráficos

    Returns:
        df_result_chart: DataFrame com dados limpos para criar gráficos
    """
    # Convert "categoria" column to categorical data type
    df_result["categoria"] = pd.Categorical(df_result["categoria"])

    # Config Gráfico
    PROFITABILITY = f"profitability_{SCORE_TYPE}"
    VOLATILITY = f"volatility_{SCORE_TYPE}"

    # Filtra produtos com volatilidade menor que MAX_VOLATILITY e cria um score para ordenar
    df_result_chart = (
        df_result
        .copy()
        .assign(profitability = lambda _: _[PROFITABILITY].round(2))
        .assign(volatility = lambda _: _[VOLATILITY].round(2))
        .query(f"profitability < {MAX_PROFITABILITY}")
        .query(f"volatility < {MAX_VOLATILITY}")
        .query(f"profitability > {MIN_PROFITABILITY}")
        .query(f"volatility > {MIN_VOLATILITY}")
    )

    return df_result_chart


# Cria gráfico interativo
def chart_interactive(
    df_result_chart: pd.DataFrame,
    TOOTLIP_SIMPLE: bool,
    TOOLTIP_SIMPLE_COLUMNS: list
):
    """
    Função que cria gráfico interativo

    Args:
        df_result_chart (pd.DataFrame): DataFrame com dados limpos para criar gráficos

    Returns:
        fig: Gráfico interativo
    """
    if TOOTLIP_SIMPLE == True:
        tooltip_columns = TOOLTIP_SIMPLE_COLUMNS

    else:
        tooltip_columns = list(df_result_chart.columns)

    # Create an interactive scatter plot
    fig = px.scatter(df_result_chart, x = "volatility", y = "profitability", color = "categoria",
                    hover_data = tooltip_columns)
    
    # Layout do gráfico
    fig.update_layout(title = f"SuperCarteira: Rentabilidade x Volatilidade (filtrado < {MAX_VOLATILITY})",
                        xaxis_title = f"Volatilidade_{SCORE_TYPE}",
                        yaxis_title = f"Rentabilidade_{SCORE_TYPE}",
                        legend_title = "Categoria")

    return fig


# Transformar o DataFrame em um arquivo Excel na memória
@st.cache_data
def convert_df_to_excel(df):
    """
    Função que converte um DataFrame em um arquivo Excel na memória

    Args:
        df (pd.DataFrame): DataFrame com dados

    Returns:
        processed_data: Arquivo Excel na memória
    """
    # Usando um buffer para armazenar o arquivo Excel
    from io import BytesIO

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        # writer.save()  # A chamada a `save()` pode ser omitida, pois `close()` automaticamente salva
    output.seek(0)

    return output



# ---------------------------------------------------------- 04. DATA MANIPULATION ---------------------------------------------------------



# Le arquivo com dados template
df_template = pd.read_excel("Template_SuperCarteira_Result.xlsx", engine = "openpyxl")



# -------------------------------------------------------- 05. STREAMLIT COMPONENTS --------------------------------------------------------



# Função que cria os componentes principais da aplicação
def main_components():
    """
    Função que cria os componentes principais da aplicação

    Args:
        None

    Returns:
        tab_webscrapping: Aba de webscrapping
        tab_data_analysis: Aba de data analysis
    """
    # Título da aplicação
    st.title("Ferramenta de Investimento")

    # Cria abas de navegação
    tab_webscrapping, tab_data_analysis = st.tabs(["Webscrapping", "Data_Analysis"])

    # Botão para download
    df_template_converted = convert_df_to_excel(df_template)

    return tab_webscrapping, tab_data_analysis, df_template_converted


# Função que cria a primeira parte da sidebar
def sidebar_part1(
    SCORE_OPTIONS: list
):
    """
    Função que cria a primeira parte da sidebar

    Args:
        SCORE_OPTIONS (list): Lista com opções de score

    Returns:
        SCORE_TYPE: Tipo de score escolhido pelo usuário
    """
    # Filtros da Tabela
    st.sidebar.header("Configurações")  

    # Componente de seleção para o usuário escolher SCORE_TYPE
    expander_data = st.sidebar.expander(label = "Filtros dos Dados")
    SCORE_TYPE = expander_data.selectbox(f"Escolha o horizonte dos dados", SCORE_OPTIONS)

    # Config Gráfico
    PROFITABILITY = f"profitability_{SCORE_TYPE}"
    VOLATILITY = f"volatility_{SCORE_TYPE}"

    return PROFITABILITY, VOLATILITY, SCORE_TYPE


# Função que cria a segunda parte da sidebar
def sidebar_part2(
    df_result: pd.DataFrame,
):
    """
    Função que cria a segunda parte da sidebar

    Args:
        df_result (pd.DataFrame): DataFrame com dados limpos para criar gráficos

    Returns:
        MAX_PROFITABILITY: Máxima rentabilidade
        MAX_VOLATILITY: Máxima volatilidade
        MIN_PROFITABILITY: Mínima rentabilidade
        MIN_VOLATILITY: Mínima volatilidade
    """
    # Filtros do gráfico
    expander_chart = st.sidebar.expander(label = "Filtros do Gráfico")
    expander_chart.write('''
        Modifique o gráfico de acordo com suas preferências
    ''')

    # Sidebar checkboxes para sugestões de filtros do gráfico
    USE_SUGESTION = expander_chart.checkbox(
        "Usar Sugestões de Eixos", 
        value = False, 
        help = "Ao clicar no botão 'Aceitar sugestões', você aceita os valores sugeridos para os filtros"
        )

    # Caso o usuário queira usar sugestões de filtros
    if USE_SUGESTION == True:

        # Cria sugestões de valores para os limites de filtro
        min_profitability = 0
        max_profitability = 999999
        min_volatility = 0
        max_volatility = 100

    # Caso o usuário não queira usar sugestões de filtros
    else:

        # Cria deafult values para os limites de filtro
        min_profitability = df_result[PROFITABILITY].min()
        max_profitability = df_result[PROFITABILITY].max()
        min_volatility = df_result[VOLATILITY].min()
        max_volatility = df_result[VOLATILITY].max()

    # Recebe valores que definem os limites de filtro
    MIN_PROFITABILITY, MAX_PROFITABILITY = expander_chart.slider(
        "Profitability", value = (min_profitability, max_profitability)
        )

    MIN_VOLATILITY, MAX_VOLATILITY = expander_chart.slider(
        "Volatility", value = (min_volatility, max_volatility)
        )

    # Sidebar checkboxes para sugestões de filtros do gráfico
    TOOLIP_SIMPLE = expander_chart.checkbox(
        "Usar Tootltip Simples", 
        value = False, 
        help = "Ao clicar no botão 'Usar Tootltip Simples', o tooltip usará somente mostrará colunas chaves ao invés de todas as colunas"
        )

    return MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY, TOOLIP_SIMPLE


# Função que cria a primeira parte da aba de data analysis
def tab_data_analysis_part1(
    tab_data_analysis: st.tabs,
    df_template_converted: pd.DataFrame
):
    """
    Função que cria a primeira parte da aba de data analysis

    Args:
        tab_data_analysis (st.tabs): Aba de data analysis
        df_template_converted (pd.DataFrame): DataFrame com dados convertidos

    Returns:
        df_result: DataFrame com dados limpos para criar gráficos
    """
    # Colapsa as funcionalidades de upload e download de arquivos
    expander_data_analisys = tab_data_analysis.expander(label = "Uploads/Downloads")

    # Caso o usuário clique no botão, o arquivo template será baixado
    expander_data_analisys.download_button(
        label = "Download Template",
        data = df_template_converted,
        file_name = "Template_SuperCarteira.xlsx",
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Permite usuário dar upload de arquivo com dados no formato para construir os gráficos
    Upload_Data = expander_data_analisys.file_uploader("Upload Arquivo Excel (no formato do template)", type = ["xlsx"])

    if Upload_Data is not None:

        # Pega valor de bytes do arquivo
        bytes_data = Upload_Data.getvalue()

        # Le arquivo excel
        dataframe = pd.read_excel(BytesIO(bytes_data))  
        df_result = dataframe

    # Caso não haja upload, usa dados dos default
    if Upload_Data is None:

        # Le arquivo com dados template
        df_result = pd.read_excel("Template_SuperCarteira_Result.xlsx", engine = "openpyxl")

    return df_result


# Função que cria a segunda parte da aba de data analysis
def tab_data_analysis_part2(
    tab_data_analysis: st.tabs,
    df_result: pd.DataFrame,
    SCORE_TYPE: str,
    MAX_PROFITABILITY: float,
    MAX_VOLATILITY: float,
    MIN_PROFITABILITY: float,
    MIN_VOLATILITY: float,
    TOOTLIP_SIMPLE: bool,
    TOOLTIP_SIMPLE_COLUMNS: list
):
    """
    Função que cria a segunda parte da aba de data analysis

    Args:
        tab_data_analysis (st.tabs): Aba de data analysis
        df_result (pd.DataFrame): DataFrame com dados limpos para criar gráficos
        SCORE_TYPE (str): Tipo de score escolhido pelo usuário
        MAX_PROFITABILITY (float): Máxima rentabilidade
        MAX_VOLATILITY (float): Máxima volatilidade
        MIN_PROFITABILITY (float): Mínima rentabilidade
        MIN_VOLATILITY (float): Mínima volatilidade

    Returns:
        None
    """
    # Prepara os dados para criar os gráficos
    df_result_chart = clean_to_chart(df_result, SCORE_TYPE, MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY)

    # Cria gráfico interativo
    fig = chart_interactive(df_result_chart, TOOTLIP_SIMPLE, TOOLTIP_SIMPLE_COLUMNS)

    # Exibir o gráfico Plotly no aplicativo Streamlit
    tab_data_analysis.plotly_chart(fig, use_container_width=True)

    # Converter o gráfico Plotly em HTML
    html_str = pio.to_html(fig, full_html=False)

    return None


# Função que cria a aba de webscrapping
def tab_web_scraping(
    tab_webscrapping: st.tabs
):
    """
    Função que cria a aba de webscrapping

    Args:
        tab_webscrapping (st.tabs): Aba de webscrapping

    Returns:
        None
    """
    # Cria a aba de webscrapping
    tab_webscrapping.write('''
        Aqui você pode fazer webscrapping para obter dados de investimentos
    ''')

    # Colapsa as funcionalidades de upload e download de arquivos
    expander_data_analisys = tab_webscrapping.expander(label = "Uploads/Downloads")

    # Permite usuário dar upload de arquivo com dados no formato para construir os gráficos
    Upload_Data_MaisRetorno = expander_data_analisys.file_uploader("Upload Webscrapping MaisRetorno", type = ["xlsx"])

    ##########
    # Caso o usuário clique no botão, o arquivo template será baixado
    if Upload_Data_MaisRetorno is not None:

        # Pega valor de bytes do arquivo
        bytes_data = Upload_Data_MaisRetorno.getvalue()

        # Le arquivo excel
        dataframe = pd.read_excel(BytesIO(bytes_data))  
        webscrapping_maisretorno_result = dataframe

    # Caso não haja upload, usa dados dos default
    if Upload_Data_MaisRetorno is None:

        # Le arquivo com dados template
        webscrapping_maisretorno_result = pd.read_excel("Template_Webscrapping_MaisRetorno.xlsx", engine = "openpyxl")

    # Permite usuário dar upload de arquivo com dados no formato para construir os gráficos
    Upload_Data_BTG = expander_data_analisys.file_uploader("Upload Webscrapping BTG", type = ["xlsx"])

    ##########
    # Caso o usuário clique no botão, o arquivo template será baixado
    if Upload_Data_BTG is not None:

        # Pega valor de bytes do arquivo
        bytes_data = Upload_Data_BTG.getvalue()

        # Le arquivo excel
        dataframe = pd.read_excel(BytesIO(bytes_data))  
        webscrapping_result_btg = dataframe

    # Caso não haja upload, usa dados dos default
    if Upload_Data_BTG is None:

        # Le arquivo com dados template
        df_result = pd.read_excel("Template_Webscrapping_BTG.xlsx", engine = "openpyxl")

    ##########
    # Caso seja clicado o botão de webscrapping do MaisRetorno
    if tab_webscrapping.button("Webscrapping MaisRetorno"):

        # Executa o webscrapping
        webscrapping_maisretorno_result, dm_ativos, dm_ativos_not_found, list_not_found = webscrapping_maisretorno(list_categorias, tab_webscrapping)    

        print(f"webscrapping_maisretorno_result = {webscrapping_maisretorno_result}")

        # Nome do arquivo com resultados do webscrapping
        time_now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        result_file_name = f"Investimento_Webscrapping_MaisRetorno_{time_now}.xlsx"

        # Assuming webscrapping_maisretorno_result is your DataFrame
        excel_buffer = io.BytesIO()
        webscrapping_maisretorno_result.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)  # Rewind the buffer

        # Caso o usuário clique no botão, o arquivo template será baixado
        tab_webscrapping.download_button(
            label = "Download Webscrapping MaisRetorno",
            data = excel_buffer,
            file_name = result_file_name,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )    

    ##########
    # Caso seja clicado o botão de webscrapping do BTG
    if tab_webscrapping.button("Webscrapping BTG"):

        # Executa o webscrapping
        webscrapping_btg_result = webscrapping_btg(MAX_PRODUCTS_BTG, SIZE_PER_PAGE_BTG)    

        # Nome do arquivo com resultados do webscrapping
        time_now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        result_file_name_btg = f"Investimento_Webscrapping_BTG_{time_now}.xlsx"

        # Assuming df_result is your DataFrame
        excel_buffer_btg = io.BytesIO()
        webscrapping_btg_result.to_excel(excel_buffer_btg, index=False)
        excel_buffer_btg.seek(0)  # Rewind the buffer

        # Caso o usuário clique no botão, o arquivo template será baixado
        tab_webscrapping.download_button(
            label = "Download Webscrapping BTG",
            data = excel_buffer_btg,
            file_name = result_file_name_btg,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )    

    # ##########
    # Caso seja clicado o botão de join webscrappings
    if tab_webscrapping.button("Webscrapping Join"):

        # Executa o webscrapping
        webscrapping_result_join = webscrapping_join(webscrapping_maisretorno_result, webscrapping_btg_result)    

        # Nome do arquivo com resultados do webscrapping
        time_now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        result_file_name_join = f"Investimento_Webscrapping_Join_{time_now}.xlsx"

        # Assuming df_result is your DataFrame
        excel_buffer_join = io.BytesIO()
        webscrapping_result_join.to_excel(excel_buffer_join, index=False)
        excel_buffer_join.seek(0)  # Rewind the buffer

        # Caso o usuário clique no botão, o arquivo template será baixado
        tab_webscrapping.download_button(
            label = "Download Webscrapping Join",
            data = excel_buffer_join,
            file_name = result_file_name_join,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    ##########
    # Caso seja clicado o botão de join webscrappings
    if tab_webscrapping.button("Webscrapping All"):

        # Executa o webscrapping
        webscrapping_join_result, webscrapping_btg_result, webscrapping_maisretorno_result, dm_ativos, dm_ativos_not_found, list_not_found  = webscrapping_all(
              list_categorias, 
              tab_webscrapping,
              MAX_PRODUCTS_BTG, 
              SIZE_PER_PAGE_BTG
        )

        # Nome do arquivo com resultados do webscrapping
        time_now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        result_file_name_join = f"Investimento_Webscrapping_Join_{time_now}.xlsx"

        # Assuming df_result is your DataFrame
        excel_buffer_all = io.BytesIO()
        webscrapping_join_result.to_excel(excel_buffer_all, index=False)
        excel_buffer_all.seek(0)  # Rewind the buffer

        # Caso o usuário clique no botão, o arquivo template será baixado
        tab_webscrapping.download_button(
            label = "Download Webscrapping All",
            data = excel_buffer_all,
            file_name = result_file_name_join,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )  
    

    return None



# --------------------------------------------------------- 06. STREAMLIT PIPELINE ---------------------------------------------------------



# Cria os componentes principais da aplicação
tab_webscrapping, tab_data_analysis, df_template_converted = main_components()

# Cria a aba de webscrapping
tab_web_scraping(tab_webscrapping)

# Cria a primeira parte da sidebar
PROFITABILITY, VOLATILITY, SCORE_TYPE = sidebar_part1(SCORE_OPTIONS)

# Cria a aba de data analysis
df_result = tab_data_analysis_part1(tab_data_analysis, df_template_converted)

# Cria a segunda parte da sidebar
MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY, TOOTLIP_SIMPLE = sidebar_part2(df_result)

# Cria a segunda parte da aba de data analysis
tab_data_analysis_part2(tab_data_analysis, df_result, SCORE_TYPE, MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY, TOOTLIP_SIMPLE, TOOLTIP_SIMPLE_COLUMNS)
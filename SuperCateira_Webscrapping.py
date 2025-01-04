# ======================================================= WEBSCRAPPING INVESTIMENTOS =======================================================
# ------------------------------------------------------ MAIS RETORNO & SUPERCARTEIRA ------------------------------------------------------



## Contexto
# SuperCarteira é uma plataforma de análise de investimentos que classifica produtos financeiros em categorias de acordo com o perfil de 
# risco e retorno. Usa-la para escolher investimentos para compor uma carteira de investimentos facilita a escolha de produtos financeiros
# dado que a partir dela basta comparar alguns parâmetros sobre os ativos disponíveis na maisretorno.com.

## Propósito deste código
# Puxar informações de produtos financeiros que foram classificados pela SuperCarteira para criar um gráfico que ajude a escolher quais
# investimentos são mais atrativos para compor a carteira de investimentos.

## Sumário
# 01. Introdução
# 02. Cockpit
# 03. Funções
# 04. Manipulação de Dados
# 05. Webscrapping
# 06. Cria Gráficos
# 07. Salva Resultado



# ------------------------------------------------------------- 01. INTRODUÇÃO -------------------------------------------------------------



# Importa bibilioecas necessárias
import os
import sys

# Bibliotecas relacionadas ao Tempo
import time  # Funções relacionadas ao tempo
from datetime import datetime  # Funções relacionadas ao tempo
from datetime import timedelta  # Função de duração do tempo

# Bibliotecas que fazem gráficos
import plotly.express as px
import plotly.io as pio

# Bibliotecas de manipulação de dados e webscrapping
import http.client
import pandas as pd
import numpy as np
from pandas import json_normalize

# Limpar memória
import gc
gc.collect()

# Adicione o caminho para a pasta principal ao caminho do sistema
sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

# Verifique onde este arquivo está salvo
wdir = os.getcwd()  # Determine o caminho do diretório de trabalho
wdir = wdir.replace("\\", "/")  # Substitua o caractere do caminho para corresponder à sintaxe Python
os.chdir(wdir)  # Defina o diretório de trabalho

# Caminho adicional para armazenar inputs
input_path = os.path.abspath(
    os.path.join(wdir, "/input")
)

# Caminho adicional para armazenar output
output_path = os.path.abspath(
    os.path.join(wdir, "/output")
)

# Leitura improvisada APAGAR
wdir = "/Users/64462/Documents/Dropbox/Documents_LG/Zeug/Planejamento Financeiro Pessoal/SuperCarteira/03. Code"
input_path = wdir + "/01. Input"
output_path = wdir + "/02. Output"

# Encerra este capítulo
print("01. Introdução | OK")



# ------------------------------------------------------------- 02. COCKPIT -------------------------------------------------------------



# Setup para conexão com API
conn = http.client.HTTPSConnection("api.maisretorno.com")
payload = ""
headers = { 'User-Agent': "insomnia/8.6.1" }

# Nome das categorias da SuperCarteira
list_categorias = ["ANTIFRAGILIDADE", "DIVERSIFICACAO", "ESTABILIDADE", "VALORIZACAO", "OUTROS"]

# Tempo de espera entre requisições
SLEEP_SECONDS = 2

# Maximo de volatividade permitida no gráfico
MAX_VOLATILITY = 100
MAX_PROFITABILITY = 999999999
MAX_PROFITABILITY = 3000

# Alavancas do código
SWITCH_WEBSRAPING = 1
SWITCH_CHARTS = 0
SWITCH_SAVE_CHARTS = 1
SWITCH_SAVE_EXCEL = 1
SWITCH_SAVE_CSV = 1

# Define as opções de score que podem ser escolhidas em SCORE_TYPE
SCORE_OPTIONS = ["12m", "36m", "60m", "begin"]

# Encerra este capítulo
print("02. Cockpit | OK")



# ------------------------------------------------------------- 03. FUNÇÕES -------------------------------------------------------------



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


# Função que prepara os dados para criar os gráficos
def clean_to_chart(
    df_result: pd.DataFrame,
    SCORE_TYPE: str = "60m"
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
    )

    return df_result_chart

# Cria gráfico interativo
def chart_interactive(
    df_result_chart: pd.DataFrame
):
    """
    Função que cria gráfico interativo

    Args:
        df_result_chart (pd.DataFrame): DataFrame com dados limpos para criar gráficos

    Returns:
        fig: Gráfico interativo
    """
    # Create an interactive scatter plot
    fig = px.scatter(df_result_chart, x = "volatility", y = "profitability", color = "categoria",
                    hover_data = ["name", "profitability", "volatility", "categoria"])
    
    # Layout do gráfico
    fig.update_layout(title = f"SuperCarteira: Rentabilidade x Volatilidade (filtrado < {MAX_VOLATILITY})",
                        xaxis_title = f"Volatilidade_{SCORE_TYPE}",
                        yaxis_title = f"Rentabilidade_{SCORE_TYPE}",
                        legend_title = "Categoria")

    return fig

# Encerra este capítulo
print("03. Funções | OK")



# -------------------------------------------------------- 04. MANIPULAÇÃO DE DADOS --------------------------------------------------------



# Marcar o ponto de partida do capítulo
start_time = time.monotonic()

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

# Para cada categoria de ativos da SuperCarteira, pega cnpj de gestora de fundos de investimento desta categoria
for categoria in list_categorias:
    dm_ativos = get_cnpj(dm_ativos, f"{input_path}/SuperCarteira_{categoria}.json", categoria).reset_index(drop = True)

# Encerra este capítulo
end_time = time.monotonic()
duration = timedelta(seconds=end_time - start_time)
print("04. Manipulação de Dados | OK")
print(f"    Duração: {duration}")
print(" ")



# ------------------------------------------------------------ 05. WEBSCRAPPING ------------------------------------------------------------



# Marcar o ponto de partida do capítulo
start_time = time.monotonic()

# Caso alavanca para webscrapping esteja ativada
if SWITCH_WEBSRAPING == 1:

    # Status da alavanca
    print("    SWITCH_WEBSRAPING ON")
        
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
            
            print(f"count = {count}/{len(dm_ativos.cnpj)}", end = "\r")
            
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
        .drop(columns = "cnpj")
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

# Caso alavanca para webscrapping esteja desligada
if SWITCH_WEBSRAPING == 0:

    # Status da alavanca
    print("    SWITCH_WEBSRAPING OFF")

    # Ler último resultado disponível
    df_result = pd.read_excel(f"{output_path}/SuperCarteira_Result.xlsx")

# Encerra este capítulo
end_time = time.monotonic()
duration = timedelta(seconds=end_time - start_time)
print("05. Webscrapping | OK")
print(f"    Duração: {duration}")
print(" ")



# ----------------------------------------------------------- 06. CRIA GRÁFICOS ------------------------------------------------------------



# Marcar o ponto de partida do capítulo
start_time = time.monotonic()

# Caso alavanca para criar gráficos esteja ativada
if SWITCH_CHARTS == 1:

    # Status da alavanca
    print("    SWITCH_CHARTS ON")

    # # Prepara os dados para criar os gráficos
    # df_result_chart = clean_to_chart(df_result)

    # # Cria gráfico interativo
    # fig = chart_interactive(df_result_chart)

    # Convert "categoria" column to categorical data type
    df_result["categoria"] = pd.Categorical(df_result["categoria"])

    # Define as opções de score que podem ser escolhidas em SCORE_TYPE
    SCORE_OPTIONS = ["12m", "36m", "60m", "begin"]

    # Config Gráfico
    SCORE_TYPE = "60m"
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
    )

    # Create an interactive scatter plot
    fig = px.scatter(df_result_chart, x = f"volatility", y = "profitability", color = "categoria",
                    hover_data = ["name", "profitability", "volatility", "categoria"])
    
    # Layout do gráfico
    fig.update_layout(title = f"SuperCarteira: Rentabilidade x Volatilidade (filtrado < {MAX_VOLATILITY})",
                        xaxis_title = f"Volatilidade_{SCORE_TYPE}",
                        yaxis_title = f"Rentabilidade_{SCORE_TYPE}",
                        legend_title = "Categoria")

    # Show the plot
    fig.show()

    # Download
    pio.write_html(fig, file = f"{output_path}/SuperCarteira_Chart.html", auto_open = True)

# Encerra este capítulo
end_time = time.monotonic()
duration = timedelta(seconds=end_time - start_time)
print("06. Cria Gráficos | OK")
print(f"    Duração: {duration}")
print(" ")



# ---------------------------------------------------------- 07. SALVA RESULTADO -----------------------------------------------------------



# Marcar o ponto de partida do capítulo
start_time = time.monotonic()

# Grava horário no formato para salvar o arquivo
version_save = datetime.now().strftime("%Y-%m-%d--%Hh%M")

# Caso alavanca para salvar excel esteja ativada
if SWITCH_SAVE_EXCEL == 1:

    # Status da alavanca
    print("    SWITCH_SAVE_EXCEL ON")

    # Salva o resultado em excel
    with pd.ExcelWriter(f"{output_path}/SuperCarteira_Result.xlsx", engine = "openpyxl") as writer:
        df_result.to_excel(writer, sheet_name = "Resumo", index = False)

    # Salva o copia
    with pd.ExcelWriter(f"{output_path}/SuperCarteira_Result_{version_save}.xlsx", engine = "openpyxl") as writer:
        df_result.to_excel(writer, sheet_name = "Resumo", index = False)

# Caso alavanca para salvar csv esteja ativada
if SWITCH_SAVE_CSV == 1:

    # Status da alavanca
    print("    SWITCH_SAVE_CSV ON")

    # Salva o resultado em csv
    df_result.to_csv(f"{output_path}/SuperCarteira_Result.csv", index = False)
    df_result.to_csv(f"{output_path}/SuperCarteira_Result_{version_save}.csv", index = False)

# Caso alavanca para salvar gráficos esteja ativada
if SWITCH_SAVE_CHARTS == 1:

    # Status da alavanca
    print("    SWITCH_SAVE_CHARTS ON")

    for SCORE_TYPE in SCORE_OPTIONS:

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
        )

        # Create an interactive scatter plot
        fig = px.scatter(df_result_chart, x = f"volatility", y = "profitability", color = "categoria",
                        hover_data = ["name", "profitability", "volatility", "categoria"])
        
        # Layout do gráfico
        fig.update_layout(title = f"SuperCarteira: Rentabilidade x Volatilidade (filtrado < {MAX_VOLATILITY})",
                            xaxis_title = f"Volatilidade_{SCORE_TYPE}",
                            yaxis_title = f"Rentabilidade_{SCORE_TYPE}",
                            legend_title = "Categoria")

        # Save the plot as an HTML file
        pio.write_html(fig, file = f"{output_path}/SuperCarteira_Chart_{SCORE_TYPE}.html", auto_open = True)
        pio.write_html(fig, file = f"{output_path}/SuperCarteira_Chart_{SCORE_TYPE}_{version_save}.html", auto_open = True)

# Encerra este capítulo
end_time = time.monotonic()
duration = timedelta(seconds=end_time - start_time)
print("07. Salva Arquivo | OK")
print(f"    Duração: {duration}")
print(" ")



# ------------------------------------------------------------- 99. SCRAPYWARD -------------------------------------------------------------
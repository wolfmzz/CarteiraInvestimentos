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

from io import BytesIO                                                                                                                      # Biblioteca para manuseio de dados binários

import streamlit as st                                                                                                                      # Biblioteca para criar web apps

import time                                                                                                                                 # Biblioteca que permite realizar operações relacionadas com tempo 
from   datetime import timedelta                                                                                                            # Biblioteca para calcular duração de trechos do código
from   datetime import datetime                                                                                                             # Biblioteca para dizer data de hoje
import warnings

# Biblioteca para criar gráficos
import plotly.express as px
import plotly.io as pio

# from SuperCarteira_WebScraping.py import (
#     clean_to_chart,
#     chart_interactive
# )       

# Adiciona o caminho para a pasta principal ao caminho do sistema
sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

# Clock inicio código
Start_Time = time.monotonic()

# Define o diretório onde está salvo os arquivos que serão utilizados
wdir = os.getcwd()                                                                                                                          # Guarda a localização do diretório do arquivo
wdir = wdir.replace("\\", "/")                                                                                                              # Troca o padrão de localização da Microsoft Windows para o padrão universal
os.chdir(wdir)                                                                                                                              # Define esse como o diretório padrão para esse algoritimo

# Cria um caminho para puxar os dados brutos e outro para o armazenamento dos resultados
Inputs_path  = "/01. Inputs/"
Results_path = "/02. Results/"


# --------------------------------------------------------------- 02. COCKPIT --------------------------------------------------------------



# Define as opções de score que podem ser escolhidas
SCORE_OPTIONS = ["12m", "36m", "60m", "begin"]



# -------------------------------------------------------------- 03. FUNCTIONS -------------------------------------------------------------


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



# ------------------------------------------------------- 05. COMPONENTES DE FILTROS -------------------------------------------------------



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

    return MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY


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
    # Caso o usuário clique no botão, o arquivo template será baixado
    tab_data_analysis.download_button(
        label = "Download Template",
        data = df_template_converted,
        file_name = "Template_SuperCarteira.xlsx",
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Permite usuário dar upload de arquivo com dados no formato para construir os gráficos
    Upload_Data = tab_data_analysis.file_uploader("Upload Arquivo Excel (no formato do template)", type = ["xlsx"])

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
    MIN_VOLATILITY: float
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
    fig = chart_interactive(df_result_chart)

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
MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY = sidebar_part2(df_result)

# Cria a segunda parte da aba de data analysis
tab_data_analysis_part2(tab_data_analysis, df_result, SCORE_TYPE, MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY)



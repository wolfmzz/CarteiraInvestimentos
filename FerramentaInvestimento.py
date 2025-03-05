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
from io import BytesIO                                                                                                                      # Biblioteca para manuseio de dados binários
import streamlit as st                                                                                                                      # Biblioteca para criar web apps

# Bibliotecas de manipulação de dados e webscrapping
import http.client

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
def webscrapping(
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
            progress_bar.progress(value = count / len(dm_ativos.cnpj), text = f"Percentual dos {len(dm_ativos.cnpj)} ativos analisados")
            
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

    # Retorna quantos ativos foram encontrados e quantos não foram
    tab_webscrapping.write(f"Quantidade de ativos encontrados: {len(dm_ativos)}")
    tab_webscrapping.write(f"Quantidade de ativos não encontrados: {len(dm_ativos_not_found)}")

    # Nomeia quais ativos não foram encontrados
    tab_webscrapping.write(f"Ativos não encontrados: {list_not_found}")

    return df_result, dm_ativos, dm_ativos_not_found, list_not_found

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

    if tab_webscrapping.button("Webscrapping"):

        # Executa o webscrapping
        df_result, dm_ativos, dm_ativos_not_found, list_not_found = webscrapping(list_categorias, tab_webscrapping)

        # Nome do arquivo com resultados do webscrapping
        result_file_name = f"Investimento_Webscrapping_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"

        # Caso o usuário clique no botão, o arquivo template será baixado
        tab_webscrapping.download_button(
            label = "Download Webscrapping",
            data = df_result,
            file_name = result_file_name,
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
MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY = sidebar_part2(df_result)

# Cria a segunda parte da aba de data analysis
tab_data_analysis_part2(tab_data_analysis, df_result, SCORE_TYPE, MAX_PROFITABILITY, MAX_VOLATILITY, MIN_PROFITABILITY, MIN_VOLATILITY)



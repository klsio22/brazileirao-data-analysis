from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import requests


class BrasileiraoAPI:
    def __init__(self, api_key="", mongo_uri="mongodb://127.0.0.1:27017/", 
                 db_name="statistics_futebol", collection_name="brasileirao"):
        """
        Inicializa a classe.
        
        Para apenas consultar/manipular dados do MongoDB, não é necessário fornecer a API_KEY.
        Para usar os métodos de cadastro (que dependem da API), informe a API_KEY.
        """
        self.API_KEY = api_key
        self.BASE_URL = 'https://api.football-data.org/v4/'
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        
        # Configura os headers somente se a chave da API for informada
        if self.API_KEY:
            self.headers = {'X-Auth-Token': self.API_KEY}
        else:
            self.headers = {}
    
    # ------------------------------------
    # Métodos de API (para inserir/atualizar dados novos)
    # ------------------------------------
    def obter_dados_competicao(self, competicao_id, temporada='2023'):
        """
        Obtém dados da API de futebol para a competição e temporada informadas.
        Use este método apenas se precisar cadastrar novos dados no banco.
        """
        if not self.API_KEY:
            print("API_KEY não configurada. Informe sua chave para acessar a API.")
            return None
        
        url = f"{self.BASE_URL}competitions/{competicao_id}/matches?season={temporada}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter dados da competição {competicao_id}: {response.status_code}")
            print(f"Mensagem de erro: {response.text}")
            return None
        
    def inserir_dados_mongodb(self, dados):
        """
        Insere os dados (dicionário com chave 'matches') na coleção do MongoDB.
        Use este método após obter dados via API.
        """
        if dados and 'matches' in dados:
            self.collection.insert_many(dados['matches'])
            print(f"Inseridos {len(dados['matches'])} registros na coleção '{self.collection.name}'.")
        else:
            print("Nenhum dado para inserir.")
    
    def upsert_dados_mongodb(self, dados):
        """
        Atualiza ou insere os dados (dicionário com chave 'matches') na coleção do MongoDB.
        Use este método para garantir que não haja duplicatas.
        """
        if dados and 'matches' in dados:
            for partida in dados['matches']:
                filtro = {'id': partida['id']}
                self.collection.update_one(filtro, {'$set': partida}, upsert=True)
            print(f"Atualizados/inseridos {len(dados['matches'])} registros na coleção '{self.collection.name}'.")
        else:
            print("Nenhum dado para atualizar/inserir.")
    
    # ------------------------------------
    # Métodos de manipulação de dados (consulta, listagem, etc.)
    # ------------------------------------
    def consultar_dados_mongodb(self, filtro=None):
        """
        Consulta a coleção do MongoDB e retorna um DataFrame com os dados.
        Caso informe um filtro, ele será aplicado à consulta.
        """
        if filtro:
            dados = list(self.collection.find(filtro))
        else:
            dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        return df
    
    def obter_partidas_time(self, nome_time):
        """
        Retorna as partidas em que o time informado aparece como mandante ou visitante.
        """
        query = {
            "$or": [
                {"homeTeam.name": nome_time},
                {"awayTeam.name": nome_time}
            ]
        }
        return self.consultar_dados_mongodb(filtro=query)
    
    def obter_todos_times(self):
        """
        Consulta a coleção e extrai os nomes únicos dos times cadastrados
        (tanto em 'homeTeam' quanto em 'awayTeam').
        """
        dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        times_home = []
        times_away = []
        if 'homeTeam' in df.columns:
            times_home = df['homeTeam'].apply(lambda t: t.get('name') if isinstance(t, dict) else t)
        if 'awayTeam' in df.columns:
            times_away = df['awayTeam'].apply(lambda t: t.get('name') if isinstance(t, dict) else t)
        todos_times = pd.unique(list(times_home) + list(times_away))
        return todos_times

    
    def calcular_resultado(self, row, time):
        if row['homeTeam']['name'] == time:
            if row['score']['fullTime']['home'] > row['score']['fullTime']['away']:
                return 'Vitória'
            elif row['score']['fullTime']['home'] < row['score']['fullTime']['away']:
                return 'Derrota'
            else:
                return 'Empate'
        else:
            if row['score']['fullTime']['home'] < row['score']['fullTime']['away']:
                return 'Vitória'
            elif row['score']['fullTime']['home'] > row['score']['fullTime']['away']:
                return 'Derrota'
            else:
                return 'Empate'
    
    def plot_bar(self, resultados, time):
        plt.figure(figsize=(8,6))
        resultados.plot(kind='bar', color=['green', 'red', 'grey'])
        plt.title(f'Resultados do {time} no Brasileirão 2023')
        plt.xlabel('Resultado')
        plt.ylabel('Número de Partidas')
        plt.xticks(rotation=0)
        plt.show()
    
    def plot_pie(self, resultados, time):
        plt.figure(figsize=(8,6))
        plt.pie(resultados, labels=resultados.index, autopct='%1.1f%%', colors=['green', 'red', 'grey'])
        plt.title(f'Distribuição dos Resultados do {time}')
        plt.show()
    
    def plot_gols(self, df, time):
        plt.figure(figsize=(10,6))
        plt.plot(df['gols_marcados'], label='Gols Marcados', color='blue')
        plt.plot(df['gols_sofridos'], label='Gols Sofridos', color='orange')
        plt.title(f'Gols Marcados e Sofridos pelo {time}')
        plt.xlabel('Partida')
        plt.ylabel('Número de Gols')
        plt.legend()
        plt.show()
    
    def obter_times_competicao(self, competicao_id):
        """
        Obtém os times de uma competição através da API.
        Use este método apenas quando precisar cadastrar dados.
        """
        if not self.API_KEY:
            print("API_KEY não configurada. Informe sua chave para acessar a API.")
            return None
        url = f"{self.BASE_URL}competitions/{competicao_id}/teams"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter times da competição {competicao_id}: {response.status_code}")
            print(f"Mensagem de erro: {response.text}")
            return None
    
    def verificar_time_na_competicao(self, competicao_id, nome_time):
        """
        Verifica se um time participa de uma competição com base nos dados da API.
        Use este método apenas quando precisar cadastrar dados.
        """
        dados_times = self.obter_times_competicao(competicao_id)
        if dados_times:
            times = [time['name'] for time in dados_times['teams']]
            if nome_time in times:
                print(f"O time '{nome_time}' está na competição.")
                return True
            else:
                print(f"O time '{nome_time}' não está na competição.")
                return False
        else:
            print("Não foi possível obter a lista de times da competição.")
            return False
        
    def obter_todos_times(self):
        """
        Consulta a coleção e extrai os nomes únicos dos times cadastrados
        (tanto em 'homeTeam' quanto em 'awayTeam').
        """
        dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        
        times_home = []
        times_away = []
        
        if 'homeTeam' in df.columns:
            times_home = df['homeTeam'].apply(lambda t: t.get('name') if isinstance(t, dict) else t)
        
        if 'awayTeam' in df.columns:
            times_away = df['awayTeam'].apply(lambda t: t.get('name') if isinstance(t, dict) else t)
        
        todos_times = pd.unique(list(times_home) + list(times_away))
        return sorted(todos_times)  # Retorna ordenado alfabeticamente
    
    def limpar_colecao(self):
        """
        Remove todos os documentos da coleção.
        Retorna o número de documentos removidos.
        """
        resultado = self.collection.delete_many({})
        return resultado.deleted_count
    
    def obter_vitorias_time(self, nome_time):
        # Obtém todas as partidas do time
        partidas = self.obter_partidas_time(nome_time)
        
        # Filtra apenas as vitórias
        vitorias = partidas[partidas.apply(lambda row: (
            (row['homeTeam']['name'] == nome_time and 
            row['score']['fullTime']['home'] > row['score']['fullTime']['away']) or
            (row['awayTeam']['name'] == nome_time and 
            row['score']['fullTime']['away'] > row['score']['fullTime']['home'])
        ), axis=1)]
        
        return vitorias
    
    def plot_desempenho_temporada(self, nome_time):
        """
        Cria um scatter plot do desempenho do time ao longo da temporada
        """
        # Obtém todas as partidas do time
        partidas = self.obter_partidas_time(nome_time)
        
        # Ordena as partidas por data
        partidas['utcDate'] = pd.to_datetime(partidas['utcDate'])
        partidas = partidas.sort_values('utcDate')
        
        # Calcula os gols marcados e sofridos
        partidas['gols_marcados'] = partidas.apply(
            lambda row: row['score']['fullTime']['home'] 
            if row['homeTeam']['name'] == nome_time 
            else row['score']['fullTime']['away'], 
            axis=1
        )
        
        partidas['gols_sofridos'] = partidas.apply(
            lambda row: row['score']['fullTime']['away'] 
            if row['homeTeam']['name'] == nome_time 
            else row['score']['fullTime']['home'], 
            axis=1
        )
        
        # Configuração do plot
        plt.figure(figsize=(12, 6))
        
        # Scatter plot com tamanho variável baseado no total de gols
        total_gols = partidas['gols_marcados'] + partidas['gols_sofridos']
        plt.scatter(partidas['utcDate'], partidas['gols_marcados'], 
                    s=total_gols*100, alpha=0.6, label='Gols Marcados')
        plt.scatter(partidas['utcDate'], partidas['gols_sofridos'], 
                    s=total_gols*100, alpha=0.6, label='Gols Sofridos')
        
        # Configurações do gráfico
        plt.title(f'Desempenho do {nome_time} na Temporada')
        plt.xlabel('Data')
        plt.ylabel('Número de Gols')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Rotacionar datas para melhor visualização
        plt.xticks(rotation=45)
        
        # Ajustar layout
        plt.tight_layout()
        plt.show()
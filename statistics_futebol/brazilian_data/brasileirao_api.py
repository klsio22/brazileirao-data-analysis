from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import requests


class BrasileiraoAPI:
    def __init__(self, mongo_uri="mongodb://127.0.0.1:27017/", 
                 db_name="statistics_futebol", collection_name="brasileirao"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    
    def limpar_colecao(self):
        resultado = self.collection.delete_many({})
        print(f"Removidos {resultado.deleted_count} documentos.")
        return resultado.deleted_count
    
    
    def importar_csv_para_mongodb(self, csv_path):
        """
        Lê os dados do CSV e insere no MongoDB.
        Ajuste os nomes das colunas de acordo com o seu CSV.
        """
        df = pd.read_csv(csv_path)
        
        # Ajuste os nomes das colunas conforme o seu CSV
        documentos = []
        for _, row in df.iterrows():
            documento = {
                "homeTeam": {"name": row['mandante']},
                "awayTeam": {"name": row['visitante']},
                "score": {
                    "fullTime": {
                        "home": int(row['mandante_Placar']) if 'mandante_Placar' in row else 0,
                        "away": int(row['visitante_Placar']) if 'visitante_Placar' in row else 0
                    }
                },
                "utcDate": row['data'] if 'data' in row else ""
                # Adicione outros campos conforme necessário
            }
            documentos.append(documento)
        
        if documentos:
            self.collection.insert_many(documentos)
            print(f"Inseridos {len(documentos)} documentos no MongoDB.")
        else:
            print("Nenhum documento para inserir.")

            
    def consultar_dados_mongodb(self, filtro=None):
        if filtro:
            dados = list(self.collection.find(filtro))
        else:
            dados = list(self.collection.find())
        return pd.DataFrame(dados)
    
    def obter_partidas_time(self, nome_time):
        query = {
            "$or": [
                {"homeTeam.name": nome_time},
                {"awayTeam.name": nome_time}
            ]
        }
        return self.consultar_dados_mongodb(query)

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
            times_home = df['homeTeam'].apply(
                lambda t: t.get('name') if isinstance(t, dict) and 'name' in t else None
            ).dropna().tolist()
        if 'awayTeam' in df.columns:
            times_away = df['awayTeam'].apply(
                lambda t: t.get('name') if isinstance(t, dict) and 'name' in t else None
            ).dropna().tolist()
        todos_times = pd.unique(times_home + times_away)
        return sorted(todos_times)

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
    
   
    def plot_desempenho_temporada(self, nome_time):
        # Obtém todas as partidas do time
        partidas = self.obter_partidas_time(nome_time)
        
        # Converte a coluna utcDate para datetime
        partidas['utcDate'] = pd.to_datetime(partidas['utcDate'], errors='coerce')
        partidas = partidas.sort_values('utcDate')
        
        # Calcula os gols marcados e sofridos com acesso seguro
        partidas['gols_marcados'] = partidas.apply(
            lambda row: row['score']['fullTime']['home']
                        if isinstance(row.get('homeTeam'), dict) and row['homeTeam'].get('name') == nome_time 
                        else row['score']['fullTime']['away'],
            axis=1
        )
        
        partidas['gols_sofridos'] = partidas.apply(
            lambda row: row['score']['fullTime']['away']
                        if isinstance(row.get('homeTeam'), dict) and row['homeTeam'].get('name') == nome_time 
                        else row['score']['fullTime']['home'],
            axis=1
        )
        
        # Configuração do scatter plot
        plt.figure(figsize=(12,6))
        total_gols = partidas['gols_marcados'] + partidas['gols_sofridos']
        plt.scatter(partidas['utcDate'], partidas['gols_marcados'], 
                    s=total_gols*100, alpha=0.6, label='Gols Marcados')
        plt.scatter(partidas['utcDate'], partidas['gols_sofridos'], 
                    s=total_gols*100, alpha=0.6, label='Gols Sofridos')
        
        plt.title(f'Desempenho do {nome_time} na Temporada')
        plt.xlabel('Data')
        plt.ylabel('Número de Gols')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
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
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json


class BrasileiraoAPI:
    def __init__(self, mongo_uri="mongodb://127.0.0.1:27017/", 
                 db_name="statistics_futebol", collection_name="brasileirao"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
             
    def importar_json_para_mongodb(self, json_path):
        import pandas as pd
        # Lê o arquivo JSON
        df = pd.read_json(json_path)
        
        # Converte o DataFrame para uma lista de dicionários
        registros = df.to_dict(orient='records')
        
        documentos = []
        for record in registros:
            documento = {
                "ID": record.get("ID"),
                "rodada": record.get("rodada"),
                "data": record.get("data"),
                "hora": record.get("hora"),
                "homeTeam": {
                    "name": record.get("mandante"),
                    "formacao": record.get("formacao_mandante"),
                    "tecnico": record.get("tecnico_mandante"),
                    "estado": record.get("mandante_Estado")
                },
                "awayTeam": {
                    "name": record.get("visitante"),
                    "formacao": record.get("formacao_visitante"),
                    "tecnico": record.get("tecnico_visitante"),
                    "estado": record.get("visitante_Estado")
                },
                "score": {
                    "fullTime": {
                        "home": int(record.get("mandante_Placar")),
                        "away": int(record.get("visitante_Placar"))
                    }
                },
                "vencedor": record.get("vencedor"),
                "arena": record.get("arena")
            }
            documentos.append(documento)
        
        if documentos:
            self.collection.insert_many(documentos)
            print(f"Inseridos {len(documentos)} documentos no MongoDB.")
        else:
            print("Nenhum documento para inserir.")

    def limpar_colecao(self):
        resultado = self.collection.delete_many({})
        print(f"Removidos {resultado.deleted_count} documentos.")
        return resultado.deleted_count
    
            
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
        

    def consultar_dados_mongodb(self, filtro=None):
        if filtro:
            dados = list(self.collection.find(filtro))
        else:
            dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        return df

    def montar_tabelas(self, start_year=2003, end_year=2022):
        """
        Agrega os resultados (jogos, vitórias, empates, derrotas, gols marcados, gols sofridos e pontos)
        para cada time, por temporada (de start_year até end_year), e retorna um JSON com as tabelas.
        """
        df = self.consultar_dados_mongodb()
        df["data_dt"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df = df.dropna(subset=["data_dt"])
        df = df[(df["data_dt"].dt.year >= start_year) & (df["data_dt"].dt.year <= end_year)]
        
        tabelas = {}
        for season in range(start_year, end_year + 1):
            tabelas[str(season)] = {}

        def atualizar_time(tabela, time, gols_feitos, gols_sofridos, resultado):
            if time not in tabela:
                tabela[time] = {
                    "jogos": 0,
                    "vitorias": 0,
                    "empates": 0,
                    "derrotas": 0,
                    "gols_marcados": 0,
                    "gols_sofridos": 0,
                    "pontos": 0
                }
            tabela[time]["jogos"] += 1
            tabela[time]["gols_marcados"] += gols_feitos
            tabela[time]["gols_sofridos"] += gols_sofridos
            if resultado == "Vitória":
                tabela[time]["vitorias"] += 1
                tabela[time]["pontos"] += 3
            elif resultado == "Empate":
                tabela[time]["empates"] += 1
                tabela[time]["pontos"] += 1
            elif resultado == "Derrota":
                tabela[time]["derrotas"] += 1

        for _, row in df.iterrows():
            season = str(row["data_dt"].year)
            home_team = row["homeTeam"]["name"] if isinstance(row["homeTeam"], dict) else row["homeTeam"]
            away_team = row["awayTeam"]["name"] if isinstance(row["awayTeam"], dict) else row["awayTeam"]
            score_home = row["score"]["fullTime"]["home"]
            score_away = row["score"]["fullTime"]["away"]

            if score_home > score_away:
                resultado_home = "Vitória"
                resultado_away = "Derrota"
            elif score_home < score_away:
                resultado_home = "Derrota"
                resultado_away = "Vitória"
            else:
                resultado_home = "Empate"
                resultado_away = "Empate"
            
            atualizar_time(tabelas[season], home_team, score_home, score_away, resultado_home)
            atualizar_time(tabelas[season], away_team, score_away, score_home, resultado_away)
        
        return json.dumps(tabelas, indent=4, ensure_ascii=False)
    
    def exportar_tabelas_json(self, start_year=2003, end_year=2022, output_path="../../data/tabelas_aggregadas.json"):
        """
        Gera as tabelas agregadas via montar_tabelas, salva o JSON gerado em um arquivo e retorna o caminho do arquivo.
        """
        tabelas_json = self.montar_tabelas(start_year, end_year)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(tabelas_json)
        print(f"Arquivo JSON com as tabelas criado em: {output_path}")
        return output_path

    def inserir_tabelas_no_mongodb(self, start_year=2003, end_year=2022, collection_name="tabelas_aggregadas"):
        """
        Insere as tabelas agregadas em uma nova coleção do MongoDB.
        Cada documento conterá a temporada (season) e os dados agregados para os times.
        """
        import json
        # Gera o JSON das tabelas
        tabelas_json = self.montar_tabelas(start_year, end_year)
        tabelas_dict = json.loads(tabelas_json)
        
        # Define a nova coleção
        nova_colecao = self.db[collection_name]
        
        documentos = []
        for season, tabela in tabelas_dict.items():
            documento = {
                "season": season,
                "tabela": tabela
            }
            documentos.append(documento)
        
        if documentos:
            nova_colecao.insert_many(documentos)
            print(f"Inseridos {len(documentos)} documentos na coleção '{collection_name}'.")
        else:
            print("Nenhum documento para inserir.")
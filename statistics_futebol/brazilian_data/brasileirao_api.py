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
            
    
  
    def gerar_odds_todos_times(self, start_year=2003, end_year=2022,
                          collection_name="odds_times_aggregados",
                          output_path="../../data/odds_times_aggregados.json"):
        """
        Agrega os dados de desempenho de cada time para as temporadas de start_year a end_year,
        incluindo as médias de vitórias (homeWin), empates (draw) e derrotas (awayWin).
        """
        import json
        import pandas as pd
        
        df = self.consultar_dados_mongodb()
        df["data_dt"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df = df.dropna(subset=["data_dt"])
        
        odds_list = []
        
        for season in range(start_year, end_year + 1):
            df_season = df[df["data_dt"].dt.year == season]
            if df_season.empty:
                continue
            
            times_home = df_season["homeTeam"].apply(lambda t: t.get("name") if isinstance(t, dict) else t)
            times_away = df_season["awayTeam"].apply(lambda t: t.get("name") if isinstance(t, dict) else t)
            unique_times = pd.unique(list(times_home) + list(times_away))
            
            for time in unique_times:
                dados = {
                    "time": time,
                    "season": season,
                    "jogos": 0,
                    "vitorias": 0,
                    "empates": 0,
                    "derrotas": 0,
                    "gols_marcados": 0,
                    "gols_sofridos": 0,
                    "pontos": 0,
                    "odds": {
                        "homeWin": None,  # média de vitórias
                        "draw": None,     # média de empates
                        "awayWin": None   # média de derrotas
                    }
                }
                
                df_team = df_season[
                    (df_season["homeTeam"].apply(lambda t: t.get("name") if isinstance(t, dict) else t) == time) |
                    (df_season["awayTeam"].apply(lambda t: t.get("name") if isinstance(t, dict) else t) == time)
                ]
                
                for _, row in df_team.iterrows():
                    dados["jogos"] += 1
                    
                    home_team = row["homeTeam"].get("name") if isinstance(row["homeTeam"], dict) else row["homeTeam"]
                    away_team = row["awayTeam"].get("name") if isinstance(row["awayTeam"], dict) else row["awayTeam"]
                    score_home = row["score"]["fullTime"]["home"]
                    score_away = row["score"]["fullTime"]["away"]
                    
                    if home_team == time:
                        dados["gols_marcados"] += score_home
                        dados["gols_sofridos"] += score_away
                        if score_home > score_away:
                            dados["vitorias"] += 1
                            dados["pontos"] += 3
                        elif score_home == score_away:
                            dados["empates"] += 1
                            dados["pontos"] += 1
                        else:
                            dados["derrotas"] += 1
                    elif away_team == time:
                        dados["gols_marcados"] += score_away
                        dados["gols_sofridos"] += score_home
                        if score_away > score_home:
                            dados["vitorias"] += 1
                            dados["pontos"] += 3
                        elif score_away == score_home:
                            dados["empates"] += 1
                            dados["pontos"] += 1
                        else:
                            dados["derrotas"] += 1
                
                # Calcula as médias com duas casas decimais
                if dados["jogos"] > 0:
                    dados["odds"]["homeWin"] = round(dados["vitorias"] / dados["jogos"], 2)
                    dados["odds"]["draw"] = round(dados["empates"] / dados["jogos"], 2)
                    dados["odds"]["awayWin"] = round(dados["derrotas"] / dados["jogos"], 2)
                
                odds_list.append(dados)
        
        if odds_list:
            nova_colecao = self.db[collection_name]
            nova_colecao.insert_many(odds_list)
            print(f"Inseridos {len(odds_list)} documentos na coleção '{collection_name}'.")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(odds_list, f, indent=4, ensure_ascii=False, default=str)
        print(f"Arquivo JSON com os odds de cada time criado em: {output_path}")
        
        return odds_list
        
    def limpar_colecao_por_nome(self, collection_name):
        """
        Limpa a coleção especificada, removendo todos os documentos.
        """
        result = self.db[collection_name].delete_many({})
        print(f"Removidos {result.deleted_count} documentos da coleção '{collection_name}'.")
        return result.deleted_count
    
    
    def plot_desempenho_time(self, nome_time):
        """
        Gera um scatter plot com reta de regressão para o time especificado.
        - Eixo x: Temporadas (years)
        - Eixo y: Média de desempenho (média de homeWin, draw e awayWin)
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # Consulta os dados agregados do time em todas as temporadas
        dados = list(self.db["odds_times_aggregados"].find({"time": nome_time}))
        if not dados:
            print(f"Nenhum dado encontrado para o time {nome_time}")
            return

        # Ordena os dados por temporada para visualização cronológica
        dados = sorted(dados, key=lambda d: d["season"])
        
        temporadas = []
        medias_desempenho = []

        for doc in dados:
            temporadas.append(doc["season"])
            odds = doc["odds"]
            media = np.mean([
                odds["homeWin"] or 0,
                odds["draw"] or 0,
                odds["awayWin"] or 0
            ])
            medias_desempenho.append(media)

        plt.figure(figsize=(12, 8))
        plt.scatter(temporadas, medias_desempenho, s=100, alpha=0.7, c='blue', label=nome_time)

        # Ajusta a reta de regressão (as temporadas são numéricas)
        coef, intercept = np.polyfit(temporadas, medias_desempenho, 1)
        x_line = np.linspace(min(temporadas), max(temporadas), 100)
        y_line = coef * x_line + intercept
        plt.plot(x_line, y_line, "r--", alpha=0.8, label=f"y = {coef:.2f}x + {intercept:.2f}")

        plt.xlabel("Temporadas")
        plt.ylabel("Média de Desempenho")
        plt.title(f"Desempenho do {nome_time} ao longo das Temporadas")
        plt.xticks(temporadas, rotation=45)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        
 
    def plot_media_porcentagem_time(self, nome_time):
        """
        Gera um gráfico de barras agrupadas mostrando a média percentual
        de vitórias (homeWin), empates (draw) e derrotas (awayWin) do time em cada temporada.
        Os valores são apresentados em porcentagem, com ticks do eixo y de 5 em 5 e 
        os valores exibidos em cima de cada barra.
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # Consulta os dados agregados do time em todas as temporadas
        dados = list(self.db["odds_times_aggregados"].find({"time": nome_time}))
        if not dados:
            print(f"Nenhum dado encontrado para o time {nome_time}")
            return

        # Ordena os dados por temporada para visualização cronológica
        dados = sorted(dados, key=lambda d: d["season"])
        
        temporadas = [doc["season"] for doc in dados]
        # Converte os valores em porcentagem (multiplica por 100) e arredonda para 2 casas
        vit_percent = [round((doc["odds"]["homeWin"] or 0) * 100, 2) for doc in dados]
        emp_percent = [round((doc["odds"]["draw"] or 0) * 100, 2) for doc in dados]
        der_percent = [round((doc["odds"]["awayWin"] or 0) * 100, 2) for doc in dados]
        
        # Configuração do gráfico de barras agrupadas
        x = np.arange(len(temporadas))
        largura = 0.25
    
        plt.figure(figsize=(12, 8))
        bars1 = plt.bar(x - largura, vit_percent, width=largura, color='green', label='Vitórias (%)')
        bars2 = plt.bar(x, emp_percent, width=largura, color='gray', label='Empates (%)')
        bars3 = plt.bar(x + largura, der_percent, width=largura, color='red', label='Derrotas (%)')
        
        plt.xlabel("Temporadas")
        plt.ylabel("Porcentagem (%)")
        plt.title(f"Média Percentual de Desempenho do {nome_time} por Temporada")
        plt.xticks(x, temporadas, rotation=45)
        
        # Configura os ticks do eixo y de 5 em 5
        max_y = max(max(vit_percent), max(emp_percent), max(der_percent))
        y_max = (int(max_y / 5) + 1) * 5
        plt.yticks(np.arange(0, y_max+1, 5))
        
        # Grid com major e minor ticks
        plt.minorticks_on()
        plt.grid(axis='y', which='major', linestyle='-', linewidth=0.5, alpha=0.7)
        plt.grid(axis='y', which='minor', linestyle='--', linewidth=0.5, alpha=0.5)
        
        # Adiciona os valores de porcentagem acima de cada barra
        for bars in [bars1, bars2, bars3]:
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2.0, height, f'{height:.1f}%', 
                        ha='center', va='bottom', fontsize=8)
        
        plt.legend()
        plt.tight_layout()
        plt.show()
 
    def plot_desempenho_todos_times(self):
        """
        Gera um scatter plot mostrando a performance (pontos / (jogos*3))
        de cada time em cada temporada. Cada ponto representa um time em uma temporada
        e as cores diferenciam os times. Além disso, uma reta horizontal é adicionada
        representando a média geral de performance.
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # Consulta todos os documentos agregados de odds e desempenho
        docs = list(self.db["odds_times_aggregados"].find())
        if not docs:
            print("Nenhum dado encontrado para exibir.")
            return

        # Obtém a lista única de times e mapeia cada time para uma cor
        teams = sorted(set(doc["time"] for doc in docs))
        cmap = plt.get_cmap("tab20", len(teams))
        colors = {team: cmap(i) for i, team in enumerate(teams)}

        plt.figure(figsize=(12, 8))
        performance_values = []  # Armazena todas as performances para cálculo da média

        # Para cada documento, calcula a performance como pontos / (jogos * 3)
        for doc in docs:
            season = int(doc["season"])
            jogos = doc.get("jogos", 0)
            pontos = doc.get("pontos", 0)
            # Evita divisão por zero
            performance = pontos / (jogos * 3) if jogos > 0 else 0
            performance_values.append(performance)
            team = doc["time"]
            plt.scatter(season, performance, color=colors[team], alpha=0.7, s=100)

        # Adiciona uma reta horizontal representando a média geral de performance
        media_geral = np.mean(performance_values)
        plt.axhline(y=media_geral, color='black', linestyle='--', linewidth=2, label=f"Média Geral ({media_geral:.2f})")

        # Cria uma legenda sem duplicatas (um item por time)
        legend_handles = []
        for team in teams:
            handle = plt.Line2D([], [], marker='o', linestyle='None',
                                markersize=8, color=colors[team])
            legend_handles.append(handle)

        # Adiciona a linha da média geral à legenda
        legend_handles.append(plt.Line2D([], [], color='black', linestyle='--', linewidth=2))
        teams.append("Média Geral")
        
        plt.legend(legend_handles, teams, bbox_to_anchor=(1.05, 1),
                loc='upper left', borderaxespad=0.)
        plt.xlabel("Temporadas")
        plt.ylabel("Performance (%)")
        plt.title("Performance dos Times ao longo das Temporadas (Pontos / (Jogos × 3))")
        
        # Ajusta o eixo x com as temporadas
        temporadas = sorted(set(int(doc["season"]) for doc in docs))
        plt.xticks(temporadas, rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
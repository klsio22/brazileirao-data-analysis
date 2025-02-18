from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import json


class BrasileiraoAPI:
    def __init__(self, mongo_uri="mongodb://127.0.0.1:27017/", 
                 db_name="statistics_futebol", collection_name="brasileirao"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
             
    def importar_json_para_mongodb(self, json_path):
        import pandas as pd
        df = pd.read_json(json_path)
        
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
        if filtro:
            dados = list(self.collection.find(filtro))
        else:
            dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        return df
    
    def obter_partidas_time(self, nome_time):
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
        partidas = self.obter_partidas_time(nome_time)
        
        partidas['utcDate'] = pd.to_datetime(partidas['utcDate'], errors='coerce')
        partidas = partidas.sort_values('utcDate')
        
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
        
        partidas = self.obter_partidas_time(nome_time)
        
        partidas['utcDate'] = pd.to_datetime(partidas['utcDate'])
        partidas = partidas.sort_values('utcDate')
        
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
        

        plt.figure(figsize=(12, 6))
        
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
        

    def consultar_dados_mongodb(self, filtro=None):
        if filtro:
            dados = list(self.collection.find(filtro))
        else:
            dados = list(self.collection.find())
        df = pd.DataFrame(dados)
        return df

    def montar_tabelas(self, start_year=2003, end_year=2022):
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
        tabelas_json = self.montar_tabelas(start_year, end_year)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(tabelas_json)
        print(f"Arquivo JSON com as tabelas criado em: {output_path}")
        return output_path

    def inserir_tabelas_no_mongodb(self, start_year=2003, end_year=2022, collection_name="tabelas_aggregadas"):
        import json
        tabelas_json = self.montar_tabelas(start_year, end_year)
        tabelas_dict = json.loads(tabelas_json)
        
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
                        "homeWin": None,  
                        "draw": None,    
                        "awayWin": None  
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
        result = self.db[collection_name].delete_many({})
        print(f"Removidos {result.deleted_count} documentos da coleção '{collection_name}'.")
        return result.deleted_count
    
    
    def plot_desempenho_time(self, nome_time):
        import matplotlib.pyplot as plt
        import numpy as np

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
        import matplotlib.pyplot as plt
        import numpy as np

        dados = list(self.db["odds_times_aggregados"].find({"time": nome_time}))
        if not dados:
            print(f"Nenhum dado encontrado para o time {nome_time}")
            return

        dados = sorted(dados, key=lambda d: d["season"])
        
        temporadas = [doc["season"] for doc in dados]
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
        

        for bars in [bars1, bars2, bars3]:
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2.0, height, f'{height:.1f}%', 
                        ha='center', va='bottom', fontsize=8)
        
        plt.legend()
        plt.tight_layout()
        plt.show()
 
    def plot_desempenho_todos_times(self):

        import matplotlib.pyplot as plt
        import numpy as np

        docs = list(self.db["odds_times_aggregados"].find())
        if not docs:
            print("Nenhum dado encontrado para exibir.")
            return

        teams = sorted(set(doc["time"] for doc in docs))
        cmap = plt.get_cmap("tab20", len(teams))
        colors = {team: cmap(i) for i, team in enumerate(teams)}

        plt.figure(figsize=(12, 8))
        performance_values = [] 

        # Para cada documento, calcula a performance como pontos / (jogos * 3)
        for doc in docs:
            season = int(doc["season"])
            jogos = doc.get("jogos", 0)
            pontos = doc.get("pontos", 0)
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
    
    
    def buscar_partidas_por_confronto(self, time1, time2):
        query = {
            "$or": [
                {"$and": [
                    {"homeTeam.name": {"$regex": f"^{time1}$", "$options": "i"}},
                    {"awayTeam.name": {"$regex": f"^{time2}$", "$options": "i"}}
                ]},
                {"$and": [
                    {"homeTeam.name": {"$regex": f"^{time2}$", "$options": "i"}},
                    {"awayTeam.name": {"$regex": f"^{time1}$", "$options": "i"}}
                ]}
            ]
        }
        return list(self.collection.find(query))

    def buscar_partidas_por_time_or(self, time1, time2):
        query = {"$or": [{"homeTeam.name": time1}, {"awayTeam.name": time2}]}
        return list(self.collection.find(query))

    def buscar_partidas_por_rodadas(self, rodadas):

        query = {"rodada": {"$in": rodadas}}
        return list(self.collection.find(query))


    # Métodos de agregação utilizando funções diferentes

    def agregacao_total_gols_por_time(self):
        pipeline = [
            {"$group": {
                "_id": "$homeTeam.name",
                "total_gols": {"$sum": "$score.fullTime.home"}
            }}
        ]
        return list(self.collection.aggregate(pipeline))

    def agregacao_media_gols_por_time(self):

        pipeline = [
            {"$group": {
                "_id": "$homeTeam.name",
                "media_gols": {"$avg": "$score.fullTime.home"}
            }}
        ]
        return list(self.collection.aggregate(pipeline))

    def agregacao_max_gols_por_time(self):
        pipeline = [
            {"$group": {
                "_id": "$homeTeam.name",
                "max_gols": {"$max": "$score.fullTime.home"}
            }}
        ]
        return list(self.collection.aggregate(pipeline))

    def agregacao_min_gols_por_time(self):
        pipeline = [
            {"$group": {
                "_id": "$homeTeam.name",
                "min_gols": {"$min": "$score.fullTime.home"}
            }}
        ]
        return list(self.collection.aggregate(pipeline))

    def estatisticas_vitorias_derrotas_mandantes(self, time1, time2):
        query = {
            "$or": [
                {"$and": [
                    {"homeTeam.name": {"$regex": f"^{time1}$", "$options": "i"}},
                    {"awayTeam.name": {"$regex": f"^{time2}$", "$options": "i"}}
                ]},
                {"$and": [
                    {"homeTeam.name": {"$regex": f"^{time2}$", "$options": "i"}},
                    {"awayTeam.name": {"$regex": f"^{time1}$", "$options": "i"}}
                ]}
            ]
        }
        partidas = list(self.collection.find(query))
        
        stats = {
            time1: {"vitorias": 0, "derrotas": 0},
            time2: {"vitorias": 0, "derrotas": 0}
        }
        
        for partida in partidas:
            home = partida["homeTeam"]["name"]
            vencedor = partida.get("vencedor", "")
            
            # Se o time foi mandante, verifica se venceu ou perdeu
            if home.lower() == time1.lower():
                if vencedor.lower() == home.lower():
                    stats[time1]["vitorias"] += 1
                else:
                    stats[time1]["derrotas"] += 1
            elif home.lower() == time2.lower():
                if vencedor.lower() == home.lower():
                    stats[time2]["vitorias"] += 1
                else:
                    stats[time2]["derrotas"] += 1
                    
        return stats

    
    def fazer_backup(self, db_name="statistics_futebol", repo_name="brazileirao-data-analysis"):
        import json
        import os
        from datetime import datetime
        
        # Define o caminho base do backup
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../backup"))
        repo_dir = os.path.join(base_dir, repo_name)
        backup_dir = os.path.join(repo_dir, "backup")
        
        # Cria a estrutura de diretórios
        os.makedirs(backup_dir, exist_ok=True)
        
        # Nome do diretório específico para este backup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_timestamp_dir = os.path.join(backup_dir, f"{db_name}_backup_{timestamp}")
        os.makedirs(backup_timestamp_dir, exist_ok=True)
        
        # Para cada collection no banco
        for collection_name in self.db.list_collection_names():
            collection = self.db[collection_name]
            
            # Nome do arquivo de backup
            filename = f"{collection_name}.json"
            filepath = os.path.join(backup_timestamp_dir, filename)
            
            # Recupera todos os documentos
            documents = list(collection.find({}))
            
            # Remove o campo _id
            for doc in documents:
                doc.pop('_id', None)
            
            # Salva em arquivo JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(documents, f, ensure_ascii=False, indent=2)
            
            print(f"Backup da collection {collection_name} salvo em: {filepath}")
        
        print(f"\nBackup completo do banco {db_name} salvo em: {backup_timestamp_dir}")
        
        return backup_timestamp_dir
    
    def verificar_e_inserir_documentos(self, collection_name):

        dados_brasileirao = [
            {
                "rodada": 1,
                "data": "15/04/2024",
                "hora": "20:00",
                "homeTeam": {"name": "Santos", "estado": "SP"},
                "awayTeam": {"name": "Palmeiras", "estado": "SP"},
                "score": {"fullTime": {"home": 2, "away": 1}},
                "vencedor": "Santos"
            },
            {
                "rodada": 1,
                "data": "15/04/2024",
                "hora": "20:00", 
                "homeTeam": {"name": "São Paulo", "estado": "SP"},
                "awayTeam": {"name": "Flamengo", "estado": "RJ"},
                "score": {"fullTime": {"home": 1, "away": 1}},
                "vencedor": "-"
            },
            {
                "rodada": 1,
                "data": "15/04/2024",
                "hora": "20:00",
                "homeTeam": {"name": "Corinthians", "estado": "SP"},
                "awayTeam": {"name": "Cruzeiro", "estado": "MG"},
                "score": {"fullTime": {"home": 3, "away": 0}},
                "vencedor": "Corinthians"
            },
            {
                "rodada": 1,
                "data": "15/04/2024",
                "hora": "20:00",
                "homeTeam": {"name": "Grêmio", "estado": "RS"},
                "awayTeam": {"name": "Internacional", "estado": "RS"},
                "score": {"fullTime": {"home": 2, "away": 2}},
                "vencedor": "-"
            },
            {
                "rodada": 1,
                "data": "15/04/2024",
                "hora": "20:00",
                "homeTeam": {"name": "Athletico-PR", "estado": "PR"},
                "awayTeam": {"name": "Coritiba", "estado": "PR"},
                "score": {"fullTime": {"home": 1, "away": 0}},
                "vencedor": "Athletico-PR"
            }
        ]

        dados_odds = [
            {
                "time": "Santos",
                "season": 2024,
                "jogos": 1,
                "vitorias": 1,
                "empates": 0,
                "derrotas": 0,
                "odds": {"homeWin": 0.6, "draw": 0.2, "awayWin": 0.2}
            },
            {
                "time": "São Paulo",
                "season": 2024,
                "jogos": 1,
                "vitorias": 0,
                "empates": 1,
                "derrotas": 0,
                "odds": {"homeWin": 0.4, "draw": 0.3, "awayWin": 0.3}
            },
            {
                "time": "Corinthians",
                "season": 2024,
                "jogos": 1,
                "vitorias": 1,
                "empates": 0,
                "derrotas": 0,
                "odds": {"homeWin": 0.5, "draw": 0.3, "awayWin": 0.2}
            },
            {
                "time": "Grêmio",
                "season": 2024,
                "jogos": 1,
                "vitorias": 0,
                "empates": 1,
                "derrotas": 0,
                "odds": {"homeWin": 0.4, "draw": 0.4, "awayWin": 0.2}
            },
            {
                "time": "Athletico-PR",
                "season": 2024,
                "jogos": 1,
                "vitorias": 1,
                "empates": 0,
                "derrotas": 0,
                "odds": {"homeWin": 0.5, "draw": 0.3, "awayWin": 0.2}
            }
        ]

        dados = dados_brasileirao if collection_name == "brasileirao" else dados_odds
        documentos_novos = []
        documentos_existentes = 0

        for doc in dados:
            # Monta o critério de busca específico para cada coleção
            if collection_name == "brasileirao":
                criterio = {
                    "$and": [
                        {"rodada": doc["rodada"]},
                        {"data": doc["data"]},
                        {"homeTeam.name": doc["homeTeam"]["name"]},
                        {"awayTeam.name": doc["awayTeam"]["name"]}
                    ]
                }
            else:  # odds_times_aggregados
                criterio = {
                    "$and": [
                        {"time": doc["time"]},
                        {"season": doc["season"]}
                    ]
                }
            
            # Verifica se existe usando count_documents ao invés de find_one
            contagem = self.db[collection_name].count_documents(criterio)
            
            if contagem == 0:
                documentos_novos.append(doc)
            else:
                documentos_existentes += 1
                print(f"Documento já existe: {criterio}")

        if documentos_novos:
            try:
                self.db[collection_name].insert_many(documentos_novos)
                print(f"\nInseridos {len(documentos_novos)} novos documentos em {collection_name}")
            except Exception as e:
                print(f"Erro ao inserir documentos: {e}")
        
        resultado = {
            "novos": len(documentos_novos),
            "existentes": documentos_existentes,
            "total": len(dados)
        }
        
        print(f"\nResultados para {collection_name}:")
        print(f"- Documentos novos: {resultado['novos']}")
        print(f"- Documentos existentes: {resultado['existentes']}")
        print(f"- Total verificado: {resultado['total']}")
        
        return resultado
        
    
    def editar_documentos(self, collection_name):
        if collection_name == "brasileirao":
            self.db[collection_name].update_many(
                {"hora": "20:00"},
                {"$set": {"hora": "21:30"}}
            )
            
            self.db[collection_name].update_one(
                {"homeTeam.name": "Santos"},
                {"$set": {"score.fullTime.home": 3}}
            )
            

            self.db[collection_name].update_one(
                {"homeTeam.name": "Santos"},
                {"$set": {"vencedor": "Santos"}}
            )
            

            self.db[collection_name].update_many(
                {"rodada": 1},
                {"$set": {"rodada": 2}}
            )
            
    
            self.db[collection_name].update_many(
                {"data": "15/04/2024"},
                {"$set": {"data": "22/04/2024"}}
            )
            
        else:  
            
            self.db[collection_name].update_many(
                {"season": 2024},
                {"$set": {"odds.homeWin": 0.55}}
            )
            
            self.db[collection_name].update_many(
                {"season": 2024},
                {"$inc": {"jogos": 1}}
            )
            
            self.db[collection_name].update_many(
                {"season": 2024},
                {"$inc": {"vitorias": 1}}
            )
            
            self.db[collection_name].update_many(
                {"season": 2024},
                {"$set": {"empates": 0}}
            )
            
            self.db[collection_name].update_many(
                {"season": 2024},
                {"$set": {"derrotas": 0}}
            )
    
    def verificar_alteracoes_brasileirao(self):
        jogos_alterados_hora = list(self.db["brasileirao"].find({"hora": "21:30"}))
        print(f"\n1. Jogos com horário alterado para 21:30: {len(jogos_alterados_hora)}")
        for jogo in jogos_alterados_hora:
            print(f"- {jogo['homeTeam']['name']} vs {jogo['awayTeam']['name']}")
        
        # Verifica alterações do Santos
        jogos_santos = list(self.db["brasileirao"].find({"homeTeam.name": "Santos"}))
        print("\n2. Alterações nos jogos do Santos:")
        for jogo in jogos_santos:
            print(f"- Placar: {jogo['score']['fullTime']['home']} x {jogo['score']['fullTime']['away']}")
            print(f"- Vencedor: {jogo['vencedor']}")
        
        # Verifica jogos na rodada 2 (alterados da rodada 1)
        jogos_rodada2 = list(self.db["brasileirao"].find({"rodada": 2}))
        print(f"\n3. Jogos movidos para rodada 2: {len(jogos_rodada2)}")
        for jogo in jogos_rodada2:
            print(f"- {jogo['homeTeam']['name']} vs {jogo['awayTeam']['name']}")
        
        # Verifica jogos com nova data
        jogos_nova_data = list(self.db["brasileirao"].find({"data": "22/04/2024"}))
        print(f"\n4. Jogos com nova data (22/04/2024): {len(jogos_nova_data)}")
        for jogo in jogos_nova_data:
            print(f"- {jogo['homeTeam']['name']} vs {jogo['awayTeam']['name']}")

    def buscar_todos_documentos(self, collection_name):
        return list(self.db[collection_name].find())
    
    
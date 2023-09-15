from urllib.request import urlopen  
import json
import time
from azure.cosmos import CosmosClient
import pandas as pd
import csv
import sys
import subprocess
import calendar
from slugify import slugify
import uuid
from datetime import datetime,date, timedelta
import numpy as np
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd


fecha_muestra = datetime.now()
cad_fecha_muestra = fecha_muestra.strftime('%Y-%m-%d')

array_dict_campeonatos = [\
#{ "ChampName": "Europa / UEFA Champions League", "ChampId": 4584, "Sport":"Futbol"},\
#{ "ChampName": "Europa / UEFA Europa League", "ChampId": 4230, "Sport":"Futbol"},\
#{"ChampName": "Internacional / Amistosos", "ChampId": 3645, "Sport":"Futbol"},\
#{"ChampName": "America / Copa Libertadores", "ChampId": 102, "Sport":"Futbol"},\
#{"ChampName": "America / Copa Sudamericana", "ChampId": 389, "Sport":"Futbol"},\
#{"ChampName": "Peru / Liga 1", "ChampId": 583, "Sport":"Futbol"},\
{"ChampName": "España / La Liga", "ChampId": 11, "Sport":"Futbol"},\
#{"ChampName": "España / Copa del Rey", "ChampId": 2973, "Sport":"Futbol"},\
#{"ChampName": "Inglaterra / Premier League", "ChampId": 7, "Sport":"Futbol"},\
#{"ChampName": "Italia / Serie A", "ChampId": 17, "Sport":"Futbol"},\
#{"ChampName": "Francia / Ligue 1", "ChampId": 4610, "Sport":"Futbol"},\
#{"ChampName": "Alemania / Bundesliga", "ChampId": 25, "Sport":"Futbol"},\
#{"ChampName": "Primera División", "ChampId": 3152, "Sport":"Futbol"},\
#{"ChampName": "Ecuador / Liga Pro", "ChampId": 5062, "Sport":"Futbol"},\
#{"ChampName": "USA / MLS", "ChampId": 104, "Sport":"Futbol"},\
#{"ChampName": "Argentina / Copa Liga Profesional", "ChampId": 7214, "Sport":"Futbol"},\
#{"ChampName": "Argentina / Copa Argentina", "ChampId": 640, "Sport":"Futbol"},\
#{"ChampName": "Brasil / Brasileirao", "ChampId": 113, "Sport":"Futbol"},\
#{"ChampName": "Eliminatorias Conmebol", "ChampId": 613, "Sport":"Futbol"},\
#{"ChampName": "Eliminatorias Eurocopa", "ChampId": 6071, "Sport":"Futbol"},\
]

inicio = date.today()
cad_inicio = inicio.strftime('%Y-%m-%d')

def get_json(url_base):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')    
        text_content = soup.get_text()
        parsed_data = json.loads(text_content)
    else:
        parsed_data = ''
    
    return parsed_data
    
def get_info(url_base, eventos,max_game_id):
    parsed_data = get_json(url_base)
    flag_continue = 1
    try:
        games = parsed_data["games"]
        for data in games:        
            game_id = data.get('id', 'N/A')
            if game_id >max_game_id:
                max_game_id = game_id
            max_game_id = data.get('id', 'N/A')
            home_name = data['homeCompetitor'].get('name', 'N/A')
            away_name = data['awayCompetitor'].get('name', 'N/A')
            fecha = data.get('startTime', 'N/A')[:16]

            evento=({
                    "deporte": sport,
                    "liga": nombre_liga,
                    "local": home_name,
                    "visita": away_name,
                    "fecha_hora_evento": fecha+'Z'
                })
            eventos.append(evento)
    except:
        flag_continue = 0
        pass

    return flag_continue, eventos,max_game_id

eventos = []
for i in range(len(array_dict_campeonatos)):
    max_game_id = 0
    # Info del diccionario de ligas
    nombre_liga = array_dict_campeonatos[i]['ChampName']
    print(nombre_liga)
    code = array_dict_campeonatos[i]['ChampId']
    sport = array_dict_campeonatos[i]['Sport']
    
    #url de la liga
    url = 'https://webws.365scores.com/web/games/fixtures/?appTypeId=5&langId=29&timezoneName=America/Lima&userCountryId=146&competitions={id}&showOdds=true&includeTopBettingOpportunity=1'.format(id= code)
    print(url)
    
    #info
    flag_continue, eventos, max_game_id = get_info(url, eventos, max_game_id)
    while flag_continue:
        url = 'https://webws.365scores.com/web/games/?langId=29&timezoneId=74&userCountryId=146&competitions={id}&games=1&aftergame={max_id}&direction=1'.format(id= code, max_id=max_game_id)
        print(url)
        flag_continue, eventos, max_game_id = get_info(url, eventos, max_game_id)
    

df = pd.DataFrame(eventos)
print(df.head(10))


if len(df)>1:
    df['cod_semana']=df['fecha_hora_evento'].apply(lambda x: str(datetime.strptime(x[:10], '%Y-%m-%d').isocalendar()[0]) + '-'+str(datetime.strptime(x[:10], '%Y-%m-%d').isocalendar()[1]))
    df["cod_semana"]=df["cod_semana"].values.astype('str')
    df['cod_mes']=df['fecha_hora_evento'].apply(lambda x: int(str(x[:4])  + str(x[5:7])))
    print(np.unique(df['liga']))
    print(df.tail(10))

    df.to_csv('indice_de_contenido_365.csv', encoding='utf-8', sep='\t', index=False)
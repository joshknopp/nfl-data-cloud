from uuid import uuid4

from nitric.resources import api, collection, bucket
from nitric.application import Nitric
from nitric.faas import HttpContext

import requests
import requests_cache
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json

requests_cache.install_cache('nfl_scores_cache', expire_after=60)

def parse_games_from_html(text) -> pd.DataFrame:
    soup = BeautifulSoup(text, 'html.parser')
    
    weeks = soup.find_all('div', class_='ltbluediv')

    all_games_data = []

    for week_div in weeks:
        week_number = int(week_div.find('span', class_='divheader').text.strip().replace('Week ', ''))
        week_table = week_div.find_next('table', class_='statistics')
        
        if week_table:
            for row in week_table.find_all('tr', class_='row0') + week_table.find_all('tr', class_='row1'):
                date_string = row.find('td').find('span', class_='hidden-xs').text.strip()
                date = datetime.strptime(date_string, '%m/%d/%Y')
                away_team = row.find_all('span', class_='visible-xs-inline')[1].text.strip()
                home_team = row.find_all('span', class_='visible-xs-inline')[2].text.strip()
                away_score = int(row.find_all('td', class_='center')[0].text.strip())
                home_score = int(row.find_all('td', class_='center')[1].text.strip())

                game_data = {
                    'week': week_number,
                    'date': date,
                    'awayTeam': away_team,
                    'awayScore': away_score,
                    'homeTeam': home_team,
                    'homeScore': home_score
                }

                all_games_data.append(game_data)
    
    return pd.DataFrame(all_games_data)

def fetch_scores_from_web() -> pd.DataFrame:
    scores_url = 'https://www.footballdb.com/games/index.html'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(scores_url, headers=headers)
    response.raise_for_status()
    return parse_games_from_html(response.text)

# Create an api named public
scores_api = api("public")

@scores_api.get("/scores")
async def get_scores(ctx: HttpContext) -> None:
  df = fetch_scores_from_web()
  # Pandas datetime objects are not JSON-friendly
  df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
  data_records = df.to_dict(orient='records')
  json_array = json.dumps(data_records)
  ctx.res.headers['Content-Type'] = 'application/json'
  ctx.res.body = json_array
  
Nitric.run()
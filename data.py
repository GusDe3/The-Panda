import os
from dotenv import load_dotenv
import requests
import gspread
from google.oauth2 import service_account
import logging
import time
from datetime import datetime, timedelta
from dateutil.parser import parse
from collections import Counter

load_dotenv()
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your actual values
BS_TOKEN = os.getenv('B')   # Remplacez par votre token Brawl Stars
SHEET_ID = os.getenv('G')   # Remplacez par l'ID de votre Google Sheet
CREDENTIALS_FILE = 'credentials.json'  # Chemin vers votre fichier de credentials

# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
try:
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    gs_client = gspread.authorize(creds)
    sheet = gs_client.open_by_key(SHEET_ID)
    players_worksheet = sheet.worksheet('Players')  # Column A has player tags like #ABC123
    matches_worksheet = sheet.worksheet('Matches')  # Headers: PlayerTag, BattleTime, EventMode, EventMap, BrawlerName, Result, TrophyChange
    logging.info("Successfully initialized Google Sheets client.")
except Exception as e:
    logging.error(f"Failed to initialize Google Sheets client: {e}")
    raise

def update_sheet():
    try:
        players = [row[0] for row in players_worksheet.get_all_values()[1:] if row[0].strip()]  # Filtrer les tags vides
        logging.info(f"Found {len(players)} valid players to process: {players}")
        existing_matches = matches_worksheet.get_all_records()
        
        headers = {'Authorization': f'Bearer {BS_TOKEN}'}
        
        for player in players:
            tag = player.strip().lstrip('#').upper()  # Clean spaces and #
            url = f'https://api.brawlstars.com/v1/players/%23{tag}/battlelog'
            logging.info(f"Attempting to fetch battlelog for player: {player} (tag: #{tag})")
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                battles = response.json()['items']
                logging.info(f"Successfully fetched {len(battles)} battles for {player}")
                
                for battle in battles:
                    battle_time = battle['battleTime']
                    if any(m['PlayerTag'] == player and m['BattleTime'] == battle_time for m in existing_matches):
                        continue
                    
                    event = battle['event']
                    event_mode = event.get('mode', '')
                    event_map = event.get('map', None)
                    if event_map is None:
                        continue
                    
                    battle_data = battle['battle']
                    brawler_name = None
                    
                    if 'teams' in battle_data:
                        for team in battle_data['teams']:
                            for p in team:
                                if p['tag'] == player:
                                    brawler_name = p['brawler']['name'].upper()
                                    break
                            if brawler_name:
                                break
                    elif 'players' in battle_data:
                        for p in battle_data['players']:
                            if p['tag'] == player:
                                brawler_name = p['brawler']['name'].upper()
                                break
                    
                    if not brawler_name:
                        continue
                    
                    result = battle_data.get('result', '')
                    trophy_change = battle.get('trophyChange', 0)
                    
                    if trophy_change != 0:
                        logging.info(f"Skipped non-friendly match for {player} at {battle_time} (trophyChange = {trophy_change})")
                        continue
                    
                    row = [player, battle_time, event_mode, event_map, brawler_name, result, str(trophy_change)]
                    matches_worksheet.append_row(row)
                    logging.info(f'Added new friendly match for {player} at {battle_time}')
                    time.sleep(1)  # Pause de 1 seconde pour respecter la limite de 60 écritures/minute
            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP error for {player}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {player}: {e}")
                if '429' in str(e):
                    logging.warning("Quota limit reached, waiting 60 seconds...")
                    time.sleep(60)  # Attendre 60 secondes avant de continuer
    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")

if __name__ == "__main__":
    logging.info("Starting data.py execution...")
    update_sheet()
    logging.info("Data update completed.")
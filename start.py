import os
import logging
import threading
import requests
import time
import asyncio  # Import explicite pour éviter tout doute
from dotenv import load_dotenv
import gspread
from google.oauth2 import service_account

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Log initial
print("Starting start.py - Debug check")
logging.info("Starting start.py script")

# Configuration pour Google Sheets
CREDENTIALS_FILE = 'credentials.json'
SHEET_ID = os.getenv('G')
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Initialisation différée de Google Sheets
def init_sheets():
    try:
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        gs_client = gspread.authorize(creds)
        sheet = gs_client.open_by_key(SHEET_ID)
        players_worksheet = sheet.worksheet('Players')
        matches_worksheet = sheet.worksheet('Matches')
        logging.info("Successfully initialized Google Sheets client.")
        return players_worksheet, matches_worksheet
    except Exception as e:
        logging.error(f"Failed to initialize Google Sheets: {e}")
        raise

# Fonction de mise à jour
def update_sheet():
    players_worksheet, matches_worksheet = init_sheets()
    try:
        players = [row[0] for row in players_worksheet.get_all_values()[1:] if row[0].strip()]
        logging.info(f"Found {len(players)} valid players to process: {players}")
        existing_matches = matches_worksheet.get_all_records()
        BS_TOKEN = os.getenv('B')
        headers = {'Authorization': f'Bearer {BS_TOKEN}'}

        for player in players:
            tag = player.strip().lstrip('#').upper()
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
                    time.sleep(1)  # Pause pour respecter la limite de 60 écritures/minute
            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP error for {player}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {player}: {e}")
                if '429' in str(e):
                    logging.warning("Quota limit reached, waiting 60 seconds...")
                    time.sleep(60)
    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")
    finally:
        logging.info("Data update thread completed.")

# Lancer la mise à jour dans un thread
def run_data_update():
    logging.info("Starting data update thread...")
    try:
        update_sheet()
    except Exception as e:
        logging.error(f"Error in data update thread: {e}")

# Fonction pour démarrer le bot
def start_bot():
    logging.info("Starting bot1.py...")
    try:
        import bot1
        loop = asyncio.new_event_loop()  # Nouvelle boucle pour le bot
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot1.bot.start(os.getenv('D')))
    except Exception as e:
        logging.error(f"Error in bot startup: {e}")
    finally:
        if 'loop' in locals():
            loop.close()

def main():
    # Lancer la mise à jour des données dans un thread
    data_thread = threading.Thread(target=run_data_update)
    data_thread.start()
    
    # Lancer le bot dans un thread séparé
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.start()
    
    # Attendre la fin des threads
    data_thread.join()
    bot_thread.join()

if __name__ == "__main__":
    main()
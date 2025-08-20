import os
import logging
import threading
import time
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2 import service_account
import requests  # Conservé pour les appels à l'API Brawl Stars

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
        # Récupérer toutes les lignes existantes
        all_matches = matches_worksheet.get_all_records()
        current_time = datetime.utcnow()
        logging.info(f"Current time (UTC): {current_time}")
        thirty_days_ago = current_time - timedelta(days=30)
        logging.info(f"Thirty days ago (UTC): {thirty_days_ago}")

        # Identifier les lignes à conserver (moins de 30 jours) et à supprimer
        valid_matches = []
        rows_to_delete = []
        write_count = 0  # Compteur de requêtes d'écriture
        for i, match in enumerate(all_matches, start=2):  # Commence à 2 pour ignorer l'en-tête
            battle_time = datetime.strptime(match['BattleTime'], '%Y%m%dT%H%M%S.000Z')
            logging.info(f"Checking BattleTime: {match['BattleTime']} (parsed: {battle_time})")
            if battle_time < thirty_days_ago:  # Seulement supprimer si plus vieux que 30 jours
                rows_to_delete.append(i)
                logging.info(f"Marked for deletion: {match['BattleTime']}")
            else:
                valid_matches.append([match['PlayerTag'], match['BattleTime'], match['EventMode'], match['EventMap'], match['BrawlerName'], match['Result'], match['Trophy Change']])
                logging.info(f"Kept: {match['BattleTime']}")

        # Supprimer les anciennes lignes en lots si possible
        if rows_to_delete:
            logging.info(f"Deleting {len(rows_to_delete)} old entries...")
            for row in sorted(rows_to_delete, reverse=True):
                matches_worksheet.delete_rows(row, row)
                write_count += 1
                if write_count % 60 == 0:  # Limite de 60 écritures par minute
                    logging.info("Approaching write limit, waiting 60 seconds...")
                    time.sleep(60)
            logging.info(f"Deleted old entries successfully.")

        # Réécrire les matchs valides en une seule requête
        if valid_matches:
            matches_worksheet.clear()  # 1 écriture
            write_count += 1
            matches_worksheet.append_row(['PlayerTag', 'BattleTime', 'EventMode', 'EventMap', 'BrawlerName', 'Result', 'Trophy Change'])  # 1 écriture
            write_count += 1
            if valid_matches:
                matches_worksheet.update('A2', valid_matches)  # Écrire toutes les lignes en une seule requête
                write_count += 1
            logging.info(f"Reorganized {len(valid_matches)} valid entries.")

        # Ajouter les nouveaux matchs
        players = [row[0] for row in players_worksheet.get_all_values()[1:] if row[0].strip()]
        logging.info(f"Found {len(players)} valid players to process: {players}")
        existing_matches = matches_worksheet.get_all_records()
        BS_TOKEN = os.getenv('B')
        headers = {'Authorization': f'Bearer {BS_TOKEN}'}

        new_matches = []
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
                    new_matches.append([player, battle_time, event_mode, event_map, brawler_name, result, str(trophy_change)])

        # Ajouter les nouveaux matchs par lots
        if new_matches:
            batch_size = 50  # Ajuster selon le nombre d'écritures restantes
            for i in range(0, len(new_matches), batch_size):
                batch = new_matches[i:i + batch_size]
                matches_worksheet.append_rows(batch)
                write_count += 1
                logging.info(f'Added {len(batch)} new friendly matches')
                if write_count % 60 == 0:  # Limite de 60 écritures par minute
                    logging.info("Approaching write limit, waiting 60 seconds...")
                    time.sleep(60)

        if not new_matches and not rows_to_delete:
            logging.info("No new matches added and no old entries deleted.")

    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")
    finally:
        logging.info("Data update thread completed.")

# Lancer la mise à jour dans un thread avec boucle
def run_data_update():
    logging.info("Starting data update thread with hourly loop...")
    last_run = 0
    while True:
        current_time = time.time()
        if current_time - last_run >= 3600:  # 1 heure (3600 secondes)
            logging.info("Starting data update cycle...")
            update_sheet()
            last_run = current_time
            logging.info("Waiting 1 hour before next update...")
        time.sleep(300)  # Vérifier toutes les 5 minutes (aligné avec ton ping)

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
    data_thread = threading.Thread(target=run_data_update, daemon=True)
    data_thread.start()
    
    # Lancer le bot dans un thread séparé
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    
    # Laisser les threads tourner en arrière-plan
    while True:
        time.sleep(30)  # Boucle principale pour garder le script actif

if __name__ == "__main__":
    main()

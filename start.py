import os
import logging
import threading
import time
import asyncio
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
import gspread
from google.oauth2 import service_account
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
print("Starting start.py - Debug check")
logging.info("Starting start.py script")

CREDENTIALS_FILE = 'credentials.json'
SHEET_ID = os.getenv('G')
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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

def update_sheet():
    players_worksheet, matches_worksheet = init_sheets()
    try:
        # Récupérer toutes les données existantes
        all_data = matches_worksheet.get_all_values()
        if not all_data or len(all_data) <= 1:  # Seulement l'en-tête ou vide
            all_matches = []
            headers = ['PlayerTag', 'BattleTime', 'EventMode', 'EventMap', 'BrawlerName', 'Result', 'Trophy Change']
            if all_data and all_data[0] != headers:
                matches_worksheet.update('A1', [headers])
        else:
            headers = all_data[0]
            all_matches = [dict(zip(headers, row)) for row in all_data[1:]]
        
        current_time = datetime.now(UTC)
        logging.info(f"Current time (UTC): {current_time}")
        thirty_days_ago = current_time - timedelta(days=30)
        logging.info(f"Thirty days ago (UTC): {thirty_days_ago}")

        # Préparer les nouvelles données
        new_data = [headers]  # Commencer avec les en-têtes
        rows_to_keep = 0
        rows_to_delete = 0
        
        # Filtrer les entrées pour garder seulement celles de moins de 30 jours
        for match in all_matches:
            try:
                if 'BattleTime' not in match:
                    continue
                    
                battle_time_str = match['BattleTime']
                # Parser la date
                try:
                    battle_time = datetime.strptime(battle_time_str, '%Y%m%dT%H%M%S.%fZ').replace(tzinfo=UTC)
                except ValueError:
                    try:
                        battle_time = datetime.strptime(battle_time_str, '%Y%m%dT%H%M%SZ').replace(tzinfo=UTC)
                    except ValueError:
                        logging.error(f"Could not parse BattleTime: {battle_time_str}")
                        continue
                
                # Garder seulement les entrées de moins de 30 jours
                if battle_time >= thirty_days_ago:
                    new_data.append([
                        match.get('PlayerTag', ''),
                        match.get('BattleTime', ''),
                        match.get('EventMode', ''),
                        match.get('EventMap', ''),
                        match.get('BrawlerName', ''),
                        match.get('Result', ''),
                        match.get('Trophy Change', '')
                    ])
                    rows_to_keep += 1
                else:
                    rows_to_delete += 1
                    
            except Exception as e:
                logging.error(f"Error processing match: {e}")
                continue

        logging.info(f"Keeping {rows_to_keep} entries, deleting {rows_to_delete} old entries")

        # Récupérer les nouveaux matchs des joueurs
        players = [row[0] for row in players_worksheet.get_all_values()[1:] if row and row[0].strip()]
        logging.info(f"Found {len(players)} valid players to process: {players}")
        
        BS_TOKEN = os.getenv('B')
        headers_api = {'Authorization': f'Bearer {BS_TOKEN}'}
        
        # Créer un set des BattleTime existants pour éviter les doublons
        existing_battle_times = {match['BattleTime'] for match in all_matches if 'BattleTime' in match}
        
        new_matches_count = 0
        for player in players:
            tag = player.strip().lstrip('#').upper()
            url = f'https://api.brawlstars.com/v1/players/%23{tag}/battlelog'
            logging.info(f"Fetching battlelog for player: {player} (tag: #{tag})")
            
            try:
                response = requests.get(url, headers=headers_api)
                response.raise_for_status()
                battles = response.json()['items']
                logging.info(f"Successfully fetched {len(battles)} battles for {player}")

                for battle in battles:
                    battle_time = battle['battleTime']
                    
                    # Vérifier si ce match existe déjà
                    if battle_time in existing_battle_times:
                        continue
                    
                    event = battle['event']
                    event_mode = event.get('mode', '')
                    event_map = event.get('map', None)
                    if event_map is None:
                        continue
                    
                    battle_data = battle['battle']
                    brawler_name = None
                    
                    # Trouver le brawler utilisé par le joueur
                    if 'teams' in battle_data:
                        for team in battle_data['teams']:
                            for p in team:
                                if p['tag'] == f"#{tag}":
                                    brawler_name = p['brawler']['name'].upper()
                                    break
                            if brawler_name:
                                break
                    elif 'players' in battle_data:
                        for p in battle_data['players']:
                            if p['tag'] == f"#{tag}":
                                brawler_name = p['brawler']['name'].upper()
                                break
                    
                    if not brawler_name:
                        continue
                    
                    result = battle_data.get('result', '')
                    trophy_change = battle.get('trophyChange', 0)
                    
                    # Ne traiter que les matchs amicaux (sans changement de trophées)
                    if trophy_change != 0:
                        continue
                    
                    # Ajouter le nouveau match
                    new_data.append([
                        f"#{tag}",
                        battle_time,
                        event_mode,
                        event_map,
                        brawler_name,
                        result,
                        str(trophy_change)
                    ])
                    
                    existing_battle_times.add(battle_time)
                    new_matches_count += 1
                    logging.info(f"Added new match for {player} at {battle_time}")

            except Exception as e:
                logging.error(f"Error fetching battlelog for player {player}: {e}")
                continue

        logging.info(f"Added {new_matches_count} new matches")

        # Mettre à jour la feuille avec les nouvelles données
        if len(new_data) > 1:  # S'il y a des données à part l'en-tête
            # Effacer et remplir la feuille
            matches_worksheet.clear()
            matches_worksheet.update('A1', new_data)
            logging.info(f"Sheet updated with {len(new_data)-1} matches")
        else:
            # Seulement l'en-tête, ajouter une ligne vide pour éviter les erreurs
            matches_worksheet.update('A1', [headers])
            logging.info("No matches to display, sheet cleared except headers")

    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")
    finally:
        logging.info("Data update thread completed.")

def run_data_update():
    logging.info("Starting data update thread with daily cleanup...")
    while True:
        try:
            # Exécuter la mise à jour une fois par jour
            logging.info("Starting daily data update and cleanup...")
            update_sheet()
            logging.info("Daily update completed. Waiting 24 hours for next update...")
            time.sleep(24 * 3600)  # Attendre 24 heures
        except Exception as e:
            logging.error(f"Error in data update thread: {e}")
            time.sleep(3600)  # Attendre 1 heure en cas d'erreur

def start_bot():
    logging.info("Starting bot1.py...")
    try:
        import bot1
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot1.bot.start(os.getenv('D')))
    except Exception as e:
        logging.error(f"Error in bot startup: {e}")
    finally:
        if 'loop' in locals():
            loop.close()

def main():
    # Démarrer le thread de mise à jour des données
    data_thread = threading.Thread(target=run_data_update, daemon=True)
    data_thread.start()
    
    # Démarrer le bot
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    
    # Maintenir le programme en vie
    while True:
        time.sleep(30)

if __name__ == "__main__":
    main()

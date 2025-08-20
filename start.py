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

# Verrou pour synchroniser l'accès au tableur
sheet_lock = threading.Lock()

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

def daily_cleanup():
    """Nettoie les entrées de plus de 30 jours une fois par jour"""
    with sheet_lock:
        players_worksheet, matches_worksheet = init_sheets()
        try:
            # Récupérer toutes les données existantes
            all_data = matches_worksheet.get_all_values()
            if not all_data or len(all_data) <= 1:
                logging.info("No data to clean up")
                return
            
            headers = all_data[0]
            all_matches = [dict(zip(headers, row)) for row in all_data[1:]]
            
            current_time = datetime.now(UTC)
            thirty_days_ago = current_time - timedelta(days=30)
            logging.info(f"Starting daily cleanup. Current time: {current_time}, 30 days ago: {thirty_days_ago}")

            # Filtrer les entrées pour garder seulement celles de moins de 30 jours
            new_data = [headers]
            rows_kept = 0
            rows_deleted = 0
            
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
                        rows_kept += 1
                    else:
                        rows_deleted += 1
                        
                except Exception as e:
                    logging.error(f"Error processing match: {e}")
                    continue

            logging.info(f"Daily cleanup: Keeping {rows_kept} entries, deleting {rows_deleted} old entries")

            # Mettre à jour la feuille avec les données filtrées
            matches_worksheet.clear()
            matches_worksheet.update('A1', new_data)
            logging.info("Daily cleanup completed successfully")

        except Exception as e:
            logging.error(f"Error in daily cleanup: {e}")

def update_new_matches():
    """Ajoute de nouveaux matchs toutes les 30 minutes"""
    with sheet_lock:
        players_worksheet, matches_worksheet = init_sheets()
        try:
            # Récupérer les BattleTime existants pour éviter les doublons
            existing_data = matches_worksheet.get_all_values()
            existing_battle_times = set()
            
            if len(existing_data) > 1:
                headers = existing_data[0]
                for row in existing_data[1:]:
                    if len(row) > 1:  # Vérifier que la ligne a au moins 2 colonnes
                        existing_battle_times.add(row[1])  # BattleTime est en colonne B (index 1)
            
            # Récupérer les joueurs à suivre
            players = [row[0] for row in players_worksheet.get_all_values()[1:] if row and row[0].strip()]
            logging.info(f"Found {len(players)} players to check for new matches")
            
            if not players:
                logging.info("No players found to check for new matches")
                return
            
            BS_TOKEN = os.getenv('B')
            headers_api = {'Authorization': f'Bearer {BS_TOKEN}'}
            
            new_matches = []
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
                        new_matches.append([
                            f"#{tag}",
                            battle_time,
                            event_mode,
                            event_map,
                            brawler_name,
                            result,
                            str(trophy_change)
                        ])
                        
                        existing_battle_times.add(battle_time)
                        logging.info(f"New match found for {player} at {battle_time}")

                except Exception as e:
                    logging.error(f"Error fetching battlelog for player {player}: {e}")
                    continue

            # Ajouter les nouveaux matchs au tableur
            if new_matches:
                matches_worksheet.append_rows(new_matches)
                logging.info(f"Added {len(new_matches)} new matches to the sheet")
            else:
                logging.info("No new matches found")

        except Exception as e:
            logging.error(f"Error in update_new_matches: {e}")

def run_daily_cleanup():
    """Exécute le nettoyage quotidien une fois par jour"""
    logging.info("Starting daily cleanup thread")
    while True:
        try:
            # Exécuter le nettoyage à minuit chaque jour
            now = datetime.now()
            next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()
            
            logging.info(f"Next cleanup scheduled at {next_run}, waiting {wait_seconds/3600:.2f} hours")
            time.sleep(wait_seconds)
            
            daily_cleanup()
        except Exception as e:
            logging.error(f"Error in daily cleanup thread: {e}")
            time.sleep(3600)  # Attendre 1 heure en cas d'erreur

def run_match_updates():
    """Exécute la mise à jour des matchs toutes les 30 minutes"""
    logging.info("Starting match update thread (every 30 minutes)")
    while True:
        try:
            update_new_matches()
            logging.info("Match update completed. Waiting 30 minutes for next update...")
            time.sleep(30 * 60)  # Attendre 30 minutes
        except Exception as e:
            logging.error(f"Error in match update thread: {e}")
            time.sleep(300)  # Attendre 5 minutes en cas d'erreur

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
    # Démarrer le thread de nettoyage quotidien
    cleanup_thread = threading.Thread(target=run_daily_cleanup, daemon=True)
    cleanup_thread.start()
    
    # Démarrer le thread de mise à jour des matchs
    match_thread = threading.Thread(target=run_match_updates, daemon=True)
    match_thread.start()
    
    # Démarrer le bot
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    
    # Maintenir le programme en vie
    while True:
        time.sleep(30)

if __name__ == "__main__":
    main()

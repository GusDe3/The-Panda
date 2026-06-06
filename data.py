import os
from dotenv import load_dotenv
import requests
import gspread
from google.oauth2 import service_account
import logging
import time
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from collections import Counter
 
load_dotenv()
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
# Replace with your actual values
BS_TOKEN = os.getenv('B')   # Token Brawl Stars
SHEET_ID = os.getenv('G')   # ID de la Google Sheet
CREDENTIALS_FILE = 'credentials.json'
 
# Types de parties consideres comme LADDER (a NE PAS enregistrer).
# /!\ Dans l'API Brawl Stars, "ranked" = LADDER (trophees).
#     Le mode "Ranked" du jeu = "soloRanked" / "teamRanked".
LADDER_BATTLE_TYPES = {'ranked'}
 
# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
try:
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    gs_client = gspread.authorize(creds)
    sheet = gs_client.open_by_key(SHEET_ID)
    players_worksheet = sheet.worksheet('Players')  # Column A: tags genre #ABC123
    matches_worksheet = sheet.worksheet('Matches')  # Headers: PlayerTag, BattleTime, EventMode, EventMap, BrawlerName, Result, TrophyChange, BattleType
    logging.info("Successfully initialized Google Sheets client.")
except Exception as e:
    logging.error(f"Failed to initialize Google Sheets client: {e}")
    raise
 
def update_sheet():
    try:
        players = [row[0] for row in players_worksheet.get_all_values()[1:] if row[0].strip()]
        logging.info(f"Found {len(players)} valid players to process: {players}")
 
        existing_matches = matches_worksheet.get_all_records()
        # Recherche O(1) des doublons (PlayerTag + BattleTime)
        existing_keys = {(m.get('PlayerTag'), m.get('BattleTime')) for m in existing_matches}
 
        headers = {'Authorization': f'Bearer {BS_TOKEN}'}
 
        for player in players:
            tag = player.strip().lstrip('#').upper()
            url = f'https://api.brawlstars.com/v1/players/%23{tag}/battlelog'
            logging.info(f"Fetching battlelog for {player} (tag: #{tag})")
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                battles = response.json()['items']
                logging.info(f"Fetched {len(battles)} battles for {player}")
 
                for battle in battles:
                    battle_time = battle['battleTime']
                    if (player, battle_time) in existing_keys:
                        continue
 
                    event = battle['event']
                    event_mode = event.get('mode', '')
                    event_map = event.get('map', None)
                    if event_map is None:
                        continue
 
                    battle_data = battle['battle']
 
                    # --- Type de partie + trophees (CORRIGE: lus dans battle_data) ---
                    battle_type = battle_data.get('type', '')
                    trophy_change = battle_data.get('trophyChange', 0)
 
                    # --- FILTRE LADDER fiable -------------------------------------
                    # On saute le ladder : type "ranked" (3v3) OU toute partie a
                    # trophees (showdown ladder inclus). Le mode Ranked du jeu
                    # (soloRanked/teamRanked) et les amicaux n'ont pas de trophyChange.
                    if battle_type.lower() in LADDER_BATTLE_TYPES or trophy_change != 0:
                        logging.info(f"Skip LADDER {player} {battle_time} (type={battle_type}, trophy={trophy_change})")
                        continue
 
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
 
                    row = [player, battle_time, event_mode, event_map,
                           brawler_name, result, str(trophy_change), battle_type]
                    matches_worksheet.append_row(row)
                    existing_keys.add((player, battle_time))
                    logging.info(f"Added {battle_type or 'unknown'} match for {player} at {battle_time}")
                    time.sleep(1)  # respect de la limite ~60 ecritures/minute
            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP error for {player}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {player}: {e}")
                if '429' in str(e):
                    logging.warning("Quota limit reached, waiting 60 seconds...")
                    time.sleep(60)
    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")
 
def prune_old_matches(days=40):
    """
    Supprime les lignes de Matches dont BattleTime est plus vieux que `days` jours.
    Hypothese : les lignes sont ajoutees chronologiquement (append_row), donc les
    vieilles forment un bloc contigu en haut de la feuille -- on les supprime en
    un seul appel API par plage contigue, ce qui menage le quota.
    """
    logging.info(f"Pruning matches older than {days} days...")
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        rows = matches_worksheet.get_all_values()
        if len(rows) < 2:
            logging.info("Nothing to prune (feuille vide ou en-tete seule).")
            return
 
        BATTLE_TIME_COL = 1  # colonne B (0-indexed)
        old_indices = []  # indices 1-based dans la feuille
        for i, row in enumerate(rows[1:], start=2):
            if len(row) <= BATTLE_TIME_COL:
                continue
            bt_str = row[BATTLE_TIME_COL].strip()
            if not bt_str:
                continue
            try:
                bt = parse(bt_str)
                if bt.tzinfo is None:
                    bt = bt.replace(tzinfo=timezone.utc)
                if bt < cutoff:
                    old_indices.append(i)
            except Exception:
                continue
 
        if not old_indices:
            logging.info("Aucune ligne plus vieille que la limite.")
            return
 
        # Regroupe les indices consecutifs en plages
        ranges = []
        start = prev = old_indices[0]
        for idx in old_indices[1:]:
            if idx == prev + 1:
                prev = idx
            else:
                ranges.append((start, prev))
                start = prev = idx
        ranges.append((start, prev))
 
        # Supprime du bas vers le haut pour ne pas decaler les indices
        deleted = 0
        for s, e in reversed(ranges):
            matches_worksheet.delete_rows(s, e)
            deleted += (e - s + 1)
            logging.info(f"Deleted rows {s}-{e}")
            time.sleep(1)  # menage le quota API
        logging.info(f"Pruned {deleted} rows older than {days} days.")
    except Exception as e:
        logging.error(f"Error in prune_old_matches: {e}")
 
 
if __name__ == "__main__":
    logging.info("Starting data.py execution...")
    prune_old_matches(days=40)  # on purge AVANT d'ajouter pour rester sous la limite
    update_sheet()
    logging.info("Data update completed.")

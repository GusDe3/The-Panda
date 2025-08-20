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
        all_matches = matches_worksheet.get_all_records()
        current_time = datetime.now(UTC)
        logging.info(f"Current time (UTC): {current_time}")
        thirty_days_ago = current_time - timedelta(days=30)
        logging.info(f"Thirty days ago (UTC): {thirty_days_ago}")

        valid_matches = []
        rows_to_delete = []
        write_count = 0
        for i, match in enumerate(all_matches, start=2):
            # Correction: Ajouter le fuseau horaire UTC à la date parsée
            battle_time = datetime.strptime(match['BattleTime'], '%Y%m%dT%H%M%S.000Z').replace(tzinfo=UTC)
            logging.info(f"Checking BattleTime: {match['BattleTime']} (parsed: {battle_time})")
            if battle_time < thirty_days_ago:
                rows_to_delete.append(i)
                logging.info(f"Marked for deletion: {match['BattleTime']}")
            else:
                valid_matches.append([match['PlayerTag'], match['BattleTime'], match['EventMode'], match['EventMap'], match['BrawlerName'], match['Result'], match['Trophy Change']])
                logging.info(f"Kept: {match['BattleTime']}")

        if rows_to_delete:
            logging.info(f"Deleting {len(rows_to_delete)} old entries...")
            for row in sorted(rows_to_delete, reverse=True):
                matches_worksheet.delete_rows(row, row)
                write_count += 1
                if write_count % 60 == 0:
                    logging.info("Approaching write limit, waiting 60 seconds...")
                    time.sleep(60)
            logging.info(f"Deleted old entries successfully.")

        if valid_matches:
            matches_worksheet.clear()
            write_count += 1
            matches_worksheet.append_row(['PlayerTag', 'BattleTime', 'EventMode', 'EventMap', 'BrawlerName', 'Result', 'Trophy Change'])
            write_count += 1
            if valid_matches:
                matches_worksheet.update('A2', valid_matches)
                write_count += 1
            logging.info(f"Reorganized {len(valid_matches)} valid entries.")

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

            except Exception as e:
                logging.error(f"Error fetching battlelog for player {player}: {e}")
                continue

        if new_matches:
            batch_size = 50
            for i in range(0, len(new_matches), batch_size):
                batch = new_matches[i:i + batch_size]
                matches_worksheet.append_rows(batch)
                write_count += 1
                logging.info(f'Added {len(batch)} new friendly matches')
                if write_count % 60 == 0:
                    logging.info("Approaching write limit, waiting 60 seconds...")
                    time.sleep(60)

        if not new_matches and not rows_to_delete:
            logging.info("No new matches added and no old entries deleted.")

    except Exception as e:
        logging.error(f"Error in update_sheet: {e}")
    finally:
        logging.info("Data update thread completed.")

def run_data_update():
    logging.info("Starting data update thread with hourly loop...")
    last_run = 0
    while True:
        current_time = time.time()
        if current_time - last_run >= 1800:
            logging.info("Starting data update cycle...")
            update_sheet()
            last_run = current_time
            logging.info("Waiting 1 hour before next update...")
        time.sleep(300)

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
    data_thread = threading.Thread(target=run_data_update, daemon=True)
    data_thread.start()
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    while True:
        time.sleep(30)

if __name__ == "__main__":
    main()


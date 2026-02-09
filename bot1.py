import os
from dotenv import load_dotenv
import discord
from discord.ext import commands
import gspread
from google.oauth2 import service_account
import datetime
from datetime import timedelta
from dateutil.parser import parse
from collections import Counter
import logging
from keep_alive import keep_alive
# import aiohttp  # Uncomment if adding Brawl API

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your actual values
DISCORD_TOKEN = os.getenv('D') # Remplacez par votre token Discord
SHEET_ID = os.getenv('G') # Remplacez par l'ID de votre Google Sheet
CREDENTIALS_FILE = 'credentials.json' # Chemin vers votre fichier de credentials
YOUR_CHANNEL_ID = 1403782456108388404 # Remplacez par l'ID du canal obtenu avec Mode développeur
BOT_PREFIX = "/"  # Configurable prefix set to '/'

# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
@@ -209,50 +115,10 @@ async def command_compare(ctx, id1: str, id2: str, id3: str, *, map_name: str):
        logging.error(f"Error in /compare: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")

@bot.command(name='counters')
async def command_counters(ctx, id1: str, id2: str, id3: str):
    logging.info(f"Command /counters received from {ctx.author.name} with args: {id1}, {id2}, {id3}")
    try:
        players = [row[0] for row in players_worksheet.get_all_values()[1:]]
        ids = [id1.upper(), id2.upper(), id3.upper()]
        logging.info(f"Players from Sheet: {players}")
        logging.info(f"IDs from command: {ids}")
        if any(p not in players for p in ids):
            logging.warning(f"Mismatch: IDs {ids} not in {players}")
            await ctx.send('id manquant')
            return
        
        matches = matches_worksheet.get_all_records()
        thirty_days_ago = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=30)
        
        filtered = [m for m in matches if m['PlayerTag'].upper() in ids and parse(m['BattleTime']) > thirty_days_ago]
        logging.info(f"Filtered matches count: {len(filtered)}")
        if not filtered:
            await ctx.send('No matches found for these players in the last 30 days.')
            return
        
        # Vérification des clés nécessaires
        valid_matches = [m for m in filtered if all(key in m for key in ['BrawlerName'])]
        if not valid_matches:
            await ctx.send("No valid matches found with required data (BrawlerName).")
            return
        
        brawler_count = Counter(m['BrawlerName'] for m in valid_matches)
        top_5 = [brawler for brawler, _ in brawler_count.most_common(5)]
        
        embed = discord.Embed(title="Top 5 Brawlers and Counters", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        for brawler in top_5:
            counters_list = COUNTERS.get(brawler, ['No counters found'])
            embed.add_field(name=brawler, value=f"Counters: {', '.join(counters_list)}", inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in /counters: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")

# Exporter bot pour être utilisé par start.py
keep_alive()
bot  # Assure que bot est disponible à l'importation


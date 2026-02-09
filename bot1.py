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

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your actual values
DISCORD_TOKEN = os.getenv('D')  # Ton token Discord
SHEET_ID = os.getenv('G')  # ID de ta Google Sheet
CREDENTIALS_FILE = 'credentials.json'  # Chemin vers credentials.json
YOUR_CHANNEL_ID = 1403782456108388404  # ID du channel Discord

BOT_PREFIX = '!'  # Changé en '!' pour éviter conflits avec slash commands

# Placeholder pour COUNTERS (ajuste avec tes données, ex. : dict de brawlers et counters)
COUNTERS = {
    'COLT': ['BEA', 'PIPER', 'BROCK'],
    'SHELLY': ['BARLEY', 'DYNAMIKE', 'TICK'],
    # Ajoute les autres brawlers ici
}

# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_key(SHEET_ID)
players_worksheet = sheet.worksheet('Players')  # Column A has player tags like #ABC123
matches_worksheet = sheet.worksheet('Matches')  # Headers: PlayerTag, BattleTime, EventMode, EventMap, BrawlerName, Result, TrophyChange

# Discord bot setup with intents
intents = discord.Intents.default()
intents.message_content = True  # Ensure message content intent is enabled
bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents)

# Log startup details
@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logging.info(f'Intents enabled: {intents}')
    logging.info(f'Connected to {len(bot.guilds)} guilds')
    logging.info(f'Guilds: {[guild.name for guild in bot.guilds]}')
    channel = bot.get_channel(YOUR_CHANNEL_ID)
    if channel:
        await channel.send(f"Bot is online! ID: {bot.user.id}. Use {BOT_PREFIX}compare or {BOT_PREFIX}counters with your player IDs and map.")

# Log all messages and handle errors
@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    logging.info(f"Message received: {message.content} from {message.author.name} in channel {message.channel.id}")
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    logging.error(f"Command error: {error}")
    await ctx.send(f"Error: {str(error)}")

@bot.command(name='compare')
async def command_compare(ctx, id1: str, id2: str, id3: str, *, map_name: str):
    logging.info(f"Command {BOT_PREFIX}compare received from {ctx.author.name} with args: {id1}, {id2}, {id3}, {map_name}")
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
        
        logging.info(f"Filtering matches for {ids} on {map_name}: Raw data count = {len(matches)}")
        filtered = [m for m in matches if m['PlayerTag'].upper() in ids
                   and m['EventMap'].lower() == map_name.lower()
                   and parse(m['BattleTime']) > thirty_days_ago
                   and m.get('EventMode', '').lower() not in ['solo showdown', 'duo showdown']]
        logging.info(f"Filtered matches count: {len(filtered)}")
        if not filtered:
            await ctx.send('No matches found for these players on this map in the last 30 days (excluding Showdown).')
            return
        
        # Débogage : Afficher les données des matchs filtrés
        for i, m in enumerate(filtered):
            logging.info(f"Match {i + 1}: PlayerTag={m.get('PlayerTag')}, EventMode={m.get('EventMode')}, BrawlerName={m.get('BrawlerName')}, Result={m.get('Result')}")
        
        # Vérification des clés avec tolérance à la casse
        valid_matches = [m for m in filtered if m.get('BrawlerName', '').strip() and m.get('Result', '').strip().lower() in ['victory', 'defeat']]
        if not valid_matches:
            await ctx.send("No valid matches found with required data (BrawlerName and Result as 'victory' or 'defeat').")
            return
        
        brawler_stats = Counter(m['BrawlerName'].upper() for m in valid_matches) # Uniformiser en majuscules
        brawler_wins = Counter(m['BrawlerName'].upper() for m in valid_matches if m['Result'].lower() == 'victory')
        
        top_20 = brawler_stats.most_common(20)
        embed = discord.Embed(title=f"Top 20 Brawlers on {map_name}", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        
        for i, (brawler, total) in enumerate(top_20, 1):
            wins = brawler_wins.get(brawler, 0)
            winrate = (wins / total * 100) if total > 0 else 0
            embed.add_field(name=f"{i}. {brawler}", value=f"Used {total} times | Winrate: {winrate:.1f}% ({wins} wins)", inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in /compare: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")

@bot.command(name='counters')
async def command_counters(ctx, id1: str, id2: str, id3: str):
    logging.info(f"Command {BOT_PREFIX}counters received from {ctx.author.name} with args: {id1}, {id2}, {id3}")
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
bot.run(DISCORD_TOKEN)  # Ajouté pour lancer le bot

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
DISCORD_TOKEN = os.getenv('D')  # Remplacez par votre token Discord
SHEET_ID = os.getenv('G')       # Remplacez par l'ID de votre Google Sheet
CREDENTIALS_FILE = 'credentials.json'  # Chemin vers votre fichier de credentials
YOUR_CHANNEL_ID = 1403782456108388404  # Remplacez par l'ID du canal obtenu avec Mode développeur

# Configurable prefix set to '/'
BOT_PREFIX = '/'

# Counters dict from Reddit source
COUNTERS = {
    'SHELLY': ['NITA', 'MEG', 'THROWERS'],
    'NITA': ['AMBER', 'WALLBREAK', 'THROWERS'],
    'COLT': ['PIPER', 'NANI', 'LILY'],
    'BULL': ['COLETTE', 'EL PRIMO', 'SHELLY'],
    'BROCK': ['MAX', 'PIPER', 'NANI'],
    'EL PRIMO': ['COLETTE', 'CORDELIUS', 'GALE'],
    'BARLEY': ['EDGAR', 'MICO', 'WALLBREAK'],
    'POCO': ['MEG', 'CROW', 'THROWERS'],
    'ROSA': ['COLETTE', 'GALE', 'EL PRIMO'],
    'JESSIE': ['AMBER', 'SNIPERS', 'THROWERS'],
    'DYNAMIKE': ['EDGAR', 'MORTIS', 'WALLBREAK'],
    'TICK': ['MORTIS', 'MICO', 'GRAY'],
    '8-BIT': ['COLETTE', 'BELLE', 'THROWERS'],
    'RICO': ['HANK', 'WALLBREAK', 'THROWERS'],
    'DARRYL': ['SHELLY', 'GALE', 'EL PRIMO'],
    'PENNY': ['MEG', 'WALLBREAK', 'THROWERS'],
    'CARL': ['COLETTE', 'MEG', 'BUZZ'],
    'JACKY': ['GALE', 'WALLBREAK', 'THROWERS'],
    'GUS': ['EDGAR', 'PIPER', 'BO'],
    'BO': ['COLETTE', 'AMBER', 'MEG'],
    'EMZ': ['SAM', 'EDGAR', 'WALLBREAK'],
    'STU': ['PAM', 'MEG', 'CHARLIE'],
    'PIPER': ['NANI', 'ANGELO', 'SPROUT'],
    'PAM': ['COLETTE', 'DRACO', 'R-T'],
    'FRANK': ['COLETTE', 'GALE', 'SHELLY'],
    'BIBI': ['COLETTE', 'GALE', 'SHELLY'],
    'BEA': ['BO', 'PIPER', 'MANDY'],
    'NANI': ['SPROUT', 'TICK', 'PEARL'],
    'EDGAR': ['GALE', 'SURGE', 'SHELLY'],
    'GRIFF': ['STU', 'MAX', 'SNIPERS'],
    'GROM': ['ASSASSINS', 'ANYTHING WITH HIGH HP'],
    'BONNIE': ['COLETTE', 'MR. P', '8-BIT'],
    'GALE': ['THROWERS', 'ANYTHING THAT CAN OUTRANGE HIM'],
    'COLETTE': ['TARA', 'CHARLIE', 'THROWERS'],
    'BELLE': ['PIPER', 'NANI', 'PEARL'],
    'ASH': ['COLETTE', 'FRANK', 'ROSA'],
    'LOLA': ['BELLE', 'JESSIE', 'SNIPERS'],
    'SAM': ['COLETTE', 'GALE', 'EL PRIMO'],
    'MANDY': ['PEARL', 'ANGELO', 'SPROUT'],
    'MAISIE': ['CHARLIE', 'MAX', 'THROWERS'],
    'HANK': ['COLETTE', 'EL PRIMO', 'WALLBREAK'],
    'PEARL': ['COLETTE', 'MR. P', 'PLAYING AGGRESSIVELY'],
    'LARRY & LAWRIE': ['EDGAR', 'MICO', 'WALLBREAK'],
    'ANGELO': ['PIPER', 'NANI', 'SPROUT'],
    'BERY': ['MICO', 'EDGAR', 'WALLBREAK'],
    'MORTIS': ['GALE', 'SURGE', 'BULL'],
    'TARA': ['SANDY', 'AMBER', 'BUSTER'],
    'GENE': ['MR. P', 'TICK', 'ANYTHING WITH A SPAWNABLE GADGET/SUPER'],
    'MAX': ['CROW', 'LILY', 'MEG'],
    'MR. P': ['JANET', 'THROWERS', 'ANY SORT OF AGGRESSION'],
    'SPROUT': ['MORTIS', 'MICO', 'GRAY'],
    'BYRON': ['PIPER', 'MANDY', 'MR. P'],
    'SQUEAK': ['COLETTE', 'MR. P', 'THROWERS'],
    'LOU': ['MAX', 'SNIPERS', 'THROWERS'],
    'RUFFS': ['AMBER', 'BUZZ', 'THROWERS'],
    'BUZZ': ['GALE', 'COLETTE', 'SURGE'],
    'FANG': ['GALE', 'SHELLY', 'OTIS'],
    'EVE': ['PIPER', 'MANDY', 'NANI'],
    'JANET': ['TANKS', 'BUZZ', 'THROWERS'],
    'OTIS': ['CHARLIE', 'SNIPERS', 'THROWERS'],
    'BUSTER': ['COLETTE', 'ROSA', 'EL PRIMO'],
    'GRAY': ['MR. P', 'SNIPERS', 'TANKS'],
    'R-T': ['COLETTE', 'MR. P', 'THROWERS'],
    'WILLOW': ['WALLBREAK', 'BARLEY', 'ANY FORM OF AGGRESSION'],
    'DOUG': ['BASICALLY ANYTHING'],
    'CHUCK': ['CORDELIUS', 'CHARLIE', 'COLETTE'],
    'CHARLIE': ['AMBER', 'JESSIE', 'THROWERS'],
    'MICO': ['BULL', 'CHESTER', 'MEG'],
    'MELODIE': ['COLETTE', 'MEG', 'TANKS'],
    'LILY': ['GALE', 'MEG', 'TANKS'],
    'CLANCY': ['THROWERS', 'ANYTHING TO OUTRANGE HIM'],
    'SPIKE': ['SNIPERS', 'THROWERS', 'LILY'],
    'CROW': ['PAM', 'SNIPERS', 'MEG'],
    'LEON': ['CROW', 'GENE', 'NITA'],
    'SANDY': ['THROWERS', 'WALLBREAK', 'ANYTHING TO OUTRANGE HIM'],
    'AMBER': ['SNIPERS', 'CROW', 'THROWERS'],
    'MEG': ['COLETTE', 'THROWERS', 'BANNING HER'],
    'SURGE': ['CHARLIE', 'WALLBREAK', 'ANYTHING TO OUTRANGE HIM'],
    'CHESTER': ['MEG', 'SNIPERS', 'THROWERS'],
    'CORDELIUS': ['NITA', 'SURGE', 'BUSTER'],
    'KIT': ['MEG', 'BULL', 'SURGE'],
    'DRACO': ['FRANK', 'BUZZ', 'COLETTE'],
    # Nouveaux brawlers ajoutés
    'OLI': ['PIPER', 'NANI', 'BELLE'],  # Longue portée pour contrer sa mobilité
    'KAZE': ['TARA', 'GENE', 'SURGE'],  # Contrôle et burst pour sa furtivité
    'ALLI': ['COLETTE', 'MR. P', 'THROWERS'],  # Débuffs et contrôle à distance
    'JEA YONG': ['MAX', 'STU', 'SNIPERS'],  # Mobilité et précision
    'JUJU': ['GALE', 'SHELLY', 'EL PRIMO'],  # Tanks pour encaisser son burst
    'SHADE': ['AMBER', 'PIPER', 'NANI'],  # Longue portée pour éviter son stealth
    'BERRY': ['MICO', 'EDGAR', 'WALLBREAK'],  # Contrôle et pression rapprochée
    'CLANCY': ['THROWERS', 'ANYTHING TO OUTRANGE HIM']
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
        await channel.send(f"Bot is online! ID: {bot.user.id}. Use /compare or /counters with your player IDs and map.")

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
    logging.info(f"Command /compare received from {ctx.author.name} with args: {id1}, {id2}, {id3}, {map_name}")
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
        
        brawler_stats = Counter(m['BrawlerName'].upper() for m in valid_matches)  # Uniformiser en majuscules
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



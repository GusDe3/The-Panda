
import os
import math
from dotenv import load_dotenv
import discord
from discord.ext import commands
import gspread
from google.oauth2 import service_account
import datetime
from datetime import timedelta
from dateutil.parser import parse
from collections import Counter, defaultdict
import logging
from keep_alive import keep_alive
 
load_dotenv()
 
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
# Replace with your actual values
DISCORD_TOKEN = os.getenv('D')  # Token Discord
SHEET_ID = os.getenv('G')  # ID de la Google Sheet
CREDENTIALS_FILE = 'credentials.json'
YOUR_CHANNEL_ID = 1403782456108388404  # ID du channel Discord
 
BOT_PREFIX = '!'
 
# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_key(SHEET_ID)
players_worksheet = sheet.worksheet('Players')
matches_worksheet = sheet.worksheet('Matches')  # Headers: PlayerTag, BattleTime, EventMode, EventMap, BrawlerName, Result, TrophyChange, BattleType
 
# Discord bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents)
 
 
# ===========================================================================
#  FILTRE LADDER vs RANKED / TOURNOIS
# ===========================================================================
# /!\ PIEGE : dans l'API Brawl Stars, le type "ranked" = LADDER (trophees).
#     Le mode "Ranked" du jeu = "soloRanked" / "teamRanked".
# On exclut UNIQUEMENT le type ladder ; tout le reste est conserve.
LADDER_BATTLE_TYPES = {'ranked'}
 
def is_ladder_match(m):
    """True si la partie est du LADDER (a exclure des stats competitives)."""
    # 1) Methode fiable : colonne BattleType (= battle.type de l'API)
    btype = str(m.get('BattleType', '')).strip().lower()
    if btype:
        return btype in LADDER_BATTLE_TYPES
    # 2) Fallback pour les anciennes lignes sans BattleType : variation de trophees
    raw = str(m.get('TrophyChange', '')).strip()
    if not raw:
        return False
    try:
        return int(float(raw)) != 0
    except ValueError:
        return False
 
 
# ===========================================================================
#  OUTILS STATS
# ===========================================================================
def wilson_lower_bound(wins, total, z=1.96):
    """
    Borne basse de l'intervalle de Wilson (95%) : un 'winrate prudent'.
    Un 3/3 (100% sur 3 games) sera classe plus bas qu'un 40/55 (~73% sur 55).
    """
    if total <= 0:
        return 0.0
    phat = wins / total
    denom = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    marge = z * math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total)
    return max(0.0, (centre - marge) / denom)
 
 
def get_team_matches(ids, map_name=None, days=30):
    """Recupere les matchs (non-ladder) d'un trio, eventuellement sur une map."""
    matches = matches_worksheet.get_all_records()
    cutoff = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=days)
    out = []
    for m in matches:
        if m.get('PlayerTag', '').upper() not in ids:
            continue
        if is_ladder_match(m):
            continue
        if m.get('EventMode', '').lower() in ('solo showdown', 'duo showdown'):
            continue
        try:
            if parse(m['BattleTime']) <= cutoff:
                continue
        except Exception:
            continue
        if map_name and m.get('EventMap', '').lower() != map_name.lower():
            continue
        out.append(m)
    return out
 
 
# ===========================================================================
#  EVENTS
# ===========================================================================
@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logging.info(f'Connected to {len(bot.guilds)} guilds: {[g.name for g in bot.guilds]}')
    channel = bot.get_channel(YOUR_CHANNEL_ID)
    if channel:
        await channel.send(
            f"Bot online ! Commandes : {BOT_PREFIX}compare, {BOT_PREFIX}main, "
            f"{BOT_PREFIX}draft, {BOT_PREFIX}debug"
        )
 
@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    logging.info(f"Message: {message.content} from {message.author.name} in {message.channel.id}")
    await bot.process_commands(message)
 
@bot.event
async def on_command_error(ctx, error):
    logging.error(f"Command error: {error}")
    await ctx.send(f"Error: {str(error)}")
 
 
# ===========================================================================
#  COMMANDE DEBUG  ->  a lancer pour diagnostiquer la feuille
# ===========================================================================
@bot.command(name='debug')
async def command_debug(ctx):
    logging.info(f"Command {BOT_PREFIX}debug from {ctx.author.name}")
    try:
        matches = matches_worksheet.get_all_records()
        total = len(matches)
        if total == 0:
            await ctx.send("La feuille Matches est vide.")
            return
 
        keys = list(matches[0].keys())
 
        tc = Counter()
        for m in matches:
            raw = str(m.get('TrophyChange', '')).strip()
            if raw == '':
                tc['vide'] += 1
            elif raw in ('0', '0.0'):
                tc['zero'] += 1
            else:
                tc['non-zero'] += 1
 
        has_bt = 'BattleType' in keys
        bt = Counter()
        if has_bt:
            for m in matches:
                bt[str(m.get('BattleType', '')).strip().lower() or '(vide)'] += 1
 
        nb_ladder = sum(1 for m in matches if is_ladder_match(m))
 
        embed = discord.Embed(title="Debug - donnees Matches", color=discord.Color.orange())
        embed.add_field(name="Lignes totales", value=str(total), inline=False)
        embed.add_field(name="Colonnes detectees", value=", ".join(keys), inline=False)
        embed.add_field(
            name="TrophyChange",
            value=f"vide: {tc['vide']} | zero: {tc['zero']} | non-zero: {tc['non-zero']}",
            inline=False
        )
        if has_bt:
            top_bt = " | ".join(f"{k}: {v}" for k, v in bt.most_common(8))
            embed.add_field(name="BattleType (valeurs)", value=top_bt or "(vide)", inline=False)
        else:
            embed.add_field(name="BattleType", value="ABSENTE (ajoute l'en-tete colonne H)", inline=False)
        embed.add_field(name="Parties classees 'ladder' (exclues)", value=f"{nb_ladder} / {total}", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in debug: {e}")
        await ctx.send("Une erreur s'est produite dans !debug.")
 
 
# ===========================================================================
#  ASSISTANT DE DRAFT
# ===========================================================================
# Syntaxe :
#   !draft <id1> <id2> <id3> <map> | ban: COLT, PIPER | enemy: SHELLY, BULL | ally: SPIKE
# Tout ce qui suit la map est optionnel.
DRAFT_DAYS = 30          # fenetre d'historique
DRAFT_MIN_GAMES = 3      # nb de games mini pour conseiller un brawler
W_MAP = 0.65             # poids du winrate sur la map
W_SYN = 0.35             # poids de la synergie avec les allies deja pick
 
def parse_draft_args(raw):
    """Parse '<map> | ban: ... | enemy: ... | ally: ...' (FR/EN toleres)."""
    map_name, bans, enemies, allies = None, [], [], []
    for part in (p.strip() for p in raw.split('|')):
        if not part:
            continue
        if ':' in part:
            key, _, val = part.partition(':')
            key = key.strip().lower()
            vals = [v.strip().upper() for v in val.replace(',', ' ').split() if v.strip()]
            if key in ('ban', 'bans', 'interdit', 'interdits'):
                bans = vals
            elif key in ('enemy', 'enemies', 'ennemi', 'ennemis', 'adv', 'adverse'):
                enemies = vals
            elif key in ('ally', 'allies', 'allie', 'team', 'equipe'):
                allies = vals
            elif key in ('map', 'carte') and not map_name:
                map_name = val.strip()
        elif map_name is None:
            map_name = part
    return map_name, bans, enemies, allies
 
 
@bot.command(name='draft')
async def command_draft(ctx, id1: str, id2: str, id3: str, *, rest: str = ""):
    logging.info(f"Command {BOT_PREFIX}draft from {ctx.author.name}: {id1} {id2} {id3} | {rest}")
    try:
        ids = {id1.upper(), id2.upper(), id3.upper()}
        map_name, bans, enemies, allies = parse_draft_args(rest)
 
        if not map_name:
            await ctx.send("Syntaxe : `!draft <id1> <id2> <id3> <map> | ban: COLT | enemy: SHELLY | ally: SPIKE`")
            return
 
        team_matches = get_team_matches(ids, map_name=map_name, days=DRAFT_DAYS)
        if not team_matches:
            await ctx.send(f"Aucune donnee (Ranked/tournoi) pour ce trio sur **{map_name}** sur {DRAFT_DAYS} jours.")
            return
 
        # --- Winrate solo par brawler sur la map ---
        games, wins = Counter(), Counter()
        for m in team_matches:
            b = m.get('BrawlerName', '').upper()
            r = m.get('Result', '').lower()
            if not b or r not in ('victory', 'defeat'):
                continue
            games[b] += 1
            if r == 'victory':
                wins[b] += 1
 
        # --- Reconstruction des comps via BattleTime (coequipiers = meme heure) ---
        battles = defaultdict(dict)  # BattleTime -> {tag: (brawler, result)}
        for m in team_matches:
            tag = m.get('PlayerTag', '').upper()
            b = m.get('BrawlerName', '').upper()
            r = m.get('Result', '').lower()
            if tag and b:
                battles[m['BattleTime']][tag] = (b, r)
 
        def comp_winrate(required):
            req = set(x.upper() for x in required)
            g = w = 0
            for players in battles.values():
                comp = {br for br, _ in players.values()}
                if req.issubset(comp):
                    g += 1
                    if next(iter(players.values()))[1] == 'victory':
                        w += 1
            return w, g
 
        # --- Pool de candidats (on retire bans, picks ennemis et allies) ---
        unavailable = {x.upper() for x in (bans + enemies + allies)}
        pool = [b for b in games if b not in unavailable]
        eligible = [b for b in pool if games[b] >= DRAFT_MIN_GAMES]
        if len(eligible) < 3:
            eligible = pool
        if not eligible:
            await ctx.send("Aucun brawler conseillable (tout est banni/pris ou pas assez de donnees).")
            return
 
        # --- Scoring : winrate map (prudent) + synergie avec les allies ---
        results = []
        for b in eligible:
            map_wr = wins[b] / games[b]
            syn_w, syn_g = comp_winrate(allies + [b]) if allies else (0, 0)
            syn_wr = (syn_w / syn_g) if syn_g else 0.0
            score = (W_MAP * wilson_lower_bound(wins[b], games[b])
                     + W_SYN * wilson_lower_bound(syn_w, syn_g))
            results.append((score, b, map_wr, games[b], syn_wr, syn_g))
 
        results.sort(reverse=True)
        top = results[:6]
 
        ctx_bits = []
        if bans: ctx_bits.append(f"bans: {', '.join(bans)}")
        if enemies: ctx_bits.append(f"ennemis: {', '.join(enemies)}")
        if allies: ctx_bits.append(f"allies: {', '.join(allies)}")
        subtitle = " | ".join(ctx_bits) if ctx_bits else "aucun contexte de draft fourni"
 
        embed = discord.Embed(title=f"Draft - {map_name}", description=subtitle,
                              color=discord.Color.from_rgb(255, 69, 0))
        for i, (score, b, map_wr, g, syn_wr, syn_g) in enumerate(top, 1):
            flag = " (peu de games)" if g < 5 else ""
            line = f"Map WR **{map_wr*100:.0f}%** ({g}g)"
            if allies and syn_g:
                line += f" | Synergie {syn_wr*100:.0f}% ({syn_g}g)"
            embed.add_field(name=f"{i}. {b}  -  score {score:.2f}{flag}", value=line, inline=False)
 
        embed.set_footer(text=f"Base sur {len(team_matches)} parties (Ranked/tournoi, {DRAFT_DAYS}j). "
                              f"Demande par {ctx.author.name}")
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in draft: {e}")
        await ctx.send("Une erreur s'est produite dans !draft.")
 
 
# ===========================================================================
#  COMMANDE COMPARE
# ===========================================================================
@bot.command(name='compare')
async def command_compare(ctx, id1: str, id2: str, id3: str, *, map_name: str):
    logging.info(f"Command {BOT_PREFIX}compare from {ctx.author.name}: {id1}, {id2}, {id3}, {map_name}")
    try:
        players = [row[0] for row in players_worksheet.get_all_values()[1:]]
        ids = [id1.upper(), id2.upper(), id3.upper()]
        if any(p not in players for p in ids):
            logging.warning(f"Mismatch: IDs {ids} not in {players}")
            await ctx.send('id manquant')
            return
 
        matches = matches_worksheet.get_all_records()
        thirty_days_ago = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=30)
 
        filtered = [m for m in matches if m['PlayerTag'].upper() in ids
                    and m['EventMap'].lower() == map_name.lower()
                    and parse(m['BattleTime']) > thirty_days_ago
                    and m.get('EventMode', '').lower() not in ['solo showdown', 'duo showdown']
                    and not is_ladder_match(m)]
        logging.info(f"Filtered matches count: {len(filtered)}")
        if not filtered:
            await ctx.send('No matches found for these players on this map in the last 30 days (Ranked/tournois uniquement).')
            return
 
        valid_matches = [m for m in filtered if m.get('BrawlerName', '').strip()
                         and m.get('Result', '').strip().lower() in ['victory', 'defeat']]
        if not valid_matches:
            await ctx.send("No valid matches found with required data (BrawlerName and Result).")
            return
 
        brawler_stats = Counter(m['BrawlerName'].upper() for m in valid_matches)
        brawler_wins = Counter(m['BrawlerName'].upper() for m in valid_matches if m['Result'].lower() == 'victory')
 
        top_15 = brawler_stats.most_common(15)
        embed = discord.Embed(title=f"Top 15 Brawlers on {map_name}", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        for i, (brawler, total) in enumerate(top_15, 1):
            w = brawler_wins.get(brawler, 0)
            winrate = (w / total * 100) if total > 0 else 0
            embed.add_field(name=f"{i}. {brawler}", value=f"Used {total} times | Winrate: {winrate:.1f}% ({w} wins)", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in compare: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")
 
 
# ===========================================================================
#  COMMANDE MAIN
# ===========================================================================
@bot.command(name='main')
async def command_main(ctx, *, map_name: str):
    logging.info(f"Command {BOT_PREFIX}main from {ctx.author.name}: {map_name}")
    try:
        matches = matches_worksheet.get_all_records()
        fifteen_days_ago = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=15)
 
        filtered = [m for m in matches if m.get('EventMap', '').lower() == map_name.lower()
                    and parse(m['BattleTime']) > fifteen_days_ago
                    and m.get('EventMode', '').lower() not in ['solo showdown', 'duo showdown']
                    and not is_ladder_match(m)]
        if not filtered:
            await ctx.send('No matches found for this map in the last 15 days (Ranked/tournois uniquement).')
            return
 
        brawler_count = Counter(m['BrawlerName'].upper() for m in filtered if m.get('BrawlerName'))
        top_15 = brawler_count.most_common(15)
 
        embed = discord.Embed(title=f"Top 15 Brawlers on {map_name} (last 15 days)", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        for i, (brawler, count) in enumerate(top_15, 1):
            embed.add_field(name=f"{i}. {brawler}", value=f"Used {count} times", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in main: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")
 
 
keep_alive()
bot.run(DISCORD_TOKEN)
 

import os
import math
import time
import requests
from dotenv import load_dotenv
import discord
from discord.ext import commands, tasks
import gspread
from google.oauth2 import service_account
import datetime
from datetime import timedelta
from dateutil.parser import parse
from collections import Counter, defaultdict
import logging
from keep_alive import keep_alive

# --- ML (optionnel) : si scikit-learn manque, on retombe sur le winrate -----
try:
    from sklearn.linear_model import LogisticRegression
    import numpy as np
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DISCORD_TOKEN = os.getenv('D')
SHEET_ID = os.getenv('G')
BS_TOKEN = os.getenv('B')  # token Brawl Stars (pour la commande !inspect)
CREDENTIALS_FILE = 'credentials.json'
YOUR_CHANNEL_ID = 1403782456108388404

BOT_PREFIX = '!'

# Set up Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_key(SHEET_ID)
players_worksheet = sheet.worksheet('Players')
matches_worksheet = sheet.worksheet('Matches')  # PlayerTag, BattleTime, EventMode, EventMap, BrawlerName, Result, TrophyChange, BattleType

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents)


# ===========================================================================
#  HELPERS GENERAUX
# ===========================================================================
def normalize_tag(t):
    """Uniformise un tag joueur en '#XXXX' majuscule (tolere espaces / # manquant)."""
    return '#' + str(t).strip().lstrip('#').upper()


# /!\ Dans l'API Brawl Stars, "ranked" = LADDER. Le mode "Ranked" = soloRanked/teamRanked.
LADDER_BATTLE_TYPES = {'ranked'}

def is_ladder_match(m):
    """True si la partie est du LADDER (a exclure)."""
    btype = str(m.get('BattleType', '')).strip().lower()
    if btype:
        return btype in LADDER_BATTLE_TYPES
    raw = str(m.get('TrophyChange', '')).strip()
    if not raw:
        return False
    try:
        return int(float(raw)) != 0
    except ValueError:
        return False


def wilson_lower_bound(wins, total, z=1.96):
    """Borne basse de Wilson (95%) : winrate prudent qui penalise les petits echantillons."""
    if total <= 0:
        return 0.0
    phat = wins / total
    denom = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    marge = z * math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total)
    return max(0.0, (centre - marge) / denom)


def get_team_matches(ids, map_name=None, days=30):
    """Matchs (non-ladder) d'un trio, eventuellement sur une map. `ids` = set de tags normalises."""
    matches = matches_worksheet.get_all_records()
    cutoff = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=days)
    out = []
    for m in matches:
        if normalize_tag(m.get('PlayerTag', '')) not in ids:
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
#  MODULE D'APPRENTISSAGE AUTOMATIQUE
# ===========================================================================
# Modele : regression logistique sur les comps reconstituees (multi-hot brawlers)
#          -> P(victoire). Sert a noter la FORCE d'un brawler (bans) et a evaluer
#          un PICK. Avec peu de donnees elle apprend surtout des forces regularisees,
#          donc les picks sont blendes avec la synergie observee.
MODEL_MIN_SAMPLES = 40   # nb de parties mini pour entrainer (sinon fallback winrate)
MODEL_C = 0.3            # regularisation L2 (petit = plus fort, anti-overfit)
W_MODEL = 0.6            # poids du modele dans le score de pick
W_SYNERGY = 0.4          # poids de la synergie observee dans le score de pick

def build_battles(team_matches):
    """Regroupe les lignes par BattleTime -> {tag: (brawler, result)} (coequipiers = meme heure)."""
    battles = defaultdict(dict)
    for m in team_matches:
        tag = normalize_tag(m.get('PlayerTag', ''))
        b = m.get('BrawlerName', '').upper()
        r = m.get('Result', '').lower()
        if b and r in ('victory', 'defeat'):
            battles[m['BattleTime']][tag] = (b, r)
    return battles

def battles_to_samples(battles):
    """Chaque partie -> (set de brawlers de notre cote, 1=win/0=loss)."""
    samples = []
    for players in battles.values():
        brawlers = {br for br, _ in players.values()}
        res = next(iter(players.values()))[1]
        if brawlers:
            samples.append((brawlers, 1 if res == 'victory' else 0))
    return samples

def comp_winrate(battles, required):
    """Winrate des parties ou tous les brawlers `required` etaient presents."""
    req = {x.upper() for x in required}
    g = w = 0
    for players in battles.values():
        comp = {br for br, _ in players.values()}
        if req.issubset(comp):
            g += 1
            if next(iter(players.values()))[1] == 'victory':
                w += 1
    return w, g

def train_model(samples, all_brawlers):
    """Entraine la regression logistique. Retourne (model, idx) ou None si impossible."""
    if not SKLEARN_AVAILABLE or len(samples) < MODEL_MIN_SAMPLES or not all_brawlers:
        return None
    y = [w for _, w in samples]
    if len(set(y)) < 2:   # besoin de victoires ET defaites
        return None
    idx = {b: i for i, b in enumerate(all_brawlers)}
    X = np.zeros((len(samples), len(idx)))
    for r, (brawlers, _) in enumerate(samples):
        for b in brawlers:
            if b in idx:
                X[r, idx[b]] = 1.0
    try:
        model = LogisticRegression(C=MODEL_C, max_iter=2000)
        model.fit(X, np.array(y, dtype=float))
    except Exception as e:
        logging.error(f"train_model failed: {e}")
        return None
    return (model, idx)

def model_winprob(model_info, comp):
    """P(victoire) predite par le modele pour une comp (set de brawlers)."""
    model, idx = model_info
    x = np.zeros((1, len(idx)))
    for b in comp:
        if b in idx:
            x[0, idx[b]] = 1.0
    return float(model.predict_proba(x)[0][1])

def suggest_bans(model_info, all_brawlers, taken, games, wins, n=5, min_games=3):
    """Bans = brawlers les plus FORTS sur la map (a refuser a l'adversaire)."""
    cands = [b for b in all_brawlers if b not in taken and games[b] >= min_games]
    scored = []
    for b in cands:
        force = model_winprob(model_info, {b}) if model_info else wilson_lower_bound(wins[b], games[b])
        scored.append((force, b, games[b]))
    scored.sort(key=lambda t: t[0], reverse=True)
    return scored[:n]

def suggest_picks(model_info, battles, all_brawlers, allies, taken, games, wins, n=5, min_games=2):
    """Picks = meilleurs completements de VOTRE comp (modele + synergie observee)."""
    ally_set = {a.upper() for a in allies}
    cands = [b for b in all_brawlers if b not in taken and games[b] >= min_games]
    if len(cands) < n:
        cands = [b for b in all_brawlers if b not in taken]
    scored = []
    for b in cands:
        if ally_set:
            syn_w, syn_g = comp_winrate(battles, ally_set | {b})
        else:
            syn_w, syn_g = wins[b], games[b]
        syn_score = wilson_lower_bound(syn_w, syn_g)
        if model_info:
            score = W_MODEL * model_winprob(model_info, ally_set | {b}) + W_SYNERGY * syn_score
        else:
            score = syn_score
        scored.append((score, b, syn_g))
    scored.sort(key=lambda t: t[0], reverse=True)
    return scored[:n]


# ===========================================================================
#  PARSING DRAFT
# ===========================================================================
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


# ===========================================================================
#  SCRAPING INTEGRE (remplace data.py) - utilise par la commande !update
# ===========================================================================
MATCHES_HEADER = ['PlayerTag', 'BattleTime', 'EventMode', 'EventMap',
                  'BrawlerName', 'Result', 'TrophyChange', 'BattleType']

def ensure_header():
    """Garantit que la ligne 1 de Matches est bien l'en-tete (repare si manquant)."""
    first = matches_worksheet.row_values(1)
    if first[:1] != ['PlayerTag']:
        # feuille vide OU ligne 1 = donnees -> on insere l'en-tete tout en haut
        matches_worksheet.insert_row(MATCHES_HEADER, index=1)
        logging.info("En-tete Matches (re)cree.")

def _extract_brawler(p):
    """
    Nom du brawler d'un joueur, en gerant les structures variables de l'API.
    Retourne None pour les modes type Duels (champ 'brawlers' au pluriel) ou
    toute structure inattendue -> la partie sera ignoree (hors 3v3).
    """
    br = p.get('brawler')
    if isinstance(br, dict) and br.get('name'):
        return br['name'].upper()
    return None

def scrape_once():
    """
    Recupere les battlelogs via l'API BS et ecrit les nouvelles parties non-ladder.
    Synchrone (a lancer dans un executor). Retourne (added, skipped_ladder, types).
    """
    ensure_header()
    players = [r[0] for r in players_worksheet.get_all_values()[1:] if r and r[0].strip()]
    existing = matches_worksheet.get_all_records()
    existing_keys = {(m.get('PlayerTag'), m.get('BattleTime')) for m in existing}
    headers = {'Authorization': f'Bearer {BS_TOKEN}'}

    new_rows = []
    skipped = 0
    types = Counter()

    for player in players:
        tag = player.strip().lstrip('#').upper()
        url = f'https://api.brawlstars.com/v1/players/%23{tag}/battlelog'
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            battles = resp.json().get('items', [])
        except Exception as e:
            logging.error(f"scrape: erreur HTTP pour {player}: {e}")
            continue

        for battle in battles:
            try:
                bt = battle.get('battleTime')
                if (player, bt) in existing_keys:
                    continue
                event = battle.get('event', {})
                emap = event.get('map')
                if not emap:
                    continue
                bd = battle.get('battle', {})
                btype = bd.get('type', '')
                tch = bd.get('trophyChange', 0)
                # filtre ladder (type "ranked" ou partie a trophees)
                if btype.lower() in LADDER_BATTLE_TYPES or tch != 0:
                    skipped += 1
                    continue

                brawler = None
                if 'teams' in bd:
                    for team in bd['teams']:
                        for p in team:
                            if p.get('tag') == player:
                                brawler = _extract_brawler(p)
                                break
                        if brawler:
                            break
                elif 'players' in bd:
                    for p in bd['players']:
                        if p.get('tag') == player:
                            brawler = _extract_brawler(p)
                            break
                if not brawler:
                    continue  # mode hors 3v3 (Duels...) ou structure inattendue -> ignore

                row = [player, bt, event.get('mode', ''), emap,
                       brawler, bd.get('result', ''), str(tch), btype]
                new_rows.append(row)
                existing_keys.add((player, bt))
                types[btype] += 1
            except Exception as e:
                logging.error(f"scrape: bataille ignoree pour {player}: {e}")
                continue

    # ecriture par lots (rapide, pas d'appel API ligne par ligne)
    for i in range(0, len(new_rows), 500):
        matches_worksheet.append_rows(new_rows[i:i + 500])
        time.sleep(1)

    return len(new_rows), skipped, dict(types)


# --- Rafraichissement automatique -------------------------------------------
AUTO_UPDATE_MINUTES = 15   # intervalle du scraping auto (baisse a 10 si grosses sessions)
_scrape_in_progress = False

async def do_scrape():
    """Lance scrape_once en evitant les executions simultanees.
       Retourne (added, skipped, types) ou None si un scrape est deja en cours."""
    global _scrape_in_progress
    if _scrape_in_progress:
        return None
    _scrape_in_progress = True
    try:
        return await bot.loop.run_in_executor(None, scrape_once)
    finally:
        _scrape_in_progress = False

@tasks.loop(minutes=AUTO_UPDATE_MINUTES)
async def auto_update():
    result = await do_scrape()
    if result is None:
        return
    added, skipped, types = result
    logging.info(f"Auto-update: {added} ajoutees, {skipped} ladder ignorees, types={types}")

@auto_update.before_loop
async def before_auto_update():
    await bot.wait_until_ready()


# ===========================================================================
#  EVENTS
# ===========================================================================
@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logging.info(f'ML disponible: {SKLEARN_AVAILABLE}')
    if not auto_update.is_running():
        auto_update.start()
        logging.info(f"Rafraichissement automatique active (toutes les {AUTO_UPDATE_MINUTES} min).")
    channel = bot.get_channel(YOUR_CHANNEL_ID)
    if channel:
        await channel.send(
            f"Bot online ! Maj auto toutes les {AUTO_UPDATE_MINUTES} min. "
            f"Commandes : {BOT_PREFIX}compare, {BOT_PREFIX}main, {BOT_PREFIX}draft, "
            f"{BOT_PREFIX}debug, {BOT_PREFIX}inspect, {BOT_PREFIX}update, {BOT_PREFIX}reset"
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
#  COMMANDE DEBUG
# ===========================================================================
@bot.command(name='debug')
async def command_debug(ctx):
    try:
        matches = matches_worksheet.get_all_records()
        total = len(matches)
        if total == 0:
            await ctx.send("La feuille Matches est vide.")
            return
        keys = list(matches[0].keys())
        bt = Counter()
        if 'BattleType' in keys:
            for m in matches:
                bt[str(m.get('BattleType', '')).strip().lower() or '(vide)'] += 1
        nb_ladder = sum(1 for m in matches if is_ladder_match(m))
        embed = discord.Embed(title="Debug - donnees Matches", color=discord.Color.orange())
        embed.add_field(name="Lignes totales", value=str(total), inline=False)
        embed.add_field(name="Colonnes", value=", ".join(keys), inline=False)
        if bt:
            embed.add_field(name="BattleType", value=" | ".join(f"{k}: {v}" for k, v in bt.most_common(8)), inline=False)
        else:
            embed.add_field(name="BattleType", value="ABSENTE (ajoute l'en-tete colonne H)", inline=False)
        embed.add_field(name="Classees 'ladder' (exclues)", value=f"{nb_ladder} / {total}", inline=False)
        embed.add_field(name="Module ML", value="actif" if SKLEARN_AVAILABLE else "inactif (scikit-learn absent)", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in debug: {e}")
        await ctx.send("Une erreur s'est produite dans !debug.")


# ===========================================================================
#  COMMANDE INSPECT  ->  interroge l'API BS DEPUIS le serveur (bon token/IP)
# ===========================================================================
# Usage : !inspect            (teste le 1er joueur de la feuille Players)
#         !inspect #ABC123     (teste un joueur precis)
@bot.command(name='inspect')
async def command_inspect(ctx, tag: str = None):
    logging.info(f"Command {BOT_PREFIX}inspect from {ctx.author.name}: {tag}")
    try:
        if tag is None:
            players = [r[0] for r in players_worksheet.get_all_values()[1:] if r and r[0].strip()]
            if not players:
                await ctx.send("Aucun joueur dans la feuille Players.")
                return
            tag = players[0]
        clean = tag.strip().lstrip('#').upper()

        url = f'https://api.brawlstars.com/v1/players/%23{clean}/battlelog'
        r = requests.get(url, headers={'Authorization': f'Bearer {BS_TOKEN}'}, timeout=15)

        embed = discord.Embed(title=f"Inspect API - #{clean}", color=discord.Color.blue())
        embed.add_field(name="HTTP status", value=str(r.status_code), inline=False)

        if r.status_code != 200:
            embed.add_field(name="Reponse API", value=f"```{r.text[:600]}```", inline=False)
            embed.set_footer(text="Status 403 = token verrouille sur une autre IP -> regenere le token pour l'IP du serveur.")
            await ctx.send(embed=embed)
            return

        items = r.json().get('items', [])
        embed.add_field(name="Batailles renvoyees", value=str(len(items)), inline=False)

        types = Counter()
        trophy = Counter()
        for it in items:
            b = it.get('battle', {})
            types[str(b.get('type', '(AUCUN)'))] += 1
            trophy['avec' if 'trophyChange' in b else 'sans'] += 1
        embed.add_field(name="Types de battle", value=", ".join(f"{k}: {v}" for k, v in types.items()) or "—", inline=False)
        embed.add_field(name="trophyChange present", value=", ".join(f"{k}: {v}" for k, v in trophy.items()) or "—", inline=False)

        if items:
            b0 = items[0].get('battle', {})
            embed.add_field(name="Cles de battle['battle']", value=(", ".join(b0.keys()))[:1000] or "—", inline=False)
            embed.add_field(name="1ere bataille",
                            value=f"type = {b0.get('type')!r}\ntrophyChange = {b0.get('trophyChange')!r}", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in inspect: {e}")
        await ctx.send(f"Erreur dans !inspect : {e}")


# ===========================================================================
#  COMMANDE UPDATE  ->  scrape l'API et remplit la feuille (remplace data.py)
# ===========================================================================
@bot.command(name='update')
async def command_update(ctx):
    logging.info(f"Command {BOT_PREFIX}update from {ctx.author.name}")
    await ctx.send("Mise a jour des donnees en cours... (ca peut prendre une minute)")
    try:
        result = await do_scrape()
        if result is None:
            await ctx.send("Une mise a jour est deja en cours, reessaie dans un instant.")
            return
        added, skipped, types = result
        msg = f"Termine : **{added}** nouvelles parties ajoutees, **{skipped}** parties ladder ignorees."
        if types:
            msg += "\nTypes ajoutes : " + ", ".join(f"{k}: {v}" for k, v in types.items())
        await ctx.send(msg)
    except Exception as e:
        logging.error(f"Error in update: {e}")
        await ctx.send(f"Erreur dans !update : {e}")


# ===========================================================================
#  COMMANDE RESET  ->  vide la feuille Matches et recree l'en-tete (depuis Discord)
# ===========================================================================
@bot.command(name='reset')
async def command_reset(ctx, confirm: str = None):
    logging.info(f"Command {BOT_PREFIX}reset from {ctx.author.name} (confirm={confirm})")
    if confirm != 'CONFIRM':
        await ctx.send("ATTENTION : cette commande vide TOUTE la feuille Matches. "
                       "Pour confirmer, tape `!reset CONFIRM`. Ensuite lance `!update` pour la repeupler.")
        return
    try:
        def do_reset():
            matches_worksheet.clear()
            matches_worksheet.append_row(MATCHES_HEADER)
        await bot.loop.run_in_executor(None, do_reset)
        await ctx.send("Feuille Matches videe et en-tete recree. Lance maintenant `!update`.")
    except Exception as e:
        logging.error(f"Error in reset: {e}")
        await ctx.send(f"Erreur dans !reset : {e}")


# ===========================================================================
#  COMMANDE DRAFT  ->  top 15 des brawlers les plus joues par le trio sur une map
# ===========================================================================
DRAFT_DAYS = 60

@bot.command(name='draft')
async def command_draft(ctx, id1: str, id2: str, id3: str, *, map_name: str):
    logging.info(f"Command {BOT_PREFIX}draft from {ctx.author.name}: {id1} {id2} {id3} | {map_name}")
    try:
        ids = {normalize_tag(id1), normalize_tag(id2), normalize_tag(id3)}

        team_matches = get_team_matches(ids, map_name=map_name, days=DRAFT_DAYS)
        if not team_matches:
            await ctx.send(f"Aucune donnee (Ranked/tournoi) pour ce trio sur **{map_name}** sur {DRAFT_DAYS} jours. "
                           f"Verifie avec `!debug` (nb de lignes) ou `!main {map_name}`.")
            return

        games, wins = Counter(), Counter()
        for m in team_matches:
            b = m.get('BrawlerName', '').upper()
            r = m.get('Result', '').lower()
            if not b or r not in ('victory', 'defeat'):
                continue
            games[b] += 1
            if r == 'victory':
                wins[b] += 1
        top15 = games.most_common(15)

        embed = discord.Embed(title=f"Top 15 brawlers - {map_name}",
                              color=discord.Color.from_rgb(255, 69, 0))
        top_lines = [f"{i}. {b} — {tot}g ({(wins[b]/tot*100):.0f}%)" for i, (b, tot) in enumerate(top15, 1)]
        embed.add_field(name=f"Trio sur {DRAFT_DAYS} jours", value="\n".join(top_lines) or "—", inline=False)
        embed.set_footer(text=f"Base sur {len(team_matches)} parties (Ranked/tournoi). Demande par {ctx.author.name}")
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in draft: {e}")
        await ctx.send("Une erreur s'est produite dans !draft.")


# ===========================================================================
#  COMMANDE COMPARE (corrigee)
# ===========================================================================
@bot.command(name='compare')
async def command_compare(ctx, id1: str, id2: str, id3: str, *, map_name: str):
    logging.info(f"Command {BOT_PREFIX}compare from {ctx.author.name}: {id1}, {id2}, {id3}, {map_name}")
    try:
        sheet_players = {normalize_tag(row[0]) for row in players_worksheet.get_all_values()[1:]
                         if row and row[0].strip()}
        ids = {normalize_tag(id1), normalize_tag(id2), normalize_tag(id3)}
        missing = [i for i in ids if i not in sheet_players]
        if missing:
            await ctx.send(f"ID introuvable(s) dans la feuille Players : {', '.join(missing)}")
            return

        team_matches = get_team_matches(ids, map_name=map_name, days=30)
        if not team_matches:
            await ctx.send(f"Aucune partie (Ranked/tournoi) pour ce trio sur **{map_name}** sur 30 jours. "
                           f"Verifie avec `!debug` ou `!main {map_name}`.")
            return

        games = Counter(m['BrawlerName'].upper() for m in team_matches
                        if m.get('BrawlerName', '').strip() and m.get('Result', '').strip().lower() in ('victory', 'defeat'))
        wins = Counter(m['BrawlerName'].upper() for m in team_matches if m.get('Result', '').strip().lower() == 'victory')
        if not games:
            await ctx.send("Aucune partie valide (BrawlerName / Result manquants).")
            return

        embed = discord.Embed(title=f"Top 15 Brawlers on {map_name}", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        for i, (brawler, total) in enumerate(games.most_common(15), 1):
            w = wins.get(brawler, 0)
            wr = (w / total * 100) if total else 0
            embed.add_field(name=f"{i}. {brawler}", value=f"Used {total} times | Winrate: {wr:.1f}% ({w} wins)", inline=False)
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
        embed = discord.Embed(title=f"Top 15 Brawlers on {map_name} (last 15 days)", color=discord.Color.from_rgb(255, 69, 0))
        embed.set_footer(text=f"Requested by {ctx.author.name} at {datetime.datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")
        for i, (brawler, count) in enumerate(brawler_count.most_common(15), 1):
            embed.add_field(name=f"{i}. {brawler}", value=f"Used {count} times", inline=False)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.error(f"Error in main: {e}")
        await ctx.send("Une erreur s'est produite dans la commande.")


keep_alive()
bot.run(DISCORD_TOKEN)

import json
import sqlite3
import csv
import ast
from pathlib import Path

# ---------- paths ----------
STATS_JSON_PATH = Path("data/nba_player_stats_2000_2025.json")
MODERN_PLAYERS_PATH = Path("data/modern_nba_players.json")
MISSING_CSV_PATH = Path("data/nba_player_missing_seasons.csv")
DRAFT_JSON_PATH = Path("data/draft_history.json")
DB_PATH = Path("data/nba_stats.db")


# ---------- helpers ----------
def safe_float(x):
    """Convert to float; return None if missing/invalid."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def compute_metrics(r):
    """
    Compute efficiency metrics and expanded per-36 stats.
    """
    min_ = safe_float(r.get("MIN"))
    pts = safe_float(r.get("PTS"))
    reb = safe_float(r.get("REB"))
    ast_ = safe_float(r.get("AST"))
    stl = safe_float(r.get("STL"))
    blk = safe_float(r.get("BLK"))

    fgm = safe_float(r.get("FGM"))
    fga = safe_float(r.get("FGA"))
    fg3m = safe_float(r.get("FG3M"))
    fta = safe_float(r.get("FTA"))

    # eFG% & TS%
    efg = (fgm + 0.5 * fg3m) / fga if fga and fga > 0 else None
    ts = pts / (2 * (fga + 0.44 * fta)) if pts and (fga or fta) and (fga + 0.44 * fta) > 0 else None

    # Per-36 (Expanded)
    p36 = r36 = a36 = s36 = b36 = None
    if min_ and min_ > 0:
        factor = 36.0 / min_
        p36 = pts * factor if pts is not None else None
        r36 = reb * factor if reb is not None else None
        a36 = ast_ * factor if ast_ is not None else None
        s36 = stl * factor if stl is not None else None
        b36 = blk * factor if blk is not None else None

    return ts, efg, p36, r36, a36, s36, b36


def parse_listish_cell(cell):
    """Handles CSV columns like: "['2011-12']" """
    if cell is None: return []
    cell = str(cell).strip()
    if not cell: return []
    try:
        val = ast.literal_eval(cell)
        return val if isinstance(val, list) else []
    except Exception:
        return []


# ---------- loaders ----------
def load_player_season_stats(cur):
    rows = json.loads(STATS_JSON_PATH.read_text())
    cur.execute("""
    CREATE TABLE player_season_stats (
        player_id INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        team_id INTEGER,
        team_abbreviation TEXT,
        season TEXT NOT NULL,
        age REAL,
        gp INTEGER,
        w INTEGER,
        l INTEGER,
        w_pct REAL,
        min REAL,
        fgm REAL, fga REAL, fg_pct REAL,
        fg3m REAL, fg3a REAL, fg3_pct REAL,
        ftm REAL, fta REAL, ft_pct REAL,
        oreb REAL, dreb REAL, reb REAL,
        ast REAL, tov REAL,
        stl REAL, blk REAL, pf REAL,
        pts REAL, plus_minus REAL,
        ts_pct REAL, efg_pct REAL,
        pts_per36 REAL, reb_per36 REAL, ast_per36 REAL,
        stl_per36 REAL, blk_per36 REAL,
        PRIMARY KEY (player_id, season)
    );
    """)

    sql = "INSERT INTO player_season_stats VALUES (" + ",".join(["?"] * 37) + ")"
    for r in rows:
        m = compute_metrics(r)
        cur.execute(sql, [
            r["PLAYER_ID"], r["PLAYER_NAME"], r["TEAM_ID"], r["TEAM_ABBREVIATION"], r["SEASON"],
            safe_float(r.get("AGE")), r.get("GP"), r.get("W"), r.get("L"), safe_float(r.get("W_PCT")),
            safe_float(r.get("MIN")), safe_float(r.get("FGM")), safe_float(r.get("FGA")), safe_float(r.get("FG_PCT")),
            safe_float(r.get("FG3M")), safe_float(r.get("FG3A")), safe_float(r.get("FG3_PCT")),
            safe_float(r.get("FTM")), safe_float(r.get("FTA")), safe_float(r.get("FT_PCT")),
            safe_float(r.get("OREB")), safe_float(r.get("DREB")), safe_float(r.get("REB")),
            safe_float(r.get("AST")), safe_float(r.get("TOV")), safe_float(r.get("STL")), 
            safe_float(r.get("BLK")), safe_float(r.get("PF")), safe_float(r.get("PTS")), 
            safe_float(r.get("PLUS_MINUS")), *m
        ])


def load_modern_players(cur):
    """Loads players table from data/modern_nba_players.json."""
    if not MODERN_PLAYERS_PATH.exists(): return
    data = json.loads(MODERN_PLAYERS_PATH.read_text())
    cur.execute("CREATE TABLE modern_players (player_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, is_active INTEGER);")
    for p in data:
        cur.execute("INSERT OR IGNORE INTO modern_players VALUES (?,?,?)", 
                    (p.get("id"), p.get("full_name"), p.get("is_active")))


def load_player_missing_seasons(cur):
    """Parses the CSV into a JSON-string storage table."""
    if not MISSING_CSV_PATH.exists(): return
    cur.execute("CREATE TABLE player_missing_seasons (player_name TEXT, seasons_json TEXT, missing_seasons_json TEXT);")
    with MISSING_CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("INSERT INTO player_missing_seasons VALUES (?,?,?);",
                        (row.get("PLAYER_NAME"), 
                         json.dumps(parse_listish_cell(row.get("SEASONS"))), 
                         json.dumps(parse_listish_cell(row.get("MISSING_SEASONS")))))


def load_draft_history(cur):
    if not DRAFT_JSON_PATH.exists(): return
    rows = json.loads(DRAFT_JSON_PATH.read_text())
    cur.execute("""CREATE TABLE draft_history (
        PERSON_ID INTEGER, 
        PLAYER_NAME TEXT, 
        SEASON TEXT, 
        ROUND_NUMBER INTEGER,
        ROUND_PICK INTEGER,
        OVERALL_PICK INTEGER, 
        DRAFT_TYPE TEXT,
        TEAM_ID INTEGER,
        TEAM_CITY TEXT,
        TEAM_NAME TEXT,
        TEAM_ABBREVIATION TEXT, 
        ORGANIZATION TEXT,
        ORGANIZATION_TYPE TEXT,
        PLAYER_PROFILE_FLAG INTEGER
    );""")
    for r in rows:
        cur.execute("""INSERT INTO draft_history VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""", (
            r.get("PERSON_ID"), 
            r.get("PLAYER_NAME"), 
            r.get("SEASON"),
            r.get("ROUND_NUMBER"),
            r.get("ROUND_PICK"),
            r.get("OVERALL_PICK"),
            r.get("DRAFT_TYPE"),
            r.get("TEAM_ID"),
            r.get("TEAM_CITY"),
            r.get("TEAM_NAME"),
            r.get("TEAM_ABBREVIATION"), 
            r.get("ORGANIZATION"),
            r.get("ORGANIZATION_TYPE"),
            r.get("PLAYER_PROFILE_FLAG")
        ))


def main():
    if DB_PATH.exists(): DB_PATH.unlink()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    print(f"🚀 Rebuilding Database: {DB_PATH}")
    load_player_season_stats(cur)
    load_modern_players(cur)
    load_player_missing_seasons(cur)
    load_draft_history(cur)
    
    con.commit()
    print("✅ All tables loaded successfully.")
    con.close()

if __name__ == "__main__":
    main()
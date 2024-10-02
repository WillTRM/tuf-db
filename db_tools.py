# https://gitlab.com/Screwtapello/sqlite-schema-diagram
import pandas as pd
import sqlite3

# connection to db
con = sqlite3.connect("tuf.db", autocommit = True)

# make database cursor
cur = con.cursor()

# remove me
def kill_everyone():
    cur.execute("DELETE FROM chart")
    con.commit()

# add new difficulty to difficulty list (probably will never be used)
def add_difficulty(diff_id, pgu, legacy):
    cur.execute("INSERT INTO difficulty (id, diff, legacy) VALUES (?, ?, ?)", (diff_id, pgu, legacy))
    con.commit()

# remove difficulty from difficulty list
def remove_difficulty(id):
    cur.execute("DELETE FROM difficulty WHERE id = (?)", (id,))
    con.commit()

# can this be shortened
def add_chart(chart_id, song, artist, charter, vfxer, team, video, dl, workshop, diff, real_diff, requester_diff, score, comment):
    cur.execute("INSERT INTO chart (id, song, artist, charter, vfxer, team, video, dl, workshop, diff, real_diff, requester_diff, score, comments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (chart_id, song, artist, charter, vfxer, team, video, dl, workshop, diff, real_diff, requester_diff, score, comment))
    con.commit()

# CAN THIS BE SHORTENED
# basically just, read in tuf csv (MUST BE LEVELS BY DEV), reorder it, then read it directly in
def add_charts_from_file(file):
    df = pd.read_csv(file)
    df.drop(["low diff", "Diff API Num", "Creator(s)", "Difficulty", "Legacy", "LegacyNum", "Diff Num", "realDiff", "clear", "cleared?", "cleared U", "16K", "Mixed", "starred", "M.Diff"], axis = 1, inplace = True)
    df = df[["F", "Song", "Artist(s)", "charter", "vfx", "team", "Raw Video Link", "Raw DL Link", "Raw Workshop Link", "diff num new", "real diff new", "requester FR", "base score", "public comments"]]
    df.columns = ["id", "song", "artist", "charter", "vfxer", "team", "video", "dl", "workshop", "diff", "real_diff", "requester_diff", "score", "comment"]
    df.to_sql("chart", con = con, if_exists = "append", index = False, method = "multi")
    con.commit()

def add_players_from_file(file):
    df = pd.read_csv(file)
    df.drop(["player name (formula)", "country", "banned?", "country code data", "flag"], axis = 1, inplace = True)
    df = df[["Fvotg", "Country", "TGS", "TRS", "TWFS", "TPPS", "isbanned"]]
    df.columns = ["name", "country", "general_score", "ranked_score", "wf_score", "pp_score", "banned"]
    df["banned"] = df["banned"].astype(int)
    df.to_sql("player", con = con, if_exists = "append", index = False, method = "multi")
    con.commit()

def add_clears_from_file(file):
    df = pd.read_csv(file)
    df.drop([])

add_players_from_file("players.csv")

#def add_clears_from_file(file):
#file = "gsheetsdl.csv"
#add_charts_from_file(file)
con.close()
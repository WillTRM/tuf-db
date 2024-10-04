# https://gitlab.com/Screwtapello/sqlite-schema-diagram
import pandas as pd
import sqlite3
import formulae

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
    print("Adding charts...")
    df = pd.read_csv(file)
    df.drop(["low diff", "Diff API Num", "Creator(s)", "Difficulty", "Legacy", "LegacyNum", "Diff Num", "realDiff", "clear", "cleared?", "cleared U", "16K", "Mixed", "starred", "M.Diff"], axis = 1, inplace = True)
    df = df[["F", "Song", "Artist(s)", "charter", "vfx", "team", "Raw Video Link", "Raw DL Link", "Raw Workshop Link", "diff num new", "real diff new", "requester FR", "base score", "public comments"]]
    df.columns = ["id", "song", "artist", "charter", "vfxer", "team", "video", "dl", "workshop", "diff", "real_diff", "requester_diff", "score", "comment"]
    df.to_sql("chart", con = con, if_exists = "append", index = False, method = "multi")

    cur.execute("DELETE from chart WHERE diff IS null")

    con.commit()
    print("Charts added")

def add_players_from_file(file):
    print("Adding players...")
    df = pd.read_csv(file)
    df.drop(["player name (formula)", "country", "banned?", "country code data", "flag"], axis = 1, inplace = True)
    df = df[["Fvotg", "Country", "TGS", "TRS", "TWFS", "TPPS", "isbanned"]]
    df.columns = ["name", "country", "general_score", "ranked_score", "wf_score", "pp_score", "banned"]
    df["banned"] = df["banned"].astype(int)
    df.to_sql("player", con = con, if_exists = "append", index = False, method = "multi")
    con.commit()
    print("Players added")

# adds clears and calculates accuracy for each one
# split accuracy into own function later
def add_clears_from_file(file):
    print("Adding clears...")
    df = pd.read_csv(file)
    df.drop(["12K", "server message", "wf indicator", "Published Date", "Publish Date (GMT)", "hehe funny id go brrrr", "WF score", "PP score", "Late!!", "Xacc", "Score"], axis = 1, inplace = True)
    df = df[["Pid", "id", "*/Speed Trial", "Passer", "Feeling Difficulty", "Title", "*/Raw Video ID", "*/Raw Time (GMT)", "Early!!", "Early!", "EPerfect!", "Perfect!", "LPerfect!", "Late! ", "NHT", "isLegacy"]]
    df.columns = ["id", "chart_id", "speed", "player_id", "fr", "title", "video", "time", "too_early", "early", "eperfect", "perfect", "lperfect", "late", "no_hold", "legacy"]
    df.to_sql("clear", con = con, if_exists = "append", index = False, method = "multi")
    cur.execute("UPDATE clear SET player_id = (SELECT id FROM player WHERE player.name = clear.player_id)")
    
    cur.execute("DELETE from clear WHERE chart_id IS null")

    print("Calculating accuracies...")
    cur.execute("SELECT too_early, early, eperfect, perfect, lperfect, late FROM clear")
    judgements = cur.fetchall()
    cur.execute("SELECT id FROM clear")
    ids = cur.fetchall()
    for chart_index, judgement_index in zip(ids, judgements):
        try:
            acc = formulae.calculate_accuracy(*judgement_index)
            cur.execute("UPDATE clear SET acc = (?) WHERE id = (?)", (acc, *chart_index))
        except TypeError:
            cur.execute("UPDATE clear SET acc = 95.0 WHERE id = (?)", (chart_index[0],))
        if chart_index[0] % 500 == 0:
            print(f"{chart_index[0]} rows calculated")

    # lists cleaned from list of one element tuples to list
    print("Calculating scores...")
    cur.execute("SELECT id, chart_id FROM clear")
    ids = cur.fetchall()
    # INFO! | id_stuff[0] = clear id, id_stuff[1] = chart_id
    for id_stuff in ids:
        cur.execute("SELECT id FROM chart WHERE id = (?)", (id_stuff[1],))
        if cur.fetchone() is None:
            pp = 0
        else:
            cur.execute("SELECT score FROM chart WHERE id = (?)", (id_stuff[1],))
            score_base = cur.fetchone()[0]
            cur.execute("SELECT acc FROM clear WHERE id = (?)", (id_stuff[0],))
            xacc = cur.fetchone()[0]
            cur.execute("SELECT speed FROM clear WHERE id = (?)", (id_stuff[0],))
            speed = cur.fetchone()[0]
            cur.execute("SELECT too_early FROM clear WHERE id = (?)", (id_stuff[0],))
            no_miss = cur.fetchone()[0]
            no_miss = True if (no_miss == 0) else False
            pp = formulae.calculate_pp(score_base, xacc, speed, no_miss)
        cur.execute("UPDATE clear SET pp = (?) WHERE id = (?)", (pp, id_stuff[0]))

        if id_stuff[0] % 500 == 0:
            print(f"{id_stuff[0]} rows calculated")

    con.commit()
    print("Clears added")

def generate_full_table(chart_file, clear_file, player_file):
    cur.execute("DELETE FROM clear")
    cur.execute("DELETE FROM player")
    cur.execute("DELETE FROM chart")
    con.commit()

    add_charts_from_file(chart_file)
    add_players_from_file(player_file)
    add_clears_from_file(clear_file)
    print("Done!")

# ----------------

# connection to db
con = sqlite3.connect("tuf.db", autocommit = True)
# make database cursor
cur = con.cursor()

generate_full_table("charts.csv", "clears.csv", "players.csv")

con.close()
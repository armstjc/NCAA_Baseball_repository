import os
import glob
from tqdm import tqdm
import pandas as pd

# import numpy as np
from multiprocessing import Pool


def reader(filename):
    # print(f'Reading in {filename}')
    return pd.read_csv(filename, encoding="latin-1")


def mergeFilesMultithreaded(filePath=""):
    # global filecount
    # filecount = 0
    num_cpus = os.cpu_count()
    print(f"{num_cpus} cpu cores advalible to this script.")

    if num_cpus <= 2:
        pass
    else:
        num_cpus -= 1

    pool = Pool(num_cpus)
    main_df = pd.DataFrame()

    file_list = glob.iglob(filePath + "/*csv")
    file_list = list(file_list)
    df_list = pool.map(reader, tqdm(file_list))

    main_df = pd.concat(df_list)

    return main_df


def merge_batting_stats():
    print("Starting the merge of Batting CSVs.")
    f = f"PlayerStats/batting"
    print("Collecting downloaded CSVs.")
    df = mergeFilesMultithreaded(os.path.abspath(f))
    print("Done!")
    max_season = df["season"].max()
    min_season = df["season"].min()
    for i in range(min_season, max_season + 1):
        print(f"Saving off the batting stats for {i}.")
        s_df = df[df["season"] == i]
        s_df.to_parquet(
            f"game_stats/player/batting_game_stats/parquet/{i}_batting.parquet"
        )

        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df : 2 * len_s_df]
        partThree = s_df.iloc[2 * len_s_df : 3 * len_s_df]
        partFour = s_df.iloc[3 * len_s_df :]

        partOne.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_01.csv", index=False
        )
        partTwo.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_02.csv", index=False
        )
        partThree.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_03.csv", index=False
        )
        partFour.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_04.csv", index=False
        )


def merge_pitching_stats():
    print("Starting the merge of Pitching CSVs.")
    f = f"PlayerStats/pitching"
    print("Collecting downloaded CSVs.")
    df = mergeFilesMultithreaded(os.path.abspath(f))

    #
    #

    max_season = df["season"].max()
    min_season = df["season"].min()

    for i in range(min_season, max_season + 1):
        print(f"Saving off the pitching stats for {i}.")
        s_df = df[df["season"] == i]
        s_df.to_parquet(
            f"game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet"
        )

        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df : 2 * len_s_df]
        partThree = s_df.iloc[2 * len_s_df : 3 * len_s_df]
        partFour = s_df.iloc[3 * len_s_df :]

        partOne.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_01.csv",
            index=False,
        )
        partTwo.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_02.csv",
            index=False,
        )
        partThree.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_03.csv",
            index=False,
        )
        partFour.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_04.csv",
            index=False,
        )


def merge_fielding_stats():
    print("Starting the merge of Fielding CSVs.")
    f = f"PlayerStats/fielding"
    print("Collecting downloaded CSVs.")
    df = mergeFilesMultithreaded(os.path.abspath(f))
    max_season = df["season"].max()
    min_season = df["season"].min()
    for i in range(min_season, max_season + 1):
        print(f"Saving off the fielding stats for {i}.")
        s_df = df[df["season"] == i]
        s_df.to_parquet(
            f"game_stats/player/fielding_game_stats/parquet/{i}_fielding.parquet"
        )

        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df : 2 * len_s_df]
        partThree = s_df.iloc[2 * len_s_df : 3 * len_s_df]
        partFour = s_df.iloc[3 * len_s_df :]

        partOne.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_01.csv",
            index=False,
        )
        partTwo.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_02.csv",
            index=False,
        )
        partThree.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_03.csv",
            index=False,
        )
        partFour.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_04.csv",
            index=False,
        )


def merge_rosters():
    f = f"TeamRosters/teams"
    df = mergeFilesMultithreaded(os.path.abspath(f))
    max_season = df["season"].max()
    min_season = df["season"].min()
    for i in range(min_season, max_season + 1):
        print(f"Saving off the rosters for {i}.")
        s_df = df[df["season"] == i]
        s_df.to_csv(f"TeamRosters/{i}_roster.csv", index=False)


def add_divisions():

    for i in tqdm(range(2013, 2024)):
        print(f"Adding NCAA Divisions to the {i} batting stats.")
        df = pd.read_parquet(
            f"game_stats/player/batting_game_stats/parquet/{i}_batting.parquet"
        )
        try:
            df = df.drop(columns=["division"])
        except:
            print("\n[division] not found in this season's batting stats.")
        # try:
        #     df = df.rename(columns={'Pitches':'pitches'})
        # except:
        #     print('[Pitches] column is properly named.')

        try:
            df = df.filter(
                items=[
                    "R",
                    "AB",
                    "H",
                    "2B",
                    "3B",
                    "TB",
                    "HR",
                    "RBI",
                    "BB",
                    "HBP",
                    "SF",
                    "SH",
                    "K",
                    "DP",
                    "CS",
                    "Picked",
                    "SB",
                    "IBB",
                    "Pitches",
                    "date",
                    "field",
                    "season_id",
                    "opponent_id",
                    "opponent_name",
                    "innings_played",
                    "extras",
                    "runs_scored",
                    "runs_allowed",
                    "run_difference",
                    "result",
                    "game_id",
                    "school_id",
                    "stats_player_seq",
                    "season",
                    "OPP DP",
                    "GDP",
                    "score",
                ]
            )
        except:
            print("Dataframe was ready for divisions to be added.")

        # df['PI'] = df['Pitches'].astype(int)
        roster_df = pd.read_csv(f"TeamRosters/{i}_roster.csv")
        roster_df = roster_df.filter(items=["player_id", "division"])
        roster_df = roster_df.dropna()
        roster_df = roster_df.drop_duplicates()
        # players_dict = pd.Series(roster_df.player_id,index=roster_df.season_id).to_dict()
        # df['division'] = df['stats_player_seq'].map(players_dict)
        df = df.merge(roster_df, left_on="stats_player_seq", right_on="player_id")
        # roster_df = roster_df.astype({'division':'int32'})
        del roster_df
        df.to_parquet(
            f"game_stats/player/batting_game_stats/parquet/{i}_batting.parquet"
        )
        len_df = len(df)
        len_df = len_df // 4
        partOne = df.iloc[:len_df]
        partTwo = df.iloc[len_df : 2 * len_df]
        partThree = df.iloc[2 * len_df : 3 * len_df]
        partFour = df.iloc[3 * len_df :]
        del df
        partOne.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_01.csv", index=False
        )
        partTwo.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_02.csv", index=False
        )
        partThree.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_03.csv", index=False
        )
        partFour.to_csv(
            f"game_stats/player/batting_game_stats/csv/{i}_batting_04.csv", index=False
        )
        del partOne, partTwo, partThree, partFour

    for i in tqdm(range(2013, 2024)):
        print(f"Adding NCAA Divisions to the {i} pitching stats.")
        df = pd.read_parquet(
            f"game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet"
        )
        try:
            df = df.drop(columns=["division"])
        except:
            print("\n[division] not found in this season's pitching stats.")
        try:
            df = df.filter(
                items=[
                    "App",
                    "GS",
                    "IP",
                    "CG",
                    "H",
                    "R",
                    "ER",
                    "BB",
                    "SO",
                    "SHO",
                    "BF",
                    "P-OAB",
                    "2B-A",
                    "3B-A",
                    "Bk",
                    "HR-A",
                    "WP",
                    "HB",
                    "IBB",
                    "Inh Run",
                    "Inh Run Score",
                    "SHA",
                    "SFA",
                    "PI",
                    "GO",
                    "FO",
                    "W",
                    "L",
                    "SV",
                    "OrdAppeared",
                    "KL",
                    "date",
                    "field",
                    "season_id",
                    "opponent_id",
                    "opponent_name",
                    "innings_played",
                    "extras",
                    "runs_scored",
                    "runs_allowed",
                    "run_difference",
                    "result",
                    "game_id",
                    "school_id",
                    "stats_player_seq",
                    "season",
                    "pickoffs",
                    "score",
                    "player_id",
                ]
            )
        except:
            print("Dataframe was ready for divisions to be added.")

        roster_df = pd.read_csv(f"TeamRosters/{i}_roster.csv")
        roster_df = roster_df.filter(items=["player_id", "division"])
        roster_df = roster_df.dropna()
        roster_df = roster_df.drop_duplicates()
        # players_dict = pd.Series(roster_df.player_id,index=roster_df.season_id).to_dict()
        # df['division'] = df['stats_player_seq'].map(players_dict)
        df = df.merge(roster_df, left_on="stats_player_seq", right_on="player_id")
        # roster_df = roster_df.astype({'division':'int32'})
        del roster_df
        df.to_parquet(
            f"game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet"
        )

        len_df = len(df)
        len_df = len_df // 4
        partOne = df.iloc[:len_df]
        partTwo = df.iloc[len_df : 2 * len_df]
        partThree = df.iloc[2 * len_df : 3 * len_df]
        partFour = df.iloc[3 * len_df :]
        del df

        partOne.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_01.csv",
            index=False,
        )
        partTwo.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_02.csv",
            index=False,
        )
        partThree.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_03.csv",
            index=False,
        )
        partFour.to_csv(
            f"game_stats/player/pitching_game_stats/csv/{i}_pitching_04.csv",
            index=False,
        )
        del partOne, partTwo, partThree, partFour

    for i in tqdm(range(2013, 2024)):
        print(f"Adding NCAA Divisions to the {i} fielding stats.")
        df = pd.read_parquet(
            f"game_stats/player/fielding_game_stats/parquet/{i}_fielding.parquet"
        )
        try:
            df = df.drop(columns=["division"])
        except:
            print("\n[division] not found in this season's fielding stats.")
        try:
            df = df.filter(
                items=[
                    "PO",
                    "A",
                    "TC",
                    "E",
                    "CI",
                    "PB",
                    "SBA",
                    "CSB",
                    "IDP",
                    "TP",
                    "date",
                    "field",
                    "season_id",
                    "opponent_id",
                    "opponent_name",
                    "innings_played",
                    "extras",
                    "runs_scored",
                    "runs_allowed",
                    "run_difference",
                    "result",
                    "game_id",
                    "school_id",
                    "stats_player_seq",
                    "season",
                ]
            )
        except:
            print("Dataframe was ready for divisions to be added.")
        roster_df = pd.read_csv(f"TeamRosters/{i}_roster.csv")
        roster_df = roster_df.filter(items=["player_id", "division"])
        roster_df = roster_df.dropna()
        roster_df = roster_df.drop_duplicates()
        # players_dict = pd.Series(roster_df.player_id,index=roster_df.season_id).to_dict()
        # df['division'] = df['stats_player_seq'].map(players_dict)
        df = df.merge(roster_df, left_on="stats_player_seq", right_on="player_id")
        # roster_df = roster_df.astype({'division':'int32'})
        del roster_df
        df.to_parquet(
            f"game_stats/player/fielding_game_stats/parquet/{i}_fielding.parquet"
        )
        len_df = len(df)
        len_df = len_df // 4
        partOne = df.iloc[:len_df]
        partTwo = df.iloc[len_df : 2 * len_df]
        partThree = df.iloc[2 * len_df : 3 * len_df]
        partFour = df.iloc[3 * len_df :]
        del df
        partOne.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_01.csv",
            index=False,
        )
        partTwo.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_02.csv",
            index=False,
        )
        partThree.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_03.csv",
            index=False,
        )
        partFour.to_csv(
            f"game_stats/player/fielding_game_stats/csv/{i}_fielding_04.csv",
            index=False,
        )
        del partOne, partTwo, partThree, partFour


def main():
    print("Starting up.")
    merge_rosters()
    merge_batting_stats()
    merge_pitching_stats()
    merge_fielding_stats()
    add_divisions()


if __name__ == "__main__":
    main()

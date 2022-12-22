import pandas as pd
import numpy as np
#from tqdm import tqdm

def generate_season_batting_stats(season:int):
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    for i in range(1,5):
        #print(i)
        part_df = pd.read_csv(f'PlayerStats/{season}_batting_0{i}.csv')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df
    main_df['G'] = 1
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq'],as_index=False)['G','AB','H','TB','R','2B','3B','HR','RBI','BB','HBP','SF','SH','K','DP','SB','CS','Picked','IBB','OPP DP','GDP'].sum())
    ## Plate Appearances
    finished_df['PA'] = finished_df['AB'] + finished_df['BB'] + finished_df['SF'] + finished_df['SH']
    ## Batting Average
    finished_df['BA'] = finished_df['H'] / finished_df['AB']
    ## On Base Percentage (OBP)
    finished_df['OBP'] = (finished_df['H'] + finished_df['BB'] + finished_df['HBP']) / finished_df['PA']
    ## Slugging Percentae
    finished_df['SLG'] = (finished_df['H'] + (finished_df['2B'] * 2) + (finished_df['3B'] * 3) + (finished_df['HR'] * 4)) / finished_df['AB']
    ## On-Base + Slugging Percentages
    finished_df['OPS'] = finished_df['OBP'] + finished_df['SLG']
    ## Batting Average on balls in play
    finished_df['BAbip'] = (finished_df['H'] - finished_df['HR']) / (finished_df['AB'] - finished_df['K'] - finished_df['HR'] + finished_df['SF'])
    ## Runs scored percentage
    finished_df['RS%'] = (finished_df['R'] - finished_df['HR']) / (finished_df['H'] + finished_df['HBP'] + finished_df['BB'] - finished_df['HR'])
    ## Home Run percentage
    finished_df['HR%'] = finished_df['HR'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['K%'] = finished_df['K'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['BB%'] = finished_df['BB'] / finished_df['PA']
    ## Walks to strikeouts ratio
    finished_df['K-BB%'] = finished_df['K%'] - finished_df['BB%']
    finished_df['BB/K'] = finished_df['BB'] / finished_df['K']
    ## Convert infinates into Null values
    finished_df = finished_df.mask(np.isinf(finished_df))
    #print(finished_df)
    finished_df.to_csv(f'season_stats/player/batting_season_stats/csv/{season}_player_season_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/batting_season_stats/parquet/{season}_player_season_batting.parquet',index=False)

def generate_season_pitching_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    for i in range(1,3):
        #print(i)
        part_df = pd.read_csv(f'PlayerStats/{season}_pitching_0{i}.csv')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df
    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})
    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq'],as_index=False)\
        ['App','IP','H','R','ER','BB','SO','SHO','BF','P-OAB','2B-A','3B-A','Bk','HR-A','WP','HB','IBB','Inh Run','Inh Run Score','SHA','SFA','Pitches','pitches','GO','FO','W','L','SV','KL','CG','pickoffs'].sum())

    finished_df['PI'] = finished_df['Pitches'] + finished_df['pitches']
    finished_df = finished_df.drop(['Pitches','pitches'], axis=1)
    finished_df = finished_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS'})
    ## Win-loss percentage
    finished_df['W-L%'] = finished_df['W'] / (finished_df['W'] + finished_df['L'])
    ## Earned Run Average (ERA)
    finished_df['ERA'] = 9 * (finished_df['ER'] / finished_df['IP'])
    ## Walks and Hits per Inning Pitched (WHIP)
    finished_df['WHIP'] = (finished_df['BB'] + finished_df['H']) / finished_df['IP']
    ## Hits per 9 innings
    finished_df['H9'] = 9 * (finished_df['H'] / finished_df['IP'])
    ## Home Runs per 9 innings
    finished_df['HR9'] = 9 * (finished_df['HR'] / finished_df['IP'])
    ## Walks per 9 innings
    finished_df['BB9'] = 9 * (finished_df['BB'] / finished_df['IP'])
    ## Strikeouts per 9 innings
    finished_df['SO9'] = 9 * (finished_df['SO'] / finished_df['IP'])
    ## Strikeouts/Walks ratio
    finished_df['SO/BB'] = finished_df['SO'] / finished_df['BB']
    ## Runs Allowed per 9 innings pitched (RA9)
    finished_df['RA9'] = 9 * (finished_df['R'] / finished_df['IP'])

    finished_df = finished_df.mask(np.isinf(finished_df))
    #print(finished_df)
    finished_df.to_csv(f'season_stats/player/pitching_season_stats/csv/{season}_player_season_pitching.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/pitching_season_stats/parquet/{season}_player_season_pitching.parquet',index=False)

def generate_season_fielding_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    for i in range(1,5):
        #print(i)
        part_df = pd.read_csv(f'PlayerStats/{season}_fielding_0{i}.csv')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df
    
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq'],as_index=False)\
        ['TC','PO','A','E','CI','PB','SBA','CSB','IDP','TP'].sum())
    ## Fielding Percentage
    finished_df['FLD%'] = (finished_df['PO'] + finished_df['A']) / (finished_df['PO'] + finished_df['A'] + finished_df['E'])
    ## Caught Stealing Percentage
    finished_df['CS%'] = finished_df['CSB'] / finished_df['SBA']
    
    #print(finished_df)
    finished_df = finished_df.mask(np.isinf(finished_df))
    finished_df.to_csv(f'season_stats/player/fielding_season_stats/csv/{season}_player_season_fielding.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/fielding_season_stats/parquet/{season}_player_season_fielding.parquet',index=False)

if __name__ == "__main__":
    for i in range(2016,2023):
        generate_season_fielding_stats(i)

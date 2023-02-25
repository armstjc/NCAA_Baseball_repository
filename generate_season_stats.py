from datetime import datetime
import pandas as pd
import numpy as np
#from tqdm import tqdm

#####################################################################################################################################################################################################################
## League stats
##
#####################################################################################################################################################################################################################

def generate_league_batting_stats():
    print('\nGenerating NCAA league batting stats.')

    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    current_year = int(datetime.now().year)

    for i in range(2013,current_year+1):
        print(f'Loading in the {i} batting stats.')
        part_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{i}_batting.parquet')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df

    main_df['G'] = 1
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','division'],as_index=False)\
        ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())
    
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
    
    ## Isolated power
    finished_df['ISO'] = (finished_df['2B'] + (finished_df['3B'] * 2) + (finished_df['3B'])) / finished_df['AB']

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
    finished_df.to_csv(f'season_stats/league/batting_season_stats/csv/league_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/league/batting_season_stats/parquet/league_batting.parquet',index=False)

def generate_league_pitching_stats():
    print('\nGenerating NCAA league pitching stats.')

    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    current_year = int(datetime.now().year)

    for i in range(2013,current_year+1):
        print(f'Loading in the {i} pitching stats.')
        part_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df

    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})
    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
    print(main_df.columns)
    print(main_df.dtypes)
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','division'],as_index=False)\
        ['App','GS','W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A',\
         'BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA'\
            ,'SFA','GO','FO','KL','pickoffs'].sum())
    
    # finished_df['PI'] = finished_df['Pitches']
    # finished_df = finished_df.drop(['Pitches'], axis=1)

    finished_df = finished_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
    
    ## Win-loss percentage
    finished_df['W-L%'] = finished_df['W'] / (finished_df['W'] + finished_df['L'])
    
    ## Earned Run Average (ERA)
    finished_df['ERA'] = 9 * (finished_df['ER'] / finished_df['IP'])

    ## Fielding Indipendent Pitching (FIP) Constant
    finished_df['FIP_const'] = finished_df['ERA'] - (((13 * finished_df['HR'])+(3 * (finished_df['BB'] + finished_df['HBP']))-(finished_df['SO']))/finished_df['IP'])
    
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
    finished_df.to_csv(f'season_stats/league/pitching_season_stats/csv/league_pitching.csv',index=False)
    finished_df.to_parquet(f'season_stats/league/pitching_season_stats/parquet/league_pitching.parquet',index=False)

#####################################################################################################################################################################################################################
## Park Factors
##
#####################################################################################################################################################################################################################

#####################################################################################################################################################################################################################
## Season Player Stats
##
#####################################################################################################################################################################################################################

def generate_season_player_batting_stats(season:int):
    def parser(division:int,main_df:pd.DataFrame()):
        #main_df = pd.concat([main_df,part_df],ignore_index=True)
        league_df = pd.read_parquet('season_stats/league/batting_season_stats/parquet/league_batting.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_ops = league_df['OPS'].iloc[0]

        main_df['G'] = 1
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq'],as_index=False)\
            ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())

        ## Plate Appearances
        main_df['PA'] = main_df['AB'] + main_df['BB'] + main_df['SF'] + main_df['SH']

        ## Batting Average
        main_df['BA'] = main_df['H'] / main_df['AB']

        ## On Base Percentage (OBP)
        main_df['OBP'] = (main_df['H'] + main_df['BB'] + main_df['HBP']) / main_df['PA']

        ## Slugging Percentae
        main_df['SLG'] = (main_df['H'] + (main_df['2B'] * 2) + (main_df['3B'] * 3) + (main_df['HR'] * 4)) / main_df['AB']

        ## On-Base + Slugging Percentages
        main_df['OPS'] = main_df['OBP'] + main_df['SLG']

        ## OPS+
        main_df['OPS+'] = 100 * (main_df['OPS']/lg_ops)
        main_df['OPS+'] = main_df['OPS+'].round(0)

        ## Isolated power
        main_df['ISO'] = (main_df['2B'] + (main_df['3B'] * 2) + (main_df['3B'])) / main_df['AB']
        main_df['ISO'] = main_df['ISO'].round(3)

        ## Batting Average on balls in play
        main_df['BAbip'] = (main_df['H'] - main_df['HR']) / (main_df['AB'] - main_df['K'] - main_df['HR'] + main_df['SF'])

        ## Runs scored percentage
        main_df['RS%'] = (main_df['R'] - main_df['HR']) / (main_df['H'] + main_df['HBP'] + main_df['BB'] - main_df['HR'])

        ## Home Run percentage
        main_df['HR%'] = main_df['HR'] / main_df['PA']

        ## Strikeout percentage
        main_df['K%'] = main_df['K'] / main_df['PA']

        ## Strikeout percentage
        main_df['BB%'] = main_df['BB'] / main_df['PA']

        ## Walks to strikeouts ratio
        main_df['K-BB%'] = main_df['K%'] - main_df['BB%']
        main_df['BB/K'] = main_df['BB'] / main_df['K']

        ## Convert infinates into Null values
        main_df = main_df.mask(np.isinf(main_df))

        #print(main_df)
        return main_df
    
    finished_df = pd.DataFrame()
    part_df = pd.DataFrame()
    main_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
    
    for i in range(1,4):
        part_df = main_df[main_df['division']==i]
        part_df = parser(i,part_df)
        finished_df = pd.concat([finished_df,part_df],ignore_index=True)

    finished_df.to_csv(f'season_stats/player/batting_season_stats/csv/{season}_player_season_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/batting_season_stats/parquet/{season}_player_season_batting.parquet',index=False)

def generate_season_player_pitching_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    #for i in range(1,5):
    #print(i)

    part_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)

    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})
    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq'],as_index=False)\
        ['App','GS','W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A','BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA','SFA','GO','FO','KL','pickoffs'].sum())

    # finished_df['PI'] = finished_df['Pitches']
    # finished_df = finished_df.drop(['Pitches'], axis=1)

    finished_df = finished_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
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

def generate_season_player_fielding_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/fielding_game_stats/parquet/{season}_fielding.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)
    
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

#####################################################################################################################################################################################################################
## Team Season Stats
##
#####################################################################################################################################################################################################################


def generate_season_team_batting_stats(season:int):
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)

    main_df['G'] = 1
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division'],as_index=False)\
        ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())
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
    finished_df.to_csv(f'season_stats/team/batting_season_stats/csv/{season}_player_season_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/batting_season_stats/parquet/{season}_player_season_batting.parquet',index=False)

def generate_season_team_pitching_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)

    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})
    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division'],as_index=False)\
        ['W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A','BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA','SFA','GO','FO','KL','pickoffs'].sum())
    # df['PI'] = df['Pitches']
    # df = df.drop(['Pitches'], axis=1)

    #finished_df['PI'] = finished_df['Pitches']
    #finished_df = finished_df.drop(['Pitches'], axis=1)

    finished_df = finished_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
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
    finished_df.to_csv(f'season_stats/team/pitching_season_stats/csv/{season}_player_season_pitching.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/pitching_season_stats/parquet/{season}_player_season_pitching.parquet',index=False)

def generate_season_team_fielding_stats(season:int):
    
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/fielding_game_stats/parquet/{season}_fielding.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)
    
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division'],as_index=False)\
        ['TC','PO','A','E','CI','PB','SBA','CSB','IDP','TP'].sum())
    ## Fielding Percentage
    finished_df['FLD%'] = (finished_df['PO'] + finished_df['A']) / (finished_df['PO'] + finished_df['A'] + finished_df['E'])
    ## Caught Stealing Percentage
    finished_df['CS%'] = finished_df['CSB'] / finished_df['SBA']
    
    #print(finished_df)
    finished_df = finished_df.mask(np.isinf(finished_df))
    finished_df.to_csv(f'season_stats/team/fielding_season_stats/csv/{season}_player_season_fielding.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/fielding_season_stats/parquet/{season}_player_season_fielding.parquet',index=False)

#####################################################################################################################################################################################################################
## Team Game Stats
##
#####################################################################################################################################################################################################################

#####################################################################################################################################################################################################################

if __name__ == "__main__":
    current_year = int(datetime.now().year)
    #generate_league_batting_stats()
    generate_league_pitching_stats()

    for i in range(2013,current_year+1):
        print(f'\n\nGenerating stats for the {i} season.\n\n')

        #generate_season_team_batting_stats(i)
        #generate_season_team_pitching_stats(i)

        #generate_season_player_batting_stats(i)
        #generate_season_player_pitching_stats(i)

        # if i >= 2016:
        #     generate_season_team_fielding_stats(i)
        #     generate_season_player_fielding_stats(i)

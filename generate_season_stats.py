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
        print(f'\tLoading in the {i} batting stats.')
        part_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{i}_batting.parquet')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df

    main_df['G'] = 1
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','division'],as_index=False)\
        ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())
    
    ## Runs Created (Technical version)
    finished_df['RC'] = ((finished_df['H'] + finished_df['BB'] - finished_df['CS'] + finished_df['HBP'] - finished_df['GDP']) * (finished_df['TB'] + (0.26 * (finished_df['BB'] - finished_df['IBB'] + finished_df['HBP']))) + (0.52 * (finished_df['SH'] + finished_df['SF'] + finished_df['SB']))) / (finished_df['AB'] + finished_df['BB'] + finished_df['HBP'] + finished_df['SH'] + finished_df['SF'])
    finished_df['RC'] = finished_df['RC'].round(3)

    ## Plate Appearances
    finished_df['PA'] = finished_df['AB'] + finished_df['BB'] + finished_df['SF'] + finished_df['SH']
    
    ## Batting Average
    finished_df['BA'] = finished_df['H'] / finished_df['AB']
    finished_df['BA'] = finished_df['BA'].round(3)
    
    ## Secondary Average
    finished_df['SecA'] = (finished_df['BB'] + (finished_df['TB'] - finished_df['H']) + (finished_df['SB'] - finished_df['CS'])) / finished_df['AB']
    finished_df['SecA'] = finished_df['SecA'].round(3)


    ## On Base Percentage (OBP)
    finished_df['OBP'] = (finished_df['H'] + finished_df['BB'] + finished_df['HBP']) / finished_df['PA']
    finished_df['OBP'] = finished_df['OBP'].round(3)

    ## Slugging Percentae
    finished_df['SLG'] = (finished_df['H'] + (finished_df['2B'] * 2) + (finished_df['3B'] * 3) + (finished_df['HR'] * 4)) / finished_df['AB']
    finished_df['SLG'] = finished_df['SLG'].round(3)

    ## On-Base + Slugging Percentages
    finished_df['OPS'] = finished_df['OBP'] + finished_df['SLG']
    finished_df['OPS'] = finished_df['OPS'].round(3)

    ## Isolated power
    finished_df['ISO'] = (finished_df['2B'] + (finished_df['3B'] * 2) + (finished_df['3B'])) / finished_df['AB']
    finished_df['ISO'] = finished_df['ISO'].round(3)

    ## Batting Average on balls in play
    finished_df['BAbip'] = (finished_df['H'] - finished_df['HR']) / (finished_df['AB'] - finished_df['K'] - finished_df['HR'] + finished_df['SF'])
    finished_df['BAbip'] = finished_df['BAbip'].round(3)
        
    ## eXtrapolated Runs 
    finished_df['XR'] = (finished_df['H'] * 0.5) + (finished_df['2B'] * 0.72) + (finished_df['3B'] * 1.04) + (finished_df['HR'] * 1.44) + (0.34 *(finished_df['HBP'] + finished_df['TB'] + finished_df['IBB'])) + (0.25 * finished_df['IBB']) + (0.18 * finished_df['SB']) + (-0.32 * finished_df['CS']) + (-0.09 * (finished_df['AB'] - finished_df['H'] - finished_df['K'])) + (-0.098 * finished_df['K']) + (-0.37 * finished_df['GDP']) + (0.37 * finished_df['SF']) + (0.04 * finished_df['SH'])
    finished_df['XR'] = finished_df['XR'].round(3)

    ## eXtrapolated Runs Reduced
    finished_df['XRR'] = (0.5 * finished_df['H']) + (0.72 * finished_df['2B']) + (1.04 * finished_df['3B']) + (1.44 * finished_df['HR']) + (0.33 *(finished_df['HBP'] + finished_df['TB'])) + (0.18 * finished_df['SB']) + (-0.32 * finished_df['CS']) + (-0.098 * (finished_df['AB'] - finished_df['H']))
    finished_df['XRR'] = finished_df['XRR'].round(3)

    ## eXtrapolated Runs Basic
    finished_df['XRB'] = (0.5 * finished_df['H']) + (0.72 * finished_df['2B']) + (1.04 * finished_df['3B']) + (1.44 * finished_df['HR']) + (0.34 *(finished_df['HBP'] + finished_df['TB'])) + (0.18 * finished_df['SB']) - (-0.32 * finished_df['CS']) + (-0.096 * (finished_df['AB'] - finished_df['H']))
    finished_df['XRB'] = finished_df['XRB'].round(3)


    ## Power-Speed Number
    finished_df['PSN'] = (2 * finished_df['HR'] * finished_df['SB']) / (finished_df['HR'] + finished_df['SB'])
    finished_df['PSN'] = finished_df['PSN'].round(1)

    ## Runs scored percentage
    finished_df['RS%'] = (finished_df['R'] - finished_df['HR']) / (finished_df['H'] + finished_df['HBP'] + finished_df['BB'] - finished_df['HR'])
    finished_df['RS%'] = finished_df['RS%'].round(3)

    ## Home Run percentage
    finished_df['HR%'] = finished_df['HR'] / finished_df['PA']
    finished_df['HR%'] = finished_df['HR%'].round(3)

    ## Strikeout percentage
    finished_df['K%'] = finished_df['K'] / finished_df['PA']
    finished_df['K%'] = finished_df['K%'].round(3)

    ## Strikeout percentage
    finished_df['BB%'] = finished_df['BB'] / finished_df['PA']
    finished_df['BB%'] = finished_df['BB%'].round(3)

    ## Walks to strikeouts ratio
    finished_df['K-BB%'] = finished_df['K%'] - finished_df['BB%']
    finished_df['K-BB%'] = finished_df['K-BB%'].round(3)

    finished_df['BB/K'] = finished_df['BB'] / finished_df['K']
    finished_df['BB/K'] = finished_df['BB/K'].round(3)

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
        print(f'\tLoading in the {i} pitching stats.')
        part_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet')
        main_df = pd.concat([main_df,part_df],ignore_index=True)
        del part_df

    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})
    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)

    # print(main_df.columns)
    # print(main_df.dtypes)

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

def generate_park_factors(season:int):
    """
    """
    main_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
    main_df['G'] = 1

    home_gm_df = main_df[main_df['field'] == 'home']
    away_gm_df = main_df[main_df['field'] != 'home']

    home_gm_df = pd.DataFrame(home_gm_df.groupby(['season','season_id','school_id','division'],as_index=False)\
        ['runs_scored','runs_allowed','G','AB','H','2B','3B','HR','BB','K'].sum())
    home_gm_df = home_gm_df.rename({'runs_scored':'home_runs_scored','runs_allowed':'home_runs_allowed','G':'home_G','AB':'home_AB','R':'home_R','2B':'home_2B','3B':'home_3B','HR':'home_HR','BB':'home_BB','K':'home_K'},axis=1)
    
    away_gm_df = pd.DataFrame(away_gm_df.groupby(['season','season_id','school_id','division'],as_index=False)\
        ['runs_scored','runs_allowed','G','AB','H','2B','3B','HR','BB','K'].sum())
    away_gm_df = away_gm_df.rename({'runs_scored':'away_runs_scored','runs_allowed':'away_runs_allowed','G':'away_G','AB':'away_AB','R':'away_R','2B':'away_2B','3B':'away_3B','HR':'away_HR','BB':'away_BB','K':'away_K'},axis=1)

    finished_df = pd.merge(home_gm_df,away_gm_df,left_on=['season','season_id','school_id','division'],right_on=['season','season_id','school_id','division'],how='left')
    
    finished_df['PF'] = 100 * (((finished_df['home_runs_scored'] + finished_df['home_runs_allowed']) / finished_df['home_G']) / ((finished_df['away_runs_scored'] + finished_df['away_runs_allowed']) / finished_df['away_G']))
    
    print(finished_df)
    finished_df.to_csv(f'season_stats/league/park_factors/csv/{season}_park_factors.csv',index=False)
    finished_df.to_parquet(f'season_stats/league/park_factors/parquet/{season}_park_factors.parquet',index=False)
    # return finished_df

#####################################################################################################################################################################################################################
## Season Player Stats
##
#####################################################################################################################################################################################################################

def generate_season_player_batting_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        #main_df = pd.concat([main_df,part_df],ignore_index=True)
        league_df = pd.read_parquet('season_stats/league/batting_season_stats/parquet/league_batting.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_ops = league_df['OPS'].iloc[0]
        lg_slg = league_df['SLG'].iloc[0]
        main_df['G'] = 1
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq','division'],as_index=False)\
            ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())

        ## Runs Created (Technical version)
        main_df['RC'] = ((main_df['H'] + main_df['BB'] - main_df['CS'] + main_df['HBP'] - main_df['GDP']) * (main_df['TB'] + (0.26 * (main_df['BB'] - main_df['IBB'] + main_df['HBP']))) + (0.52 * (main_df['SH'] + main_df['SF'] + main_df['SB']))) / (main_df['AB'] + main_df['BB'] + main_df['HBP'] + main_df['SH'] + main_df['SF'])

        ## Plate Appearances
        main_df['PA'] = main_df['AB'] + main_df['BB'] + main_df['SF'] + main_df['SH']

        ## Batting Average
        main_df['BA'] = main_df['H'] / main_df['AB']
        main_df['BA'] = main_df['BA'].round(3)
        
        ## Secondary Average
        main_df['SecA'] = (main_df['BB'] + (main_df['TB'] - main_df['H']) + (main_df['SB'] - main_df['CS'])) / main_df['AB']
        main_df['SecA'] = main_df['SecA'].round(3)
                                            
        ## On Base Percentage (OBP)
        main_df['OBP'] = (main_df['H'] + main_df['BB'] + main_df['HBP']) / main_df['PA']
        main_df['OBP'] = main_df['OBP'].round(3)

        ## Slugging Percentae
        main_df['SLG'] = (main_df['H'] + (main_df['2B'] * 2) + (main_df['3B'] * 3) + (main_df['HR'] * 4)) / main_df['AB']
        main_df['SLG'] = main_df['SLG'].round(3)

        ## On-Base + Slugging Percentages
        main_df['OPS'] = main_df['OBP'] + main_df['SLG']
        main_df['OPS'] = main_df['OPS'].round(3)

        ## OPS+
        main_df['OPS+'] = 100 * ((main_df['OPS']/lg_ops) + (main_df['SLG']/lg_slg) -1)
        main_df['OPS+'] = main_df['OPS+'].round(0)

        ## Isolated power
        main_df['ISO'] = (main_df['2B'] + (main_df['3B'] * 2) + (main_df['3B'])) / main_df['AB']
        main_df['ISO'] = main_df['ISO'].round(3)

        ## Batting Average on balls in play
        main_df['BAbip'] = (main_df['H'] - main_df['HR']) / (main_df['AB'] - main_df['K'] - main_df['HR'] + main_df['SF'])
        main_df['BAbip'] = main_df['BAbip'].round(3)

        ## eXtrapolated Runs 
        main_df['XR'] = (main_df['H'] * 0.5) + (main_df['2B'] * 0.72) + (main_df['3B'] * 1.04) + (main_df['HR'] * 1.44) + (0.34 *(main_df['HBP'] + main_df['TB'] + main_df['IBB'])) + (0.25 * main_df['IBB']) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.09 * (main_df['AB'] - main_df['H'] - main_df['K'])) + (-0.098 * main_df['K']) + (-0.37 * main_df['GDP']) + (0.37 * main_df['SF']) + (0.04 * main_df['SH'])
        main_df['XR'] = main_df['XR'].round(3)
        
        ## eXtrapolated Runs Reduced
        main_df['XRR'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.33 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.098 * (main_df['AB'] - main_df['H']))
        main_df['XRR'] = main_df['XRR'].round(3)

        ## eXtrapolated Runs Basic
        main_df['XRB'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.34 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) - (-0.32 * main_df['CS']) + (-0.096 * (main_df['AB'] - main_df['H']))
        main_df['XRB'] = main_df['XRB'].round(3)

        ## Power-Speed Number
        main_df['PSN'] = (2 * main_df['HR'] * main_df['SB']) / (main_df['HR'] + main_df['SB'])
        main_df['PSN'] = main_df['PSN'].round(1)

        ## Runs scored percentage
        main_df['RS%'] = (main_df['R'] - main_df['HR']) / (main_df['H'] + main_df['HBP'] + main_df['BB'] - main_df['HR'])
        main_df['RS%'] = main_df['RS%'].round(3)

        ## Home Run percentage
        main_df['HR%'] = main_df['HR'] / main_df['PA']
        main_df['HR%'] = main_df['HR%'].round(3)

        ## Strikeout percentage
        main_df['K%'] = main_df['K'] / main_df['PA']
        main_df['K%'] = main_df['K%'].round(3)

        ## Strikeout percentage
        main_df['BB%'] = main_df['BB'] / main_df['PA']
        main_df['BB%'] = main_df['BB%'].round(3)

        ## Walks to strikeouts ratio
        main_df['K-BB%'] = main_df['K%'] - main_df['BB%']
        main_df['K-BB%'] = main_df['K-BB%'].round(3)

        main_df['BB/K'] = main_df['BB'] / main_df['K']
        main_df['BB/K'] = main_df['BB/K'].round(3)

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

    del part_df,main_df

    finished_df.to_csv(f'season_stats/player/batting_season_stats/csv/{season}_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/batting_season_stats/parquet/{season}_batting.parquet',index=False)

def generate_season_player_pitching_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        league_df = pd.read_parquet('season_stats/league/pitching_season_stats/parquet/league_pitching.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_era = league_df['ERA'].iloc[0]
        fip_const = league_df['FIP_const'].iloc[0]

        main_df['App'] = 1
        main_df = main_df.astype({'IP':'string'})
        main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
        main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
        main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq','division'],as_index=False)\
            ['App','GS','W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A','BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA','SFA','GO','FO','KL','pickoffs'].sum())

        # main_df['PI'] = main_df['Pitches']
        # main_df = main_df.drop(['Pitches'], axis=1)

        main_df = main_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
        
        ## Win-loss percentage
        main_df['W-L%'] = main_df['W'] / (main_df['W'] + main_df['L'])
        
        ## Earned Run Average (ERA)
        main_df['ERA'] = 9 * (main_df['ER'] / main_df['IP'])

        ## Earned Run Average (ERA)
        main_df['ERA+'] = 100 * (lg_era / main_df['ERA'])
        main_df['ERA+'] = main_df['ERA+'].round(0)
        
        ## Field Indipendent Pitching
        main_df['FIP'] = (((13 * main_df['HR']) + (3 * (main_df['BB'] + main_df['HBP'])) - (2 * main_df['SO']))/main_df['IP']) + fip_const

        ## Walks and Hits per Inning Pitched (WHIP)
        main_df['WHIP'] = (main_df['BB'] + main_df['H']) / main_df['IP']
        
        ## Hits per 9 innings
        main_df['H9'] = 9 * (main_df['H'] / main_df['IP'])
        
        ## Home Runs per 9 innings
        main_df['HR9'] = 9 * (main_df['HR'] / main_df['IP'])
        
        ## Walks per 9 innings
        main_df['BB9'] = 9 * (main_df['BB'] / main_df['IP'])
        
        ## Strikeouts per 9 innings
        main_df['SO9'] = 9 * (main_df['SO'] / main_df['IP'])
        
        ## Strikeouts/Walks ratio
        main_df['SO/BB'] = main_df['SO'] / main_df['BB']
        
        ## Runs Allowed per 9 innings pitched (RA9)
        main_df['RA9'] = 9 * (main_df['R'] / main_df['IP'])

        main_df = main_df.mask(np.isinf(main_df))

        return main_df

    finished_df = pd.DataFrame()
    part_df = pd.DataFrame()
    main_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    for i in range(1,4):
        part_df = main_df[main_df['division']==i]
        part_df = parser(i,part_df)
        finished_df = pd.concat([finished_df,part_df],ignore_index=True)

    del part_df, main_df

    #print(finished_df)
    finished_df.to_csv(f'season_stats/player/pitching_season_stats/csv/{season}_pitching.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/pitching_season_stats/parquet/{season}_pitching.parquet',index=False)

def generate_season_player_fielding_stats(season:int):
    """
    """
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/fielding_game_stats/parquet/{season}_fielding.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)
    
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','stats_player_seq','division'],as_index=False)\
        ['TC','PO','A','E','CI','PB','SBA','CSB','IDP','TP'].sum())
    ## Fielding Percentage
    finished_df['FLD%'] = (finished_df['PO'] + finished_df['A']) / (finished_df['PO'] + finished_df['A'] + finished_df['E'])
    ## Caught Stealing Percentage
    finished_df['CS%'] = finished_df['CSB'] / finished_df['SBA']
    
    #print(finished_df)
    finished_df = finished_df.mask(np.isinf(finished_df))
    finished_df.to_csv(f'season_stats/player/fielding_season_stats/csv/{season}_fielding.csv',index=False)
    finished_df.to_parquet(f'season_stats/player/fielding_season_stats/parquet/{season}_fielding.parquet',index=False)

#####################################################################################################################################################################################################################
## Team Season Stats
##
#####################################################################################################################################################################################################################

def generate_season_team_batting_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        #main_df = pd.concat([main_df,part_df],ignore_index=True)
        league_df = pd.read_parquet('season_stats/league/batting_season_stats/parquet/league_batting.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_ops = league_df['OPS'].iloc[0]
        lg_slg = league_df['SLG'].iloc[0]

        main_df['G'] = 1
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division'],as_index=False)\
            ['G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())

        ## Runs Created (Technical version)
        main_df['RC'] = ((main_df['H'] + main_df['BB'] - main_df['CS'] + main_df['HBP'] - main_df['GDP']) * (main_df['TB'] + (0.26 * (main_df['BB'] - main_df['IBB'] + main_df['HBP']))) + (0.52 * (main_df['SH'] + main_df['SF'] + main_df['SB']))) / (main_df['AB'] + main_df['BB'] + main_df['HBP'] + main_df['SH'] + main_df['SF'])

        ## Plate Appearances
        main_df['PA'] = main_df['AB'] + main_df['BB'] + main_df['SF'] + main_df['SH']

        ## Batting Average
        main_df['BA'] = main_df['H'] / main_df['AB']
        main_df['BA'] = main_df['BA'].round(3)
        
        ## Secondary Average
        main_df['SecA'] = (main_df['BB'] + (main_df['TB'] - main_df['H']) + (main_df['SB'] - main_df['CS'])) / main_df['AB']
        main_df['SecA'] = main_df['SecA'].round(3)
                                            
        ## On Base Percentage (OBP)
        main_df['OBP'] = (main_df['H'] + main_df['BB'] + main_df['HBP']) / main_df['PA']
        main_df['OBP'] = main_df['OBP'].round(3)

        ## Slugging Percentae
        main_df['SLG'] = (main_df['H'] + (main_df['2B'] * 2) + (main_df['3B'] * 3) + (main_df['HR'] * 4)) / main_df['AB']
        main_df['SLG'] = main_df['SLG'].round(3)

        ## On-Base + Slugging Percentages
        main_df['OPS'] = main_df['OBP'] + main_df['SLG']
        main_df['OPS'] = main_df['OPS'].round(3)

        ## OPS+
        main_df['OPS+'] = 100 * ((main_df['OPS']/lg_ops) + (main_df['SLG']/lg_slg) -1)
        main_df['OPS+'] = main_df['OPS+'].round(0)

        ## Isolated power
        main_df['ISO'] = (main_df['2B'] + (main_df['3B'] * 2) + (main_df['3B'])) / main_df['AB']
        main_df['ISO'] = main_df['ISO'].round(3)

        ## Batting Average on balls in play
        main_df['BAbip'] = (main_df['H'] - main_df['HR']) / (main_df['AB'] - main_df['K'] - main_df['HR'] + main_df['SF'])
        main_df['BAbip'] = main_df['BAbip'].round(3)

        ## eXtrapolated Runs 
        main_df['XR'] = (main_df['H'] * 0.5) + (main_df['2B'] * 0.72) + (main_df['3B'] * 1.04) + (main_df['HR'] * 1.44) + (0.34 *(main_df['HBP'] + main_df['TB'] + main_df['IBB'])) + (0.25 * main_df['IBB']) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.09 * (main_df['AB'] - main_df['H'] - main_df['K'])) + (-0.098 * main_df['K']) + (-0.37 * main_df['GDP']) + (0.37 * main_df['SF']) + (0.04 * main_df['SH'])
        main_df['XR'] = main_df['XR'].round(3)
        
        ## eXtrapolated Runs Reduced
        main_df['XRR'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.33 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.098 * (main_df['AB'] - main_df['H']))
        main_df['XRR'] = main_df['XRR'].round(3)

        ## eXtrapolated Runs Basic
        main_df['XRB'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.34 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) - (-0.32 * main_df['CS']) + (-0.096 * (main_df['AB'] - main_df['H']))
        main_df['XRB'] = main_df['XRB'].round(3)

        ## Power-Speed Number
        main_df['PSN'] = (2 * main_df['HR'] * main_df['SB']) / (main_df['HR'] + main_df['SB'])
        main_df['PSN'] = main_df['PSN'].round(1)

        ## Runs scored percentage
        main_df['RS%'] = (main_df['R'] - main_df['HR']) / (main_df['H'] + main_df['HBP'] + main_df['BB'] - main_df['HR'])
        main_df['RS%'] = main_df['RS%'].round(3)

        ## Home Run percentage
        main_df['HR%'] = main_df['HR'] / main_df['PA']
        main_df['HR%'] = main_df['HR%'].round(3)

        ## Strikeout percentage
        main_df['K%'] = main_df['K'] / main_df['PA']
        main_df['K%'] = main_df['K%'].round(3)

        ## Strikeout percentage
        main_df['BB%'] = main_df['BB'] / main_df['PA']
        main_df['BB%'] = main_df['BB%'].round(3)

        ## Walks to strikeouts ratio
        main_df['K-BB%'] = main_df['K%'] - main_df['BB%']
        main_df['K-BB%'] = main_df['K-BB%'].round(3)

        main_df['BB/K'] = main_df['BB'] / main_df['K']
        main_df['BB/K'] = main_df['BB/K'].round(3)

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

    del part_df,main_df

    finished_df.to_csv(f'season_stats/team/batting_season_stats/csv/{season}_batting.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/batting_season_stats/parquet/{season}_batting.parquet',index=False)

def generate_season_team_pitching_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        league_df = pd.read_parquet('season_stats/league/pitching_season_stats/parquet/league_pitching.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_era = league_df['ERA'].iloc[0]
        fip_const = league_df['FIP_const'].iloc[0]

        main_df['App'] = 1
        main_df = main_df.astype({'IP':'string'})
        main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
        main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
        main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division'],as_index=False)\
            ['App','GS','W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A','BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA','SFA','GO','FO','KL','pickoffs'].sum())

        # main_df['PI'] = main_df['Pitches']
        # main_df = main_df.drop(['Pitches'], axis=1)

        main_df = main_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
        
        ## Win-loss percentage
        main_df['W-L%'] = main_df['W'] / (main_df['W'] + main_df['L'])
        
        ## Earned Run Average (ERA)
        main_df['ERA'] = 9 * (main_df['ER'] / main_df['IP'])

        ## Earned Run Average (ERA)
        main_df['ERA+'] = 100 * (lg_era / main_df['ERA'])
        main_df['ERA+'] = main_df['ERA+'].round(0)
        
        ## Field Indipendent Pitching
        main_df['FIP'] = (((13 * main_df['HR']) + (3 * (main_df['BB'] + main_df['HBP'])) - (2 * main_df['SO']))/main_df['IP']) + fip_const

        ## Walks and Hits per Inning Pitched (WHIP)
        main_df['WHIP'] = (main_df['BB'] + main_df['H']) / main_df['IP']
        
        ## Hits per 9 innings
        main_df['H9'] = 9 * (main_df['H'] / main_df['IP'])
        
        ## Home Runs per 9 innings
        main_df['HR9'] = 9 * (main_df['HR'] / main_df['IP'])
        
        ## Walks per 9 innings
        main_df['BB9'] = 9 * (main_df['BB'] / main_df['IP'])
        
        ## Strikeouts per 9 innings
        main_df['SO9'] = 9 * (main_df['SO'] / main_df['IP'])
        
        ## Strikeouts/Walks ratio
        main_df['SO/BB'] = main_df['SO'] / main_df['BB']
        
        ## Runs Allowed per 9 innings pitched (RA9)
        main_df['RA9'] = 9 * (main_df['R'] / main_df['IP'])

        main_df = main_df.mask(np.isinf(main_df))

        return main_df

    finished_df = pd.DataFrame()
    part_df = pd.DataFrame()
    main_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    for i in range(1,4):
        part_df = main_df[main_df['division']==i]
        part_df = parser(i,part_df)
        finished_df = pd.concat([finished_df,part_df],ignore_index=True)

    del part_df, main_df

    #print(finished_df)
    finished_df.to_csv(f'season_stats/team/pitching_season_stats/csv/{season}_pitching.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/pitching_season_stats/parquet/{season}_pitching.parquet',index=False)

def generate_season_team_fielding_stats(season:int):
    """
    """
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
    finished_df.to_csv(f'season_stats/team/fielding_season_stats/csv/{season}_fielding.csv',index=False)
    finished_df.to_parquet(f'season_stats/team/fielding_season_stats/parquet/{season}_fielding.parquet',index=False)

#####################################################################################################################################################################################################################
## Team Game Stats
##
#####################################################################################################################################################################################################################


def generate_team_game_batting_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        #main_df = pd.concat([main_df,part_df],ignore_index=True)
        league_df = pd.read_parquet('season_stats/league/batting_season_stats/parquet/league_batting.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_ops = league_df['OPS'].iloc[0]
        lg_slg = league_df['SLG'].iloc[0]

        main_df['G'] = 1
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division','date','game_id','field','opponent_id','opponent_name','runs_scored','runs_allowed','run_difference'],as_index=False)\
            ['AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','IBB','TB','GDP','HBP','SH','SF','DP','Picked','OPP DP'].sum())

        ## Runs Created (Technical version)
        main_df['RC'] = ((main_df['H'] + main_df['BB'] - main_df['CS'] + main_df['HBP'] - main_df['GDP']) * (main_df['TB'] + (0.26 * (main_df['BB'] - main_df['IBB'] + main_df['HBP']))) + (0.52 * (main_df['SH'] + main_df['SF'] + main_df['SB']))) / (main_df['AB'] + main_df['BB'] + main_df['HBP'] + main_df['SH'] + main_df['SF'])

        ## Plate Appearances
        main_df['PA'] = main_df['AB'] + main_df['BB'] + main_df['SF'] + main_df['SH']

        ## Batting Average
        main_df['BA'] = main_df['H'] / main_df['AB']
        main_df['BA'] = main_df['BA'].round(3)
        
        ## Secondary Average
        main_df['SecA'] = (main_df['BB'] + (main_df['TB'] - main_df['H']) + (main_df['SB'] - main_df['CS'])) / main_df['AB']
        main_df['SecA'] = main_df['SecA'].round(3)
                                            
        ## On Base Percentage (OBP)
        main_df['OBP'] = (main_df['H'] + main_df['BB'] + main_df['HBP']) / main_df['PA']
        main_df['OBP'] = main_df['OBP'].round(3)

        ## Slugging Percentae
        main_df['SLG'] = (main_df['H'] + (main_df['2B'] * 2) + (main_df['3B'] * 3) + (main_df['HR'] * 4)) / main_df['AB']
        main_df['SLG'] = main_df['SLG'].round(3)

        ## On-Base + Slugging Percentages
        main_df['OPS'] = main_df['OBP'] + main_df['SLG']
        main_df['OPS'] = main_df['OPS'].round(3)

        ## OPS+
        main_df['OPS+'] = 100 * ((main_df['OPS']/lg_ops) + (main_df['SLG']/lg_slg) -1)
        main_df['OPS+'] = main_df['OPS+'].round(0)

        ## Isolated power
        main_df['ISO'] = (main_df['2B'] + (main_df['3B'] * 2) + (main_df['3B'])) / main_df['AB']
        main_df['ISO'] = main_df['ISO'].round(3)

        ## Batting Average on balls in play
        main_df['BAbip'] = (main_df['H'] - main_df['HR']) / (main_df['AB'] - main_df['K'] - main_df['HR'] + main_df['SF'])
        main_df['BAbip'] = main_df['BAbip'].round(3)

        ## eXtrapolated Runs 
        main_df['XR'] = (main_df['H'] * 0.5) + (main_df['2B'] * 0.72) + (main_df['3B'] * 1.04) + (main_df['HR'] * 1.44) + (0.34 *(main_df['HBP'] + main_df['TB'] + main_df['IBB'])) + (0.25 * main_df['IBB']) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.09 * (main_df['AB'] - main_df['H'] - main_df['K'])) + (-0.098 * main_df['K']) + (-0.37 * main_df['GDP']) + (0.37 * main_df['SF']) + (0.04 * main_df['SH'])
        main_df['XR'] = main_df['XR'].round(3)
        
        ## eXtrapolated Runs Reduced
        main_df['XRR'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.33 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) + (-0.32 * main_df['CS']) + (-0.098 * (main_df['AB'] - main_df['H']))
        main_df['XRR'] = main_df['XRR'].round(3)

        ## eXtrapolated Runs Basic
        main_df['XRB'] = (0.5 * main_df['H']) + (0.72 * main_df['2B']) + (1.04 * main_df['3B']) + (1.44 * main_df['HR']) + (0.34 *(main_df['HBP'] + main_df['TB'])) + (0.18 * main_df['SB']) - (-0.32 * main_df['CS']) + (-0.096 * (main_df['AB'] - main_df['H']))
        main_df['XRB'] = main_df['XRB'].round(3)

        ## Power-Speed Number
        main_df['PSN'] = (2 * main_df['HR'] * main_df['SB']) / (main_df['HR'] + main_df['SB'])
        main_df['PSN'] = main_df['PSN'].round(1)

        ## Runs scored percentage
        main_df['RS%'] = (main_df['R'] - main_df['HR']) / (main_df['H'] + main_df['HBP'] + main_df['BB'] - main_df['HR'])
        main_df['RS%'] = main_df['RS%'].round(3)

        ## Home Run percentage
        main_df['HR%'] = main_df['HR'] / main_df['PA']
        main_df['HR%'] = main_df['HR%'].round(3)

        ## Strikeout percentage
        main_df['K%'] = main_df['K'] / main_df['PA']
        main_df['K%'] = main_df['K%'].round(3)

        ## Strikeout percentage
        main_df['BB%'] = main_df['BB'] / main_df['PA']
        main_df['BB%'] = main_df['BB%'].round(3)

        ## Walks to strikeouts ratio
        main_df['K-BB%'] = main_df['K%'] - main_df['BB%']
        main_df['K-BB%'] = main_df['K-BB%'].round(3)

        main_df['BB/K'] = main_df['BB'] / main_df['K']
        main_df['BB/K'] = main_df['BB/K'].round(3)

        ## Convert infinates into Null values
        #main_df = main_df.mask(np.isinf(main_df))

        #print(main_df)
        return main_df
    
    finished_df = pd.DataFrame()
    part_df = pd.DataFrame()
    main_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
    
    for i in range(1,4):
        part_df = main_df[main_df['division']==i]
        part_df = parser(i,part_df)
        finished_df = pd.concat([finished_df,part_df],ignore_index=True)

    del part_df,main_df

    finished_df.to_csv(f'game_stats/team/batting_game_stats/csv/{season}_batting.csv',index=False)
    finished_df.to_parquet(f'game_stats/team/batting_game_stats/parquet/{season}_batting.parquet',index=False)

def generate_team_game_pitching_stats(season:int):
    """
    """
    def parser(division:int,main_df:pd.DataFrame()):
        league_df = pd.read_parquet('season_stats/league/pitching_season_stats/parquet/league_pitching.parquet')
        league_df = league_df[league_df['season']==season]
        league_df = league_df[league_df['division']==division]

        if league_df.empty == True:
            return pd.DataFrame()
        
        lg_era = league_df['ERA'].iloc[0]
        fip_const = league_df['FIP_const'].iloc[0]

        main_df['App'] = 1
        main_df = main_df.astype({'IP':'string'})
        main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
        main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
        main_df['IP'] = main_df['whole_innings'] + (main_df['part_innings']/3)
        main_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division','date','game_id','field','opponent_id','opponent_name','runs_scored','runs_allowed','run_difference'],as_index=False)\
            ['W','L','SV','CG','SHO','IP','H','R','ER','2B-A','3B-A','HR-A','BB','IBB','SO','HB','Bk','WP','BF','P-OAB','Inh Run','Inh Run Score','SHA','SFA','GO','FO','KL','pickoffs'].sum())

        # main_df['PI'] = main_df['Pitches']
        # main_df = main_df.drop(['Pitches'], axis=1)

        main_df = main_df.rename(columns={'2B-A':'2B','3B-A':'3B','HR-A':'HR','Inh Run':'IR','Inh Run Score':'IRS','HB':'HBP'})
        
        ## Win-loss percentage
        main_df['W-L%'] = main_df['W'] / (main_df['W'] + main_df['L'])
        
        ## Earned Run Average (ERA)
        main_df['ERA'] = 9 * (main_df['ER'] / main_df['IP'])

        ## Earned Run Average (ERA)
        main_df['ERA+'] = 100 * (lg_era / main_df['ERA'])
        main_df['ERA+'] = main_df['ERA+'].round(0)
        
        ## Field Indipendent Pitching
        main_df['FIP'] = (((13 * main_df['HR']) + (3 * (main_df['BB'] + main_df['HBP'])) - (2 * main_df['SO']))/main_df['IP']) + fip_const

        ## Walks and Hits per Inning Pitched (WHIP)
        main_df['WHIP'] = (main_df['BB'] + main_df['H']) / main_df['IP']
        
        ## Hits per 9 innings
        main_df['H9'] = 9 * (main_df['H'] / main_df['IP'])
        
        ## Home Runs per 9 innings
        main_df['HR9'] = 9 * (main_df['HR'] / main_df['IP'])
        
        ## Walks per 9 innings
        main_df['BB9'] = 9 * (main_df['BB'] / main_df['IP'])
        
        ## Strikeouts per 9 innings
        main_df['SO9'] = 9 * (main_df['SO'] / main_df['IP'])
        
        ## Strikeouts/Walks ratio
        main_df['SO/BB'] = main_df['SO'] / main_df['BB']
        
        ## Runs Allowed per 9 innings pitched (RA9)
        main_df['RA9'] = 9 * (main_df['R'] / main_df['IP'])

        #main_df = main_df.mask(np.isinf(main_df))

        return main_df

    finished_df = pd.DataFrame()
    part_df = pd.DataFrame()
    main_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    for i in range(1,4):
        part_df = main_df[main_df['division']==i]
        part_df = parser(i,part_df)
        finished_df = pd.concat([finished_df,part_df],ignore_index=True)

    del part_df, main_df

    #print(finished_df)
    finished_df.to_csv(f'game_stats/team/pitching_game_stats/csv/{season}_pitching.csv',index=False)
    finished_df.to_parquet(f'game_stats/team/pitching_game_stats/parquet/{season}_pitching.parquet',index=False)

def generate_team_game_fielding_stats(season:int):
    """
    """
    main_df = pd.DataFrame()
    part_df = pd.DataFrame()
    # for i in range(1,5):
    #     #print(i)
    part_df = pd.read_parquet(f'game_stats/player/fielding_game_stats/parquet/{season}_fielding.parquet')
    main_df = pd.concat([main_df,part_df],ignore_index=True)
    
    finished_df = pd.DataFrame(main_df.groupby(['season','season_id','school_id','division','date','game_id','field','opponent_id','opponent_name','runs_scored','runs_allowed','run_difference'],as_index=False)\
        ['TC','PO','A','E','CI','PB','SBA','CSB','IDP','TP'].sum())
    ## Fielding Percentage
    finished_df['FLD%'] = (finished_df['PO'] + finished_df['A']) / (finished_df['PO'] + finished_df['A'] + finished_df['E'])
    ## Caught Stealing Percentage
    finished_df['CS%'] = finished_df['CSB'] / finished_df['SBA']
    
    #print(finished_df)
    #finished_df = finished_df.mask(np.isinf(finished_df))
    finished_df.to_csv(f'game_stats/team/fielding_game_stats/csv/{season}_fielding.csv',index=False)
    finished_df.to_parquet(f'game_stats/team/fielding_game_stats/parquet/{season}_fielding.parquet',index=False)


#####################################################################################################################################################################################################################

if __name__ == "__main__":
    current_year = int(datetime.now().year)
    generate_league_batting_stats()
    generate_league_pitching_stats()

    for i in range(2013,current_year+1):
        print(f'\n\nGenerating stats for the {i} season.\n\n')
        generate_team_game_batting_stats(i)
        generate_team_game_pitching_stats(i)

        generate_season_team_batting_stats(i)
        generate_season_team_pitching_stats(i)

        generate_season_player_batting_stats(i)
        generate_season_player_pitching_stats(i)

        if i >= 2016:
            generate_team_game_fielding_stats(i)
            generate_season_team_fielding_stats(i)
            generate_season_player_fielding_stats(i)

        generate_park_factors(i)

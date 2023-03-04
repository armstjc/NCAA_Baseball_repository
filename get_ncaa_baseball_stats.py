from collegebaseball import ncaa_scraper as ncaa
#from collegebaseball import datasets
from datetime import date

import time
from tqdm import tqdm
import pandas as pd
import ssl
#import warnings

#warnings.simplefilter(action='ignore', category=FutureWarning)
ssl._create_default_https_context = ssl._create_unverified_context

def get_ncaa_baseball_roster(season:int,division:int):
    """
    
    """
    if season < 2013:
        raise SyntaxError(f'\'season\' cannot be lower than 2013.')
    elif season > date.today().year:
        raise SyntaxError(f'\'season\' cannot be greater than {date.today().year}.')
    
    roster_df = pd.read_csv(f'TeamRosters/{season}_roster.csv')
    roster_df = roster_df.dropna(subset=['player_url','player_id'])

    if division == 0:
        return roster_df
    elif division >= 1 & division <= 3:
        roster_df = roster_df[roster_df['division']==division]
    else:
        raise SyntaxError('The inputted value for \'division\' must be 0, 1, 2, or 3.')
    
    print(roster_df)
    return roster_df

def get_season_ncaa_baseball_stats(season:int,division:int):
    """
    
    """

    if season < 2013:
        raise SyntaxError(f'\'season\' cannot be lower than 2013.')
    elif season > date.today().year:
        raise SyntaxError(f'\'season\' cannot be greater than {date.today().year}.')

    roster_df = get_ncaa_baseball_roster(season,division)

    if division == 0:
        return roster_df
    elif division >= 1 & division <= 3:
        roster_df = roster_df[roster_df['division']==division]
    else:
        raise SyntaxError('The inputted value for \'division\' must be 0, 1, 2, or 3.')

    player_id_arr = roster_df['player_id'].to_list()
    season_id_arr = roster_df['season_id'].to_list()
    school_id_arr = roster_df['team_id'].to_list()
    len_player_id_arr = len(player_id_arr)
    count = 0

    for i in tqdm(range(count,len_player_id_arr)):
        count += 1
        print(f'\n{count}/{len_player_id_arr}')
        ###############################################################################################################
        ##  Batting Stats
        ###############################################################################################################
        try:
            batting_df = ncaa.ncaa_player_game_logs(player=int(player_id_arr[i]), \
            season=int(season_id_arr[i]), variant='batting', school=int(school_id_arr[i]),include_advanced=False)
        except:
            print('No batting stats to download.')

        try:
            batting_df = batting_df[(batting_df['AB'] !=0) & (batting_df['result'] != 'cancelled')]
        except:
            print('No dataframe to filter.')

        if len(batting_df) > 0:
            batting_df.to_csv(f'PlayerStats/Batting/{int(season_id_arr[i])}_{int(player_id_arr[i])}.csv',index=False)

        time.sleep(4)

        ###############################################################################################################
        ##  Pitching Stats
        ###############################################################################################################
        try:
            pitching_df = ncaa.ncaa_player_game_logs(player=int(player_id_arr[i]),\
            season=int(season_id_arr[i]), variant='pitching', school=int(school_id_arr[i]),include_advanced=False)
        except:
            print('No pitching stats to download.')

        try:
            pitching_df = pitching_df.loc[(pitching_df['App'] != 0) & (pitching_df['IP'] != 0) & (pitching_df['pitches'] != 0)]
        except:
            print('No dataframe to filter.')
            
        if len(pitching_df) > 0:
            pitching_df.to_csv(f'PlayerStats/Pitching/{int(season_id_arr[i])}_{int(player_id_arr[i])}.csv',index=False)

        time.sleep(4)

        ###############################################################################################################
        ##  Fielding Stats
        ###############################################################################################################
        try:
            fielding_df = ncaa.ncaa_player_game_logs(player=int(player_id_arr[i]), \
            season=int(season_id_arr[i]), variant='fielding',  school=int(school_id_arr[i]),include_advanced=False)
        except:
            print('No fielding stats to download.')
        try:
            fielding_df = fielding_df.loc[(fielding_df['TC'] > 0) | (fielding_df['PB'] > 0) | (fielding_df['SBA'] > 0)]
        except:
            print('No dataframe to filter.')
            
        if len(fielding_df) > 0:
            fielding_df.to_csv(f'PlayerStats/Fielding/{int(season_id_arr[i])}_{int(player_id_arr[i])}.csv',index=False)

        time.sleep(4)

def main():
    get_season_ncaa_baseball_stats(2023,1)

if __name__ == "__main__":
    main()
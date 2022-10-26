from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import datasets
from datetime import date
import time
from tqdm import tqdm
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def getSchoolList():
    schools = datasets.get_school_path()
    school_df = pd.read_parquet(schools)
    #print(school_df)
    school_arr = school_df['ncaa_name'].to_numpy()
    return school_arr

def getSchoolAllTimeRoster(school='Ohio'):
    '''
    Gets the all time roster for a given school, starting from the
    2013 season, all the way to the present year.

    Args: School="Ohio" (text)
    '''
    now = date.today()
    schoolID = ncaa.lookup_school_id(school)
    minSeason = 2013
    maxSeason = now.year
    #print(minSeason,maxSeason)
    rost = ncaa.ncaa_team_roster(school,range(minSeason,maxSeason))
    #rost.to_csv(f'TeamRosters/{schoolID}.csv',index=False)
    print(rost)
    return rost

def getAllGbgStats():
    '''
    Gets literally all stats in D1 NCAA Mens Baseball.
    This will take some time to complete.
    '''
    
    schools =getSchoolList()
    #print(schools)
    hasRoster = True
    school_count = 0
    schools_len = len(schools)
    for s in range(772,len(schools)+1):
        #school_count += 1
        school_count = s
        i = schools[s]
        print(f"{school_count}/{schools_len} {i}")
        try:
            rost = getSchoolAllTimeRoster(i)
            arr_season_id = rost['season_id'].to_numpy()
            arr_school_name = rost['school'].to_numpy()
            arr_player_name = rost['name'].to_numpy()
            arr_player_id = rost['stats_player_seq'].to_numpy()
            arr_player_games = rost['games_played'].to_numpy()
            maxRost = len(rost)
            hasRoster = True
        except:
            hasRoster = False
            print(f'Could not retrive the rosters for {i}.')
        count = 0
        if hasRoster == True:
            for j in tqdm(rost.index):
                count += 1
                try:
                    season_id = arr_season_id[j]
                except:
                    season_id = 0
                try:
                    school_name = arr_school_name[j]
                except:
                    school_name = 0
                try:
                    player_name = arr_player_name[j]
                except:
                    player_name = 0
                try:                    
                    player_id = arr_player_id[j]
                except:
                    player_id = 0
                print(f'{count}/{maxRost} {season_id} {player_name}')
                
                try:
                    player_games_played = int(arr_player_games[j])
                except:
                    player_games_played = 0

                if player_games_played == 0:
                    print('This player did not play in this season.')
                else:
                    try:
                        data_b = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='batting')
                        #data_b = data_b[data_b['AB'] != 0]
                    
                        if (len(data_b)==0):
                        #print('Nothing to save')
                            print('')
                        else:
                            data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv',index=False)
                    except:
                        pass
                    #print(data_b)

                    try:

                        data_p = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='pitching')
                        #data_p = data_p.loc[data_p['App']>0]
                    
                        data_p = data_p[data_p['OrdAppeared'] != 0]
                        if (len(data_p)==0):
                            #print('Nothing to save')
                            print('')
                        else:
                            data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv',index=False)
                            #print(data_p)
                    except:
                        pass

                    try:
                        data_f = ncaa.ncaa_player_game_logs(player=player_name, season=season_id, variant='fielding',  school=school_name)
                        if len(data_f) > 0:
                            data_f.to_csv(f'PlayerStats/Fielding/{season_id}_{player_id}.csv',index=False)
                    except:
                        pass

                    time.sleep(4)

def getSeasonGbgStats(season=2020):
    '''
    Gets literally all stats in D1 NCAA Mens Baseball.
    This will take some time to complete.
    '''
    
    schools =getSchoolList()
    coll_count = 0
    max_schools = len(schools)
    #print(schools)
    hasRoster = True
    for i in schools.T:
        coll_count += 1
        print(f'{coll_count}/{max_schools} {i}')
        try:
            rost = ncaa.ncaa_team_season_roster(i,season)
            print(rost)
            
            maxRost = len(rost)
            hasRoster = True
        except:
            print(f'Could not retrive roster for {i}.')
            time.sleep(5)
            hasRoster = False
        count = 0
        if hasRoster == True:
            for j in rost.index:
                count += 1
                
                season_id = rost['season_id'][j]
                school_name = rost['school'][j]
                player_name = rost['name'][j]
                player_id = rost['stats_player_seq'][j]
                arr_player_games = rost['games_played'].to_numpy()

                try:
                    player_games_played = int(arr_player_games[j])
                except:
                    player_games_played = 0

                if player_games_played == 0:
                    print('This player did not play in this season.')
                else:

                    print(f'{count}/{maxRost} {school_name} {season} {player_name}')
                    
                    try:
                        data_b = ncaa.ncaa_player_game_logs(player=player_name, season=season_id, variant='batting',  school=school_name)
                        #data_b = data_b[(data_b['AB'] != 0) & (data_b['BB'] != 0)]
                        if len(data_b) > 0:
                            data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv',index=False)
                    except:
                        print(f'Could not get batting stats for {school_name} {season} {player_name}')

                    try:
                        data_p = ncaa.ncaa_player_game_logs( player=player_name,season=season_id, variant='pitching',  school=school_name)
                        data_p = data_p.loc[data_p['App']>0]
                        if len(data_p) > 0:
                            data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv',index=False)
                    except:
                        print(f'Could not get pitching stats for {school_name} {season} {player_name}')

                    try:
                        data_f = ncaa.ncaa_player_game_logs(player=player_name, season=season_id, variant='fielding',  school=school_name)
                        if len(data_f) > 0:
                            data_f.to_csv(f'PlayerStats/Fielding/{season_id}_{player_id}.csv',index=False)
                    except:
                        print(f'Could not get fielding stats for {school_name} {season} {player_name}')
                    time.sleep(4)


def main():
    print('starting up')
    getAllGbgStats()

    
if __name__ == "__main__":
    main() 
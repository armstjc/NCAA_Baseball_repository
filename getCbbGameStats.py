from operator import index
from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import datasets
from datetime import date
import time
import glob
from tqdm import tqdm
import pandas as pd
import numpy as np
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def getSchoolList():
    schools = datasets.get_school_table()
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

    for s in range(100,len(schools)):
        i = schools[s]
        print(i)
        try:
            rost = getSchoolAllTimeRoster(i)
            arr_season_id = rost['season_id'].to_numpy()
            arr_school_name = rost['school'].to_numpy()
            arr_player_name = rost['name'].to_numpy()
            arr_player_id = rost['stats_player_seq'].to_numpy()
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
                data_b = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='batting')
                try:
                    if (len(data_b)==0):
                    #print('Nothing to save')
                        print('')
                    else:
                        data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv',index=False)
                except:
                    pass
                #print(data_b)

                data_p = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='pitching')
                try:
                    data_p = data_p[data_p['OrdAppeared'] != 0]
                    if (len(data_p)==0):
                        #print('Nothing to save')
                        print('')
                    else:
                        data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv',index=False)
                        #print(data_p)
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
                print(f'{count}/{maxRost} {school_name} {season} {player_name}')
                data_b = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='batting')
                
                if len(data_b) > 0:
                    data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv',index=False)
            
                data_p = ncaa.ncaa_player_game_logs(school=school_name, player=player_name, season=season_id, variant='pitching')

                data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv',index=False)
                
                time.sleep(5)

def mergeFiles(filePath=""):
    main = pd.DataFrame()
    f = 0
    l = filePath

    for file in glob.iglob(l+"/*csv"):
        f +=1
    for file in tqdm(glob.iglob(l+"/*csv"),total=f,ascii=True, bar_format='{l_bar}{bar:30}{r_bar}{bar:-30b}'):
        try:
            df = pd.read_csv(file)
            main = pd.concat([main,df],ignore_index=True)
        except:
            pass
    return main

def mergeBattingLogs():
    f = "PlayerStats/Batting"
    df = mergeFiles(f)
    df.to_csv("PlayerStats/batting_logs.csv",index=False)

def mergePitchingLogs():
    f = "PlayerStats/Pitching"
    df = mergeFiles(f)
    df.to_csv("PlayerStats/pitching_logs.csv",index=False)

def splitBattingStats():
    print('Reading the batting logs file.')
    df = pd.read_csv('PlayerStats/batting_logs.csv')
    print('Done!\n')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+2):
        print(f'Creating batting logs for the {i} season.')
        s_df = df[df['season'] == i]
        partOne = s_df.sample(frac=0.5)
        partTwo = s_df.drop(partOne.index)

        partOne.to_csv(f'PlayerStats/{i}_01_batting.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_02_batting.csv',index=False)
        #s_df.to_csv(f'PlayerStats/{i}_batting.csv')

def splitPitchingStats():
    print('Reading the pitching logs file.')
    df = pd.read_csv('PlayerStats/pitching_logs.csv')
    print('Done!\n')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Creating pitching logs for the {i} season.')
        s_df = df[df['season'] == i]
        partOne = s_df.sample(frac=0.5)
        partTwo = s_df.drop(partOne.index)

        partOne.to_csv(f'PlayerStats/{i}_01_pitching.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_02_pitching.csv',index=False)
        #s_df.to_csv(f'PlayerStats/{i}_batting.csv')

def main():
    print('starting up')
    getAllGbgStats()

    
if __name__ == "__main__":
    main()
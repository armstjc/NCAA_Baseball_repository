from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import datasets
from datetime import date
import time
import pandas as pd

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
    rost = ncaa.get_multiyear_roster(school,minSeason,maxSeason)
    rost.to_csv(f'TeamRosters/{schoolID}.csv',index=False)
    print(rost)
    return rost

def getAllGbgStats():
    '''
    Gets literally all stats in D1 NCAA Mens Baseball.
    This will take some time to complete.
    '''
    
    schools =getSchoolList()
    #print(schools)

    for i in schools.T:
        print(i)
        rost = getSchoolAllTimeRoster(i)
        arr_season_id = rost['season_id'].to_numpy()
        arr_school_name = rost['school'].to_numpy()
        arr_player_name = rost['name'].to_numpy()
        arr_player_id = rost['stats_player_seq'].to_numpy()
        maxRost = len(rost)
        count = 0
        for j in rost.index:
            count += 1
            
            season_id = arr_season_id[j]
            school_name = arr_school_name[j]
            player_name = arr_player_name[j]
            player_id = arr_player_id[j]
            print(f'{count}/{maxRost} {season_id} {player_name}')
            data_b = ncaa.get_gbg_stats(school=school_name, player=player_name, season=season_id, variant='batting')
            data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv')
            #print(data_b)

            data_p = ncaa.get_gbg_stats(school=school_name, player=player_name, season=season_id, variant='pitching')
            try:
                data_p = data_p[data_p['OrdAppeared'] != 0]
            except:
                pass

            if (len(data_p)==0):
                #print('Nothing to save')
                print('')
            else:
                data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv')
                #print(data_p)
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

    for i in schools.T:
        coll_count += 1
        print(f'{coll_count}/{max_schools} {i}')
        rost = ncaa.get_roster(i,season)
        print(rost)
        #arr_season_id = rost['season_id'].to_numpy()
        #arr_school_name = rost['school'].to_numpy()
        #arr_player_name = rost['name'].to_numpy()
        #arr_player_id = rost['stats_player_seq'].to_numpy()
        maxRost = len(rost)
        count = 0
        for j in rost.index:
            count += 1
            
            season_id = rost['season_id'][j]
            school_name = rost['school'][j]
            player_name = rost['name'][j]
            player_id = rost['stats_player_seq'][j]
            print(f'{count}/{maxRost} {season} {player_name}')
            data_b = ncaa.get_gbg_stats(school=school_name, player=player_name, season=season_id, variant='batting')
            data_b.to_csv(f'PlayerStats/Batting/{season_id}_{player_id}.csv',index=False)
            #print(data_b)

            data_p = ncaa.get_gbg_stats(school=school_name, player=player_name, season=season_id, variant='pitching')
            try:
                data_p = data_p[data_p['OrdAppeared'] != 0]
            except:
                pass

            if (len(data_p)==0):
                #print('Nothing to save')
                pass
            else:
                data_p.to_csv(f'PlayerStats/Pitching/{season_id}_{player_id}.csv',index=False)
                #print(data_p)
            time.sleep(5)

def main():
    print('starting up')
    #df = getSchoolAllTimeRoster()
    getSeasonGbgStats(2013)  
    getSeasonGbgStats(2014)
    getSeasonGbgStats(2015)
    getSeasonGbgStats(2016)
    getSeasonGbgStats(2017)
    getSeasonGbgStats(2018)
    getSeasonGbgStats(2019)
    getSeasonGbgStats(2020)
    getSeasonGbgStats(2021)
    getSeasonGbgStats(2022)
if __name__ == "__main__":
    main()
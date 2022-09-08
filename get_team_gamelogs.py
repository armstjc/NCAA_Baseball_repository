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
    rost = ncaa.ncaa_team_roster(schoolID,range(minSeason,maxSeason))
    rost.to_csv(f'TeamRosters/{schoolID}.csv',index=False)
    print(rost)
    return rost

def download_gamelogs():
    schools = getSchoolList()
    df = pd.DataFrame(schools)

    df.to_csv('school-list.csv')
    

def main():
    print('Starting Up')
    arr = getSchoolList()
    for i in tqdm(arr):
        getSchoolAllTimeRoster(i)
if __name__ == "__main__":
    main()

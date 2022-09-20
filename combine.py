from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import datasets
import os
import glob
from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing import Pool

def reader(filename):
        
        return pd.read_csv(filename, encoding='latin-1')

def mergeFilesMultithreaded(filePath=""):
    #global filecount
    #filecount = 0
    num_cpus = os.cpu_count()
    print(f'{num_cpus} cpu cores advalible to this script.')

    pool = Pool(num_cpus-1)
    main_df = pd.DataFrame()
    
    l = filePath
    file_list = glob.iglob(l+"/*csv")
    file_list = list(file_list)
    df_list = pool.map(reader,tqdm(file_list))

    main_df = pd.concat(df_list)

    return main_df

def mergeFiles(filePath=""):
    
    main_df = pd.DataFrame()
    f = 0
    l = filePath
    file_list = glob.iglob(l+"/*csv")
    for file in file_list:
        f +=1

    # with open('filelist.txt','w+',encoding='utf-8') as f:
    #     f.write(str(file_list))
    for file in tqdm(glob.iglob(l+"/*csv"),total=f):
        #len_file = len(file)
        # if os.stat(file).st_size == 0:
        #     print(f'{file} is empty')
        # else:
        df = pd.read_csv(file)
        main_df = pd.concat([main_df,df],ignore_index=True)
        # main_df = pd.concat([pd.read_csv(f) for f in file_list])
    return main_df

def mergeBattingLogs():
    f = "PlayerStats/Batting"
    df = mergeFilesMultithreaded(f)
    df.to_csv("PlayerStats/batting_logs.csv",index=False)

def mergePitchingLogs():
    f = "PlayerStats/Pitching"
    df = mergeFilesMultithreaded(f)
    df.to_csv("PlayerStats/pitching_logs.csv",index=False)

def mergeFieldingLogs():
    f = "PlayerStats/Fielding"
    df = mergeFilesMultithreaded(f)
    df.to_csv("PlayerStats/fielding_logs.csv",index=False)


def mergeRosters():
    f = "TeamRosters/teams"
    df = mergeFilesMultithreaded(f)
    df.to_csv("TeamRosters/rosters.csv",index=False)

def pbpReader(filename):
        game_id = str(os.path.basename(filename)).replace(".csv","")
        df = pd.read_csv(filename, encoding='latin-1')
        df['game_id'] = game_id
        return df

def splitRosters():
    print('Reading the rosters file.')
    df = pd.read_csv('TeamRosters/rosters.csv')
    print('Done!\n')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Creating the roster file for the {i} season.')
        s_df = df[df['season'] == i]
        # len_s_df = len(s_df)
        # len_s_df = len_s_df // 2
        # partOne = s_df.iloc[:len_s_df,:]
        # partTwo = s_df.iloc[len_s_df:,:]

        # partOne.to_csv(f'TeamRosters/{i}_roster_01.csv',index=False)
        # partTwo.to_csv(f'TeamRosters/{i}_roster_02.csv',index=False)
        s_df.to_csv(f'TeamRosters/{i}_roster.csv',index=False)


def mergePbpMultithreaded(filePath=""):
    #global filecount
    #filecount = 0
    num_cpus = os.cpu_count()
    print(f'{num_cpus} cpu cores advalible to this script.')

    pool = Pool(num_cpus-1)
    main_df = pd.DataFrame()
    
    l = filePath
    file_list = glob.iglob(l+"/*csv")
    file_list = list(file_list)
    df_list = pool.map(pbpReader,tqdm(file_list))

    main_df = pd.concat(df_list)

    return main_df

def mergePbpLogs():
    f = "pbp/games"
    df = mergePbpMultithreaded(f)
    df.to_csv("pbp/pbp_logs.csv",index=False)



def splitBattingStats():
    print('Reading the batting logs file.')
    df = pd.read_csv('PlayerStats/batting_logs.csv')
    print('Done!\n')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Creating batting logs for the {i} season.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 2
        partOne = s_df.iloc[:len_s_df,:]
        partTwo = s_df.iloc[len_s_df:,:]

        partOne.to_csv(f'PlayerStats/{i}_batting_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_batting_02.csv',index=False)
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

        partOne.to_csv(f'PlayerStats/{i}_pitching_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_pitching_02.csv',index=False)
        #s_df.to_csv(f'PlayerStats/{i}_batting.csv')

def splitFieldingStats():
    print('Reading the fielding logs file.')
    df = pd.read_csv('PlayerStats/fielding_logs.csv')
    print('Done!\n')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Creating fielding logs for the {i} season.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 2
        partOne = s_df.iloc[:len_s_df,:]
        partTwo = s_df.iloc[len_s_df:,:]

        partOne.to_csv(f'PlayerStats/{i}_fielding_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_fielding_02.csv',index=False)


def splitPbpLogs():
    print('Reading the play-by-play logs file.')
    df = pd.read_csv('pbp/pbp_logs.csv')
    print('Done!\n')
    df['season'] = pd.DatetimeIndex(df['date']).year
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Creating play-by-play logs for the {i} season.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'pbp/{i}_pbp_01.csv',index=False)
        partTwo.to_csv(f'pbp/{i}_pbp_02.csv',index=False)
        partThree.to_csv(f'pbp/{i}_pbp_03.csv',index=False)
        partFour.to_csv(f'pbp/{i}_pbp_04.csv',index=False)

def main():
    print('Starting Up...')
    mergePitchingLogs()
    mergeBattingLogs()
    mergeFieldingLogs()
    mergePbpLogs()
    mergeRosters()

    splitBattingStats()
    splitPitchingStats()
    splitFieldingStats()
    splitPbpLogs()
    splitRosters()
    
    os.remove('PlayerStats/batting_logs.csv')
    os.remove('PlayerStats/pitching_logs.csv')
    os.remove('PlayerStats/fielding_logs.csv')
    os.remove('pbp/pbp_logs.csv')
    os.remove('TeamRosters/rosters.csv')

if __name__ == "__main__":
    main()
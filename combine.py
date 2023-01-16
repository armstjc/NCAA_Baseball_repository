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

def mergeBattingStats():
    print('Starting the merge of Batting CSVs.')
    f = f"PlayerStats/batting"
    print('Collecting downloaded CSVs.')
    df = mergeFilesMultithreaded(f)
    print('Done!')
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Saving off the stats for {i}.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'PlayerStats/{i}_batting_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_batting_02.csv',index=False)
        partThree.to_csv(f'PlayerStats/{i}_batting_03.csv',index=False)
        partFour.to_csv(f'PlayerStats/{i}_batting_04.csv',index=False)

def mergePitchingStats():
    print('Starting the merge of Pitching CSVs.')
    f = f"PlayerStats/pitching"
    print('Collecting downloaded CSVs.')
    df = mergeFilesMultithreaded(f)
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Saving off the stats for {i}.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'PlayerStats/{i}_pitching_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_pitching_02.csv',index=False)
        partThree.to_csv(f'PlayerStats/{i}_pitching_03.csv',index=False)
        partFour.to_csv(f'PlayerStats/{i}_pitching_04.csv',index=False)

def mergeFieldingStats():
    print('Starting the merge of Fielding CSVs.')
    f = f"PlayerStats/fielding"
    print('Collecting downloaded CSVs.')
    df = mergeFilesMultithreaded(f)
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        print(f'Saving off the stats for {i}.')
        s_df = df[df['season'] == i]
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'PlayerStats/{i}_fielding_01.csv',index=False)
        partTwo.to_csv(f'PlayerStats/{i}_fielding_02.csv',index=False)
        partThree.to_csv(f'PlayerStats/{i}_fielding_03.csv',index=False)
        partFour.to_csv(f'PlayerStats/{i}_fielding_04.csv',index=False)

def main():
    mergeBattingStats()
    mergePitchingStats()
    mergeFieldingStats()

if __name__ == "__main__":
    main()
from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import datasets
import os
import glob
from tqdm import tqdm
import pandas as pd
import numpy as np

def mergeFiles(filePath=""):
    main = pd.DataFrame()
    f = 0
    l = filePath
    file_list = glob.iglob(l+"/*csv")
    for file in file_list:
        f +=1

    # with open('filelist.txt','w+',encoding='utf-8') as f:
    #     f.write(str(file_list))
    for file in tqdm(glob.iglob(l+"/*csv"),total=f,ascii=True, bar_format='{l_bar}{bar:30}{r_bar}{bar:-30b}'):
        #len_file = len(file)
        # if os.stat(file).st_size == 0:
        #     print(f'{file} is empty')
        # else:
        df = pd.read_csv(file)
        main_df = pd.concat([main,df],ignore_index=True)
        # main_df = pd.concat([pd.read_csv(f) for f in file_list])
    return main_df

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
    print('Starting Up...')
    mergeBattingLogs()
    mergePitchingLogs()
    splitBattingStats()
    splitPitchingStats()
    #os.remove('PlayerStats/pitching_logs.csv')
    #os.remove('PlayerStats/batting_logs.csv')
if __name__ == "__main__":
    main()
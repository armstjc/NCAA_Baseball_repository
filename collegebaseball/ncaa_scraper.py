"""
ncaa_scraper 

A module to scrape and parse college baseball statistics from stats.ncaa.org

Created by Nathan Blumenfeld in Spring 2022
"""
import pandas as pd
import time
import random
from bs4 import BeautifulSoup, NavigableString, Tag
import numpy as np
from collegebaseball import datasets, metrics
from requests import Request, Session
import re


#lookup paths
_SCHOOL_ID_LU_PATH = datasets.get_school_table()
_SEASON_ID_LU_PATH = datasets.get_season_lu_table()
_PLAYERS_HISTORY_LU_PATH = datasets.get_players_history_table()
_PLAYER_ID_LU_PATH = datasets.get_player_id_lu_table()
_ROSTERS_LU_PATH = datasets.get_rosters_table()

#pre-load lookup tables for performance
_SCHOOL_ID_LU_DF = pd.read_parquet(_SCHOOL_ID_LU_PATH)
_SEASON_LU_DF = pd.read_parquet(_SEASON_ID_LU_PATH)
_PLAYERS_HISTORY_LU_DF = pd.read_parquet(_PLAYERS_HISTORY_LU_PATH)
_PLAYER_ID_LU_DF = pd.read_parquet(_PLAYER_ID_LU_PATH)
_ROSTERS_DF = pd.read_parquet(_ROSTERS_LU_PATH)

#GET request options
_HEADERS = {'User-Agent':'Mozilla/5.0'}
_TIMEOUT = 4


def ncaa_team_stats(school, season, variant, include_advanced=True):
    """
    Obtains individual aggregate single-season batting/pitching stats totals for all players
    on a given team. 
    
    Args:
        school: schools (str) or NCAA school_id (int) 
        season: season (int, YYYY) or NCAA season_id (int)
        variant (str): the type of stats, either 'batting' or 'pitching'
        include_advanced (bool, optional). Whether to automatically calcuate advanced metrics,
        Defaults to True 

        valid 2012-2022

    Returns: 
        DataFrame with the following columns:
        
        batting
        -------
        Jersey, name, Yr, pos, GP, GS, R, AB, H, 2B, 3B,
        TB, HR, RBI, BB, HBP, SF, SH, K, OPP DP, CS,
        Picked, SB, IBB, season, stats_player_seq

        pitching
        --------
        Jersey, name, Yr, pos, GP, App, ERA, IP, CG, H, R,
        ER, BB, SO, SHO, BF, P-OAB, 2B-A, 3B-A, Bk, HR-A,
        WP, HB, IBB, Inh Run, Inh Run Score, SHA, SFA, Pitches,
        GO, FO, W, L, SV, KL, pickoffs, season, stats_player_seq

        
    data from stats.ncaa.org. valid 2013 - 2022. 
    """
    season, season_id, batting_id, pitching_id = lookup_season_info(season)
    school, school_id = lookup_team_info(school)
    
    if variant == 'batting':
        stat_category_id = batting_id
    elif variant == 'pitching':
        stat_category_id = pitching_id
       
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id':str(season_id), 'id':str(season_id),\
               'year_stat_category_id':str(stat_category_id)}
    try:
        with Session() as s: 
            r = s.get(url, params=payload, headers=_HEADERS)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    soup = BeautifulSoup(r.text, features = 'lxml')
    table = soup.find(name='table', id='stat_grid')
    headers = [x for x in table.thead.tr.stripped_strings]
    headers.insert(headers.index('Player'), 'stats_player_seq')
    if season == 2022 and variant=='batting':  # 2022 NCAA data does not include RBI2out Column Data
        headers.remove('RBI2out')

    rows = []
    for i in table.tbody.find_all('tr'):
        row = []
        for j in i.find_all('td'):
            if 'data-order' in j.attrs: 
                row.append(j.attrs['data-order'])
            else: 
                if j.find('a') is not None: 
                    if 'href' in j.a.attrs:
                        row.append(int(j.a.attrs['href'].split('&')[-1].split('=')[-1]))
                    else:
                        row.append('-')             
                row.append(j.string)
        if len(row) > len(headers): 
            row = row[:-1]
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    df.columns = headers
    df['season'] = season
    res = _transform_team_stats(df, variant=variant)
    if variant == 'batting':
        if include_advanced: 
            res = metrics.add_batting_metrics(res)
            res = res.loc[res.PA > 0]
    if variant == 'pitching': 
        res = res.loc[res.App > 0]
        if include_advanced:
            res = metrics.add_pitching_metrics(res)
    return res

def ncaa_team_totals(school, season, variant):
    if type(school) is int:
        school_id = school
    elif type(school) is str: 
        school_id = lookup_school_id(school)
    if len(str(season)) == 4:
        season_ids = lookup_season_ids(season)
        season_id = season_ids[0]
    elif len(str(season)) == 5: 
        season_id = season
        season_ids = lookup_season_ids_reverse(season_id)
        season = season_ids[0]
    
    if variant == 'batting':
        stat_category_id = season_ids[1]
    elif variant == 'pitching':
        stat_category_id = season_ids[2]
       
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id':str(season_id), 'id':str(season_id),\
               'year_stat_category_id':str(stat_category_id)}
    try:
        with Session() as s: 
            r = s.get(url, params=payload, headers=_HEADERS)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    soup = BeautifulSoup(r.text, features = 'lxml')
    table = soup.find(name='table', id='stat_grid')
    headers = [x for x in table.thead.tr.stripped_strings]
    if season == 2022 and variant == 'batting':  # 2022 NCAA data does not include RBI2out Column Data
        headers.remove('RBI2out')

    rows = []
    for i in table.tfoot.find_all('tr'):
        row = []
        for j in i.find_all('td'):
            if 'data-order' in j.attrs: 
                row.append(j.attrs['data-order'])
            else:          
                row.append(j.string)
        if len(row) > len(headers): 
            row = row[:-1]
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    df['season'] = season
    res = _transform_team_stats(df, variant=variant)
    res = res.drop(columns=['Jersey', 'Yr', 'pos', 'GP'], inplace=False)
    return res

def ncaa_player_game_logs(player, season, variant, school=None):
    """
    A function to obtain game-by-game stats for a given player players
    Transmits a GET request to stats.ncaa.org, parses game-by-game stats. 

    Args: 
        player: player name (str) or NCAA stats_player_seq (int)
        variant (str): the type of stats, either 'batting' or 'pitching'
        season: season as (int, YYYY) or NCAA season_id (int)
        school (optional only if not passing a stats_player_seq): school name (str)
            or NCAA school_id (int)

    Returns: 
        DataFrame with the following columns:
        
        batting
        -------
        game_id, date, field, opponent_name, opponent_id, 
        innings_played, extras, runs_scored, runs_allowed, 
        run_difference, result, school_id, season_id, batting, GP,
        BA, G, OBPct, SlgPct, R, AB, H, 2B, 3B, TB, HR, RBI, BB, HBP,
        SF, SH, K, OPP DP, CS, Picked, SB, IBB, RBI2out, season

        pitching
        --------
        game_id, date, field, opponent_name, opponent_id, 
        innings_played, extras, runs_scored, runs_allowed, 
        run_difference, result, school_id, season_id, school_id,
        GP, G, App, GS, ERA, IP, CG, H, R, ER, BB, SO, SHO, BF,
        P-OAB, 2B-A, 3B-A, Bk, HR-A, WP, HB, IBB, Inh Run,
        Inh Run Score, SHA, SFA, Pitches, GO, FO, W, L, SV,
        OrdAppeared, KL, season
      
    data from stats.ncaa.org. valid 2012 - 2022. 
    """
    season, season_id, batting_id, pitching_id = lookup_season_info(season)
    if school is not None: 
        school, school_id = lookup_team_info(school)

    if type(player) == int:
        player_id = player
        try: 
            player_name, school, school_id = lookup_player_reverse(player_id, season)
        except: 
            print('no records found')
            return pd.DataFrame()
    elif type(player) == str: 
        player_name = player
        if school is not None: 
            player_id = lookup_player_id(player_name, school)
        else: 
            return 'must give a player_id if no school given'               
     
    stats_player_seq = str(player_id)

    if variant == 'batting':
        year_stat_category_id = batting_id
        batting_headers_dict = {
        2022:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'GDP', 'RBI2out', 'stats_player_seq'], 
        2021:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out', 'stats_player_seq'],
        2020:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out', 'stats_player_seq'],
        2019:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out', 'stats_player_seq'],
        2018:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out', 'stats_player_seq'],
        2017:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out', 'stats_player_seq'],
        2016:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'RBI2out', 
            'IBB', 'stats_player_seq'],
        2015:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB',
            'stats_player_seq'],
        2014:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'AB', 'R', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB',
            'stats_player_seq'],
        2013:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'AB', 'H', 'TB', 'R', '2B', '3B', 'HR', 'RBI', 'BB', 'HBP', 
            'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB', 
            'stats_player_seq'],
        2012:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'HBP', 
            'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB',
            'stats_player_seq']}
        headers = batting_headers_dict[season]
    else: 
        year_stat_category_id = pitching_id
        pitching_headers_dict = {
        2022:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'App', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs', 'stats_player_seq'], 
        2021:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs', 'stats_player_seq'],
        2020:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs', 'stats_player_seq'],
        2019:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2018:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2017:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2016:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2015:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2014:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'],
        2013:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq'], 
        2012:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL',
            'stats_player_seq']}
        headers = pitching_headers_dict[season]
    # try:   
    payload = {'game_sport_year_ctl_id':str(season_id), 'org_id':str(school_id), \
        'stats_player_seq':str(stats_player_seq), 'year_stat_category_id':str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/game_by_game?'
    with Session() as s: 
        r = s.get(url, params=payload, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    try:
        table = soup.find_all('table')[3]
        # this is heinous lol
        # TODO: make this more readable
        rows = []
        for val in table.find_all('tr')[3:]: 
            data = []
            for i in val.children:
                if isinstance(i, Tag):
                    if 'data-order' in i.attrs:
                        data.append(i.get('data-order'))
                    elif i.a:
                        href = i.find_all('a')[-1].get('href')
                        if '?' in href:
                            team_id = href.split('=')[-1]
                            game_id = href.split('?')[0].split('/')[-1]
                            score = i.find_all('a')[-1].string.strip()
                            if 'W' in score: 
                                result = 'win'
                                score = score.replace('W', '').strip()
                            elif 'L' in score: 
                                result = 'loss'
                                score = score.replace('L', '').strip()
                            try:
                                if '(' in score:
                                    innings_played = score.split('(')[-1].split(')')[0].strip()
                                    extras = 'True'
                                    score = score.split('(')[0].strip()
                                else: 
                                    innings_played = '9'
                                    extras = 'False'
                                data.append(innings_played)
                                data.append(extras)
                                scores = score.split('-')
                                runs_scored = int(scores[0].strip())
                                runs_allowed = int(scores[-1].strip())
                                data.append(runs_scored)
                                data.append(runs_allowed)
                                data.append(runs_scored - runs_allowed)
                            except:
                                continue
                            data.append(result)
                            data.append(score)
                            data.append(game_id)
                            data.append(team_id)
                        else:
                            try: 
                                if href.split('/')[1] == 'teams':
                                    season_id = '-'
                                    opponent_id = href.split('/')[-1]
                                elif href.split('/')[-1] == 'box_score':
                                    game_id = href.split('/')[-2]
                                    team_id = school_id
                                    score = i.find_all('a')[-1].string.strip()
                                    if 'W' in score: 
                                        result = 'win'
                                        score = score.replace('W', '').strip()
                                    elif 'L' in score: 
                                        result = 'loss'
                                        score = score.replace('L', '').strip()
                                    try:
                                        if '(' in score:
                                            innings_played = score.split('(')[-1].split(')')[0].strip()
                                            extras = 'True'
                                            score = score.split('(')[0].strip()
                                        else: 
                                            innings_played = '9'
                                            extras = 'False'
                                        data.append(innings_played)
                                        data.append(extras)
                                        scores = score.split('-')
                                        runs_scored = int(scores[0].strip())
                                        runs_allowed = int(scores[-1].strip())
                                        data.append(runs_scored)
                                        data.append(runs_allowed)
                                        data.append(runs_scored - runs_allowed)
                                    except:
                                        continue
                                    data.append(result)
                                    data.append(score)
                                    data.append(game_id)
                                    data.append(team_id)                
                                else:    
                                    season_id = href.split('/')[-1]
                                    opponent_id = href.split('/')[-2]
                            except:
                                continue
                            if not 'target' in i.find_all('a')[-1].attrs:
                                opponent = i.find_all('a')[-1].contents[0]
                                opponent = opponent.string.replace('</br>', '')
                                opponent = opponent.strip()
                                if opponent[0] == '@': 
                                    field = 'away'
                                    opponent = opponent.split('@')[-1].strip()
                                elif '@' in opponent:
                                    field = 'neutral'
                                else: 
                                    field = 'home'
                                data.append(field)
                                data.append(season_id)
                                data.append(opponent_id)
                                data.append(opponent)
                    else:
                        date = i.string
                        if date == 'Opponent Totals':
                            continue
                        else:
                            data.append(date)
            if len(data) == (len(headers)-1):
                data.append(int(player_id))
                rows.append(data)    
        res = pd.DataFrame(rows, columns=headers)
        res = res.loc[res.field.isin(['away', 'home', 'neutral'])] # if not, something has gone wrong w/ row
        res = _transform_team_stats(res, variant=variant)
        return res
    except:
        print(f'Could not find any data for {player}.')
        #time.sleep(5)
def ncaa_team_game_logs(school, season, variant:str): 
    """
    A function to obtain game-by-game stats for teams.
    Transmits a GET request to stats.ncaa.org, parses game-by-game stats. 

    Args: 
        school: school name (str) or NCAA school_id (int)
        seasons: seasons as (int, YYYY) or NCAA season_id (list)
        variant (str): 'batting' or 'pitching'

    Returns: 
        DataFrame with the following columns:
        
        batting
        -------
        game_id, date, field, opponent_name, opponent_id, 
        innings_played, extras, runs_scored, runs_allowed, 
        run_difference, result, school_id, season_id, batting, GP,
        BA, G, OBPct, SlgPct, R, AB, H, 2B, 3B, TB, HR, RBI, BB, HBP,
        SF, SH, K, OPP DP, CS, Picked, SB, IBB, RBI2out, season

        pitching
        --------
        game_id, date, field, opponent_name, opponent_id, 
        innings_played, extras, runs_scored, runs_allowed, 
        run_difference, result, school_id, season_id, school_id,
        GP, G, App, GS, ERA, IP, CG, H, R, ER, BB, SO, SHO, BF,
        P-OAB, 2B-A, 3B-A, Bk, HR-A, WP, HB, IBB, Inh Run,
        Inh Run Score, SHA, SFA, Pitches, GO, FO, W, L, SV,
        OrdAppeared, KL, season
      
    data from stats.ncaa.org. valid 2012 - 2022. 
    """
    season, season_id, batting_id, pitching_id = lookup_season_info(season)
    school, school_id = lookup_team_info(school)
    
    stats_player_seq = '-100'
    
    # html changes year to year make it difficult to automatically read tables
    # and NCAA likes to mess with the columns in different years
    # so this in-elegant solution is what we have 

    if variant == 'batting':
        year_stat_category_id = batting_id
        batting_headers_dict = {
        2022:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'GDP', 'RBI2out'], 
        2021:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out'],
        2020:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'OPP DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out'],
        2019:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out'],
        2018:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out'],
        2017:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'IBB', 
            'RBI2out'],
        2016:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'CS', 'Picked', 'SB', 'RBI2out', 
            'IBB'],
        2015:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB'],
        2014:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'AB', 'R', 'H', '2B', '3B', 'TB', 'HR', 'RBI', 'BB', 
            'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB'],
        2013:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'AB', 'H', 'TB', 'R', '2B', '3B', 'HR', 'RBI', 'BB', 'HBP', 
            'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB'],
        2012:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'HBP', 
            'SF', 'SH', 'K', 'DP', 'SB', 'CS', 'Picked', 'IBB']}

        headers = batting_headers_dict[season]

    else: 
        year_stat_category_id = pitching_id
        pitching_headers_dict = {
        2022:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'G', 'App', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs'], 
        2021:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs'],
        2020:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL', 
            'pickoffs'],
        2019:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2018:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2017:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2016:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2015:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'G', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2014:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'],
        2013:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL'], 
        2012:['date', 'field', 'season_id', 'opponent_id', 'opponent_name', 
            'innings_played', 'extras', 'runs_scored', 'runs_allowed', 
            'run_difference', 'result', 'score', 'game_id', 'school_id', 
            'App', 'GS', 'IP', 'H', 'R', 'ER', 'BB', 'SO', 
            'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP', 
            'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 
            'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared', 'KL']}
        headers = pitching_headers_dict[season]

    payload = {'game_sport_year_ctl_id':str(season_id), 'org_id':str(school_id), \
        'stats_player_seq':str(stats_player_seq), 'year_stat_category_id':str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/game_by_game?'

    with Session() as s: 
        r = s.get(url, params=payload, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    table = soup.find_all('table')[3]

    # this is heinous lol
    # TODO: make this more readable
    rows = []
    for val in table.find_all('tr')[3:]: 
        data = []
        for i in val.children:
            if isinstance(i, Tag):
                if 'data-order' in i.attrs:
                    data.append(i.get('data-order'))
                elif i.a:
                    href = i.find_all('a')[-1].get('href')
                    if '?' in href:
                        team_id = href.split('=')[-1]
                        game_id = href.split('?')[0].split('/')[-1]
                        score = i.find_all('a')[-1].string.strip()
                        if 'W' in score: 
                            result = 'win'
                            score = score.replace('W', '').strip()
                        elif 'L' in score: 
                            result = 'loss'
                            score = score.replace('L', '').strip()
                        try:
                            if '(' in score:
                                innings_played = score.split('(')[-1].split(')')[0].strip()
                                extras = 'True'
                                score = score.split('(')[0].strip()
                            else: 
                                innings_played = '9'
                                extras = 'False'
                            data.append(innings_played)
                            data.append(extras)
                            scores = score.split('-')
                            runs_scored = int(scores[0].strip())
                            runs_allowed = int(scores[-1].strip())
                            data.append(runs_scored)
                            data.append(runs_allowed)
                            data.append(runs_scored - runs_allowed)
                        except:
                            continue
                        data.append(result)
                        data.append(score)
                        data.append(game_id)
                        data.append(team_id)
                    else:
                        try: 
                            if href.split('/')[1] == 'teams':
                                season_id = '-'
                                opponent_id = href.split('/')[-1]
                            elif href.split('/')[-1] == 'box_score':
                                game_id = href.split('/')[-2]
                                team_id = school_id
                                score = i.find_all('a')[-1].string.strip()
                                if 'W' in score: 
                                    result = 'win'
                                    score = score.replace('W', '').strip()
                                elif 'L' in score: 
                                    result = 'loss'
                                    score = score.replace('L', '').strip()
                                try:
                                    if '(' in score:
                                        innings_played = score.split('(')[-1].split(')')[0].strip()
                                        extras = 'True'
                                        score = score.split('(')[0].strip()
                                    else: 
                                        innings_played = '9'
                                        extras = 'False'
                                    data.append(innings_played)
                                    data.append(extras)
                                    scores = score.split('-')
                                    runs_scored = int(scores[0].strip())
                                    runs_allowed = int(scores[-1].strip())
                                    data.append(runs_scored)
                                    data.append(runs_allowed)
                                    data.append(runs_scored - runs_allowed)
                                except:
                                    continue
                                data.append(result)
                                data.append(score)
                                data.append(game_id)
                                data.append(team_id)                
                            else:    
                                season_id = href.split('/')[-1]
                                opponent_id = href.split('/')[-2]
                        except:
                            continue
                        if not 'target' in i.find_all('a')[-1].attrs:
                            opponent = i.find_all('a')[-1].contents[0]
                            opponent = opponent.string.replace('</br>', '')
                            opponent = opponent.strip()
                            if opponent[0] == '@': 
                                field = 'away'
                                opponent = opponent.split('@')[-1].strip()
                            elif '@' in opponent:
                                field = 'neutral'
                            else: 
                                field = 'home'
                            data.append(field)
                            data.append(season_id)
                            data.append(opponent_id)
                            data.append(opponent)
                else:
                    date = i.string
                    if date == 'Opponent Totals':
                        continue
                    else:
                        data.append(date)
        if len(data) == len(headers):
            rows.append(data)    
    res = pd.DataFrame(rows, columns=headers)
    res = res.loc[res.field.isin(['away', 'home', 'neutral'])]
    res = _transform_team_stats(res, variant=variant)
    return res

def ncaa_team_results(school, season):
    """
    A function to retreive information about completed games.
    Retrieves data of completed games for a given team from stats.ncaa.org. 
    
    
    Args:
        school: school name (str) or NCAA school_id (int) 
        season: season (int, YYYY) or NCAA season_id (int)
    
    Returns: 
        DataFrame with the following columns:

        game_id, date, field, opponent_name, opponent_id, 
        innings_played, extras, runs_scored, runs_allowed, 
        run_difference, result, school_id, season_id, season
        
    data from stats.ncaa.org. valid 2019 - 2022. 
    """
    data = ncaa_team_game_logs(school=school, season=season, variant='batting')
    res = data[['game_id', 'date', 'field', 'opponent_name', 'opponent_id', \
                'innings_played', 'extras', 'runs_scored', 'runs_allowed', \
                'run_difference', 'result', 'school_id', 'season_id']] 
    return res

def ncaa_team_season_roster(school, season):     
    """
    Transmits GET request to stats.ncaa.org, parses roster information

    Args: 
        school: school name (str) or NCAA school_id (int)
        season: season as (int, YYYY) or NCAA season_id (int)
        valid for 2012 - 2022. 

    Returns: 
        DataFrame containing the following columns:
        -   jersey
        -   stats_player_seq
        -   name
        -   position
        -   class_year
        -   games_played
        -   games_started
        -   height (if 2019)
        
    data from stats.ncaa.org
    """
    school, school_id = lookup_team_info(school)
    season, season_id, batting_id, pitching_id = lookup_season_info(season)

    # doesn't take regular params, have to build url manually
    request_body = 'https://stats.ncaa.org/team/'
    request_body += f'''{str(school_id)}/roster/{str(season_id)}'''

    with Session() as s: 
        r = s.get(request_body, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    res = []
    
    # records from 2019, 2022 seasons contain an additional field: 'height'
    if (season in [2019, 14781, 2022, 15860]): 
        num_values = 7
        col_names = ['jersey','stats_player_seq', 'name', 'position', \
                    'height', 'class_year', 'games_played',\
                    'games_started']
    else:
        num_values = 6
        col_names = ['jersey', 'stats_player_seq', 'name', 'position', \
                    'class_year', 'games_played', 'games_started']
        
    for index, value in enumerate(soup.find_all('td')):
        if index % num_values == 0:
            details = [] 
            res.append(details) # DataFrame deals with extra list for us
        
        if index % num_values == 1:
            # need to get stats_player_seq from href tag
            try: 
                details.append((value.contents[0].get('href')[-7:]))
                details.append(value.contents[0].string)
            except:
                # no id found (occurs when players has 0 games played)
                details.append(None)
                details.append(value.contents[0])
        else:
            try:
                details.append(value.contents[0])
            except:
                details.append(None)
                
    
    try:
        df = pd.DataFrame(res)
        df.columns = col_names
        df.stats_player_seq = df.stats_player_seq.astype('str')
        df.stats_player_seq = df.stats_player_seq.str.replace('=', '')
        df = df.loc[df.stats_player_seq != 'None']
        df.stats_player_seq = df.stats_player_seq.astype('int64')
        df['season'] = season
        df['season'] = df['season'].astype('int16')
        df['season_id'] = season_id 
        df['season_id'] = df['season_id'].astype('int32')
        df['school'] = school
        df['school_id'] = school_id
        df['school_id'] = df['school_id'].astype('int32')
        df.name = df.name.apply(_format_names)
        return df
    except:
        return pd.DataFrame()
def ncaa_team_roster(school, seasons: list[int]) -> pd.DataFrame:
    """
    Args: 
        school/school_id (str/int): name of school or school_id. 
        end: a season/season_id (int/int), valid for 2013 - 2021.
        end must be <= start
        start: a season/season_id (int/int), valid for 2013 - 2021.
        start must be >= end

    Returns:
        concatenated DataFrame using get_roster across [start, end]
        
    data from stats.ncaa.org. valid 2013 - 2022. 
    """
    if len(seasons) ==  1: 
        return ncaa_team_season_roster(school, seasons[0]) 
    else: 
        roster = pd.DataFrame()
        for season in set(seasons):
            time.sleep(random.uniform(0, _TIMEOUT))
            try: 
                new = ncaa_team_season_roster(school, season) 
                if 'height' in new.columns: 
                    new.drop(columns=['height'], inplace=True)
                roster = pd.concat([roster, new])
                try: 
                    roster.drop(columns=['Unnamed: 0'], inplace=True)
                except: 
                    time.sleep(random.uniform(0, _TIMEOUT))
                    continue
            except: 
                time.sleep(random.uniform(0, _TIMEOUT))
                continue
            time.sleep(random.uniform(0, _TIMEOUT))
    return roster

def ncaa_career_stats(stats_player_seq, variant):
    """
    Transmits GET request to stats.ncaa.org, parses career stats

    Args: 
        stats_player_seq (int): the NCAA player_id
        variant (str): the type of stats, either 'batting' or 'pitching
    Returns: 
        DataFrame with the following columns
        
        batting
        -------
        school_id, GP, BA, G, OBPct, SlgPct, R, AB, H, 2B, 3B, TB, HR,
        RBI, BB, HBP, SF, SH, K, OPP DP, CS, Picked, SB, IBB, RBI2out,
        season

        pitching
        --------
        school_id, GP, G, App, GS, ERA, IP, CG, H, R, ER,
        BB, SO, SHO, BF, P-OAB, 2B-A, 3B-A, Bk, HR-A, WP,
        HB, IBB, Inh Run, Inh Run Score, SHA, SFA, Pitches, GO,
        FO, W, L, SV, KL, season

    data from stats.ncaa.org. valid 2013 - 2022.     
    """
    # craft GET request to NCAA site
    season = lookup_seasons_played(stats_player_seq)[0]
    season, season_id, batting_id, pitching_id = lookup_season_info(season)
    
    if variant == 'batting': 
        year_stat_category_id = batting_id
    else: 
        year_stat_category_id = pitching_id
    
    payload = {'id':str(season_id), 'stats_player_seq':str(stats_player_seq), \
        'year_stat_category_id':str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/index'
    # send request
    try:
        with Session() as s:
            r = s.get(url, params=payload, headers=_HEADERS)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    try: 
        # parse data
        soup = BeautifulSoup(r.text, features='lxml')
        table = soup.find_all('table')[2]

        # get table headers
        headers = []
        for val in table.find_all('th'):
            headers.append(val.string.strip())

        # get table data
        rows = []
        row = []
        for val in table.find_all('td'): 
            # data is also encoded in data-order attr of td elements
            if 'data-order' in val.attrs:
                row.append(val['data-order'])
            elif val.a is not None:
                row.append(val.a.attrs['href'].split('/')[2])
            elif val.string.strip() != 'Career' and 'width' not in val.attrs:
                if row != []:
                    rows.append(row)
                row = []
                row.append(val.string.strip())
            else:
                if val.string.strip() != 'Career':
                    row.append(val.string.strip())

        # Turn into DataFrame
        df = pd.DataFrame(rows)
        df.columns = headers
        df = _transform_team_stats(df, variant=variant)
        return df
    except: 
        print('no records found')
        return pd.DataFrame()

def _format_names(original):
    """
    A helper function to turn names from "Last, First" to "First Last"
    
    Args: 
        original (str): the name to reformat. must be in form "Last, First"
    Returns: 
        str of format "First Last"
        
    Examples: 
        _format_names("Blumenfeld, Nathan")
        "Nathan Blumenfeld"
    """
    try: 
        split = original.split(',')
        split.reverse()
        res = ' '.join(split).strip().title()
    except:
        res = np.nan
    return res

def _eliminate_dashes(df):
    """
    A helper function to replace the weird dashes the NCAA uses with 0.00

    Args: 
        df (DataFrame): 
    Returns:
        Dataframe with dashes replaced by 0.00 (not a copy!)
    """
    formats = ['None', '<NA>', '', ' ', '-', '--', '---']
    for i in formats: 
        df.replace(i, 0.00, inplace=True)
    return df 

def _transform_team_stats(df, variant):
    """
    A helper function to transform raw data obtained with get_career_stats. 
    
    Args:
        df: DataFrame output from get_career_stats function 
        variant: type of stats to transform, either 'batting' or 'pitching'
    
    Returns:
        DataFrame 
    """
    df = _eliminate_dashes(df)
    cols = df.columns

    # format player names 
    if 'Player' in cols: 
        df.rename(columns={'Player':'name'}, inplace=True)
        cols = df.columns
    if 'name' in cols: 
        df['name'] = df['name'].apply(_format_names)
        df['name'] = df['name'].astype('string')

    # need to do this after name formatting, which relies on commas
    try:
        df = df.replace(',','', regex=True)
    except:
        pass
    try:
        df.fillna(value=0.00, inplace=True)
    except:
        pass

    # set dtypes
    data_types = {'int32':['opponent_id', 'season_id', 'school_id', 'innings_played', 
    'game_id', 'runs_scored', 'runs_allowed', 'run_difference', 'Pos'
    'season', 'GP', 'GS', 'BB', 'Jersey', 'DP', 'H', 'DP'
    'R', 'ER', 'SO', 'TB', '2B', '3B', 'HR', 'RBI', 'R', 'AB', 'HBP', 
    'SF', 'K', 'SH', 'Picked', 'SB', 'IBB', 'CS', 'OPP DP', 'SHO', 'BF', 
    'P-OAB', '3B-A', '2B-A', 'Bk', 'HR-A', 'WP', 'IBB', 'Inh Run',
    'Inh Run Score', 'SHA', 'SFA', 'GO', 'FO', 'W', 'L', 'HB', 'SV', 
    'KL', 'pickoffs', 'OrdAppeared', 'App', 'season', 'GDP'], 'bool':['extras'], 
    'float':['ERA', 'IP'], 'string':['Yr', 'Pos', 'date', 'Year']}

    for i in data_types.keys():
        for j in data_types[i]:
            if j in cols:
                df[j] = df[j].astype(i)

    # we're going to calculate OBP/BA/SLG ourselves
    # G and RBI2out are unreliable
    drops = ['OBPct', 'BA', 'SlgPct', 'RBI2out', 'G']
    for i in drops: 
        if i in cols: 
            df.drop(columns=[i], inplace=True)
    
    # keeping as 64 byte to leave space for new potential player uuids
    if 'stats_player_seq' in cols: 
        df.stats_player_seq = df.stats_player_seq.astype('string')
        df.stats_player_seq = df.stats_player_seq.str.replace(r'\D+', '')
        df.stats_player_seq = df.stats_player_seq.astype('int64')
    # TODO: consider making these datetime
    if 'date' in cols: 
        df['date'] = df['date'].astype('string')
        df['season'] = df['date'].str[-4:]
        df['season'] = df['season'].astype('int32')
    if 'Year' in cols: 
        df['Year'] = df['Year'].astype('string')
        df['season'] = df['Year'].str[:4]
        df['season'] = df['season'].astype('int32')
        df.drop(columns=['Year'], inplace=True)
    # TODO: add position list logic from database utils
    if 'Pos' in cols:
        df = df.rename(columns={'Pos':'pos'}) 
        cols = df.columns
    # TODO: semantic choice: 'school' or 'team'?
    if 'Team' in cols:
        df = df.rename(columns={'Team':'school_id'}) 
        cols = df.columns                
    if 'Pitches' in cols: 
        df = df.rename(columns={'Pitches':'pitches'}) 
        df['pitches'] = df['pitches'].astype('string')
        df['pitches'] = df['pitches'].astype('float')
        df['pitches'] = df['pitches'].astype('int32')
    if 'ERA' in cols: 
        df['ERA'] = df['ERA'].round(4)
    if 'IP' in cols: 
        df['IP'] = df['IP'].round(4)
    return df
    
def lookup_season_ids(season):
    """
    A function that finds the year_stat_category_ids of a given season.

    Args: 
        season (int, YYYY)

    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    season_row = _SEASON_LU_DF.loc[_SEASON_LU_DF['season'] == season]
    season_id = season_row.values[0][1]
    batting_id = season_row.values[0][2]
    pitching_id = season_row.values[0][3]  
    return int(season_id), int(batting_id), int(pitching_id)

def lookup_season_ids_reverse(season_id):
    """
    A function that finds the year_stat_category_ids and season of a season_id.
    
    Args: 
        season_id (int): NCAA season_id
    
    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    season_row = _SEASON_LU_DF.loc[_SEASON_LU_DF['season_id'] == season_id]
    season = season_row['season'].values[0]
    batting_id = season_row['batting_id'].values[0]
    pitching_id = season_row['pitching_id'].values[0]
    return int(season), int(batting_id), int(pitching_id)

def lookup_season_id(season):
    """
    A function that finds the year_stat_category_id's for a given season.
    
    Args: 
        season (int, YYYY)
        
    Returns: 
        season_id as an int
    """
    season_row = _SEASON_LU_DF.loc[_SEASON_LU_DF['season'] == season]
    season_id = season_row.values[0][1]
    return int(season_id)

def lookup_seasons_played(stats_player_seq): 
    """
    A function to find the final and debut seasons of a given player. 
    
    Args: 
        stats_player_seq (int): NCAA player_id

    Returns: 
        tuple of ints: (debut season, most recent season)
    
    """
    df = _PLAYERS_HISTORY_LU_DF
    row = df.loc[df.stats_player_seq == stats_player_seq]
    return int(row['debut_season'].values[0]), int(row['season_last'].values[0])
    
def lookup_school_id(school):
    """
    A function to find a school's id from it's name.

    Args: 
        school (str): the name of the school
        
    Returns:
        school_id as int
    
    Examples: 
        lookup_school_id("cornell")
        >>> 167
    """
    school_row = _SCHOOL_ID_LU_DF.loc[(_SCHOOL_ID_LU_DF.ncaa_name == school)]
    if len(school_row) == 0:
        # try to search against bd_name
        school_row = _SCHOOL_ID_LU_DF.loc[(_SCHOOL_ID_LU_DF.bd_name == school)]
    if len(school_row) == 0: 
        return f'''could not find school {school}'''
    else: 
        return int(school_row['school_id'].values[0])
        
def lookup_school_reverse(school_id):
    """
    A function to find a school's name from a school_id. 

    Args:
        school_id as int
    
    Returns: 
        school (str): the name of the school
        
    Examples: 
        lookup_school_reverse(167)
        >>> "Cornell"
    """
    school_row = _SCHOOL_ID_LU_DF.loc[(_SCHOOL_ID_LU_DF.school_id == school_id)]
    if len(school_row) == 0: 
        return f'''could not find school {school_id}'''
    else: 
        return str(school_row['ncaa_name'].values[0])
        
def lookup_player_id(player_name, school):
    """
    A function to find a player's id from their name and school.
    
    Args: 
        player_name (str): name of player to lookup
        school: either the ncaa_name of the school (str) of the player
        
    Returns: 
        The player_id of the player as an int

    Examples: 
        lookup_player_id("Jake Gelof", "Virginia")
        >>> 2486499
    
    """
    player_row = _PLAYER_ID_LU_DF.loc[_PLAYER_ID_LU_DF.name == player_name.title()]
    player_row = player_row.loc[player_row.school == school]
        
    if len(player_row) == 0:
        return f'''could not find player {player_name}'''
    else: 
        return int(player_row['stats_player_seq'].values[0])
        
def lookup_player_reverse(player_id, season):
    """
    A function to find a player's name and school from their player_id. 
    
    Args: 
        player_name (str): name of player to lookup
        school: either the ncaa_name of the school (str) of the player
        
    Returns: 
        The name and school of the player as a strings

    Examples: 
        lookup_player_id("Jake Gelof", "Virginia")
        >>> 2486499
    
    """
    df = _ROSTERS_DF
    player_row = df.loc[(df.stats_player_seq == player_id) & (df.season == season)]        
    if len(player_row) == 0:
        return f'''could not find player {player_id}'''
    else: 
        return str(player_row['name'].values[0]), str(player_row['school'].values[0]), int(player_row['school_id'].values[0])

def lookup_team_info(x): 
    """
    a function to handle the school/school_id input types
 
    """
    if type(x) is int: 
        school_id = x 
        school = lookup_school_reverse(school_id)
    elif type(x) is str: 
        school_id = lookup_school_id(x)
        school = x 
    return school, school_id 

def lookup_season_info(x): 
    """
    handling season/season_id input types

    """
    season = 0
    season_id = 0
    batting_id = 0
    pitching_id = 0
    if len(str(x)) == 4: 
        season = x 
        season_id, batting_id, pitching_id = lookup_season_ids(x)
    # get season_id from lookup table
    elif len(str(x)) == 5: 
        season_id = x 
        season, batting_id, pitching_id  = lookup_season_ids_reverse(x)
    else:
        print('Something went wrong.')
        pass
    return season, season_id, batting_id, pitching_id

def _adj_position(pos_list):
    pos = pos_list[-1]
    if pos in ['1B', '2B', 'SS', '3B', 'UT']:
        return 'INF'
    elif pos in ['LF', 'CF', 'RF']:
        return 'OF'
    elif pos in ['OF', 'INF', 'C', 'P', 'DH']:
        return pos
    else: 
        return 'INF'
    pass

def _adj_position_pitching(pos):
    if pos in ['P/C', 'ss/p', 'LF/P', 'p', 'P']:
        return 'P'
    else:
        return 'notP'
    pass

def _pos_list(pos):
    res = []
    for i in pos.split('/'):
        res.append(i.upper())
    return res 
    pass
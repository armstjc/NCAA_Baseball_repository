library(baseballr)
library(dplyr)
library(glue)


seasons = 2023:2024

for (s in seasons) {
  print(glue::glue("Donwloading all the play-by-play data for the NCAA Baseball season of {s}"))
  df = read.csv(glue::glue("game_stats/player/batting_game_stats/csv/{s}_batting_01.csv"))
  df2 = read.csv(glue::glue("game_stats/player/batting_game_stats/csv/{s}_batting_02.csv"))
  main_df <- union(df,df2)
  game_list = main_df$game_id
  #print(list)
  game_list = unique(game_list)
  game_list_len = length(game_list)
  count = 0
  
  l_range = 0:8299
  
  for (i in l_range){
    count = count + 1
    
    gid = game_list[i]
    print(glue::glue("{i}/{game_list_len}: {gid}"))
    filename <- glue::glue("pbp/games/{gid}.csv")
    stats <- ncaa_baseball_pbp(glue::glue('https://stats.ncaa.org/game/play_by_play/{gid}'))
    write.csv(stats,filename,row.names=FALSE)
    Sys.sleep(4)
  }
}

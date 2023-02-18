library(baseballr)
library(glue)
team_ids_df = read.csv('team_ids.csv')
team_ids_list = team_ids_df$school_id

seasons = 2023:2023
for (i in seasons){
  for (j in team_ids_list){
    try({
    df <- ncaa_baseball_roster(teamid=j,team_year=i)
    print(df)
    filename <- glue::glue("TeamRosters/teams/{i}_{j}.csv")
    write.csv(df,filename,row.names = FALSE)
    Sys.sleep(4)
    })
  }
}


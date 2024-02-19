library(baseballr)
library(glue)
team_ids_df = read.csv('team_ids.csv')
team_ids_list = team_ids_df$school_id
team_count = length(team_ids_df)
count = 0

seasons = 2023:2023
for (i in seasons){
  for (j in team_ids_list){
    try({
    df <- ncaa_baseball_roster(team_id=j,year=i)
    print(df)
    filename <- glue::glue("TeamRosters/teams/{i}_{j}.csv")
    write.csv(df,filename,row.names = FALSE)
    Sys.sleep(4)
    })
  }
}


# df <- ncaa_baseball_roster(team_id=30263,year=2024)
# print(df)
# filename <- glue::glue("TeamRosters/teams/{i}_{j}.csv")
# write.csv(df,filename,row.names = FALSE)
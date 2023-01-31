#from collegebaseball.download_utils import download_rosters
from collegebaseball.guts_utils import update_rosters
import pandas as pd

# print('updating rosters for D1.')
# update_rosters(2023,1)
# print('updating rosters for D2.')
# update_rosters(2023,2)
# print('updating rosters for D3.')
# update_rosters(2023,3)

df = pd.read_parquet('collegebaseball/data/rosters_all.parquet')
df.to_csv('test.csv')
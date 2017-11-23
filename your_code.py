import numpy as np
import pandas  as pd

df = pd.read_csv('dailyinfo.csv', index_cols='date_')

print (df.tail())

######## your code start from here ########
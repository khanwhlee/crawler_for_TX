import numpy as np
import pandas  as pd
import pickle

pickle_in = open('txfuture_updated.pickle', 'rb')

df = pickle.load(pickle_in)

print (df)

######## your code start from here ########
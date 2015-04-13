__author__ = 'ssatpati'


import pandas as pd
import statsmodels.api as sm
import pylab as pl
import numpy as np


fname = "leaderboards_7308448.csv"


# read csv into dataframe
df = pd.read_csv(fname)

print(df.head())
print("\n\n")
print(df.describe())


if __name__ == '__main__':
    pass
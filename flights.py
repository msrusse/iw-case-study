#! /usr/bin/python3

from numpy.lib.index_tricks import AxisConcatenator
from numpy.lib.type_check import nan_to_num
import pandas as pd
import numpy as np

def determineDistanceGroup(row):
    distance = int(row['DISTANCE'].split(' ')[0])
    if distance < 101:
        return '0 - 101 miles'
    elif distance % 100 != 0:
        return '%s01-%s00 miles' % (int(distance/100), int(distance/100)+1)
    else:
        return '%s01-%s00 miles' % (int(distance/100)-1, int(distance/100))

def determineNextDayArrival(row):
    return int(2400-row['DEPTIME'] < 2400-row['ARRTIME'])

all_data_df = pd.read_csv('flights.txt', sep='|')
airline_df = all_data_df[['AIRLINECODE', 'AIRLINENAME']].drop_duplicates().reset_index(drop=True)
flights_df = all_data_df.drop(['AIRLINENAME', 'ORIGAIRPORTNAME', 'ORIGINCITYNAME', 'ORIGINSTATE', 'ORIGINSTATENAME', 'DESTAIRPORTNAME', 'DESTCITYNAME', 'DESTSTATE', 'DESTSTATENAME'], axis=1)


flights_df['DISTANCEGROUP'] = flights_df.apply(determineDistanceGroup, axis=1)
flights_df['DEPDELAYGT15'] = flights_df['DEPDELAY'].apply(lambda x: 1 if x > 15 else 0)
flights_df['NEXTDAYARR'] = flights_df.apply(lambda x: determineNextDayArrival if not x['CANCELLED'] else np.nan, axis=1)
airline_df = flights_df[['AIRLINECODE', 'AIRLINENAME']].reset_index()
airline_df['NOTES'] = flights_df['AIRLINENAME'].apply(lambda x: x[x.find('(')+1:x.find(')')] if '(' in x else None)
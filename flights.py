#! /usr/bin/python3

import pandas as pd
import numpy as np
import psycopg2, sys, json
import progressbar as pb

param_dic = {
    "host"      : "iw-recruiting-test.cygkjm9anrym.us-west-2.rds.amazonaws.com",
    "database"  : "tests_data_engineering",
    "user"      : "candidate1135",
    "password"  : "ODAaW1C5WPAFVWmt"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn

def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

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

def getOriginState(row):
    if row['ORIGINSTATE'] is np.nan:
        state = row['ORIGAIRPORTNAME'].split(':')[0][-2:]
        return state
    return row['ORIGINSTATE']

def getDepartureState(row):
    if row['DESTSTATE'] is np.nan:
        state = row['DESTAIRPORTNAME'].split(':')[0][-2:]
        return state
    else: 
        return row['DESTSTATE']

def getOriginStateName(row):
    state = row['STATE']
    if state == 'OK':
        return 'Oklahoma'
    elif state == 'KS':
        return 'Kansas'
    else:
        return row['ORIGINSTATENAME']

def getDepartureStateName(row):
    state = row['STATE']
    if state == 'OK':
        return 'Oklahoma'
    elif state == 'KS':
        return 'Kansas'
    else:
        return row['DESTSTATENAME']

def determineBool(val):
    lower_val = str(val.lower())
    if lower_val == 'true' or lower_val == 't' or lower_val == '1':
        return 1
    else:
        return 0

# Read in the file to a main dataframe
all_data_df = pd.read_csv('flights.txt', sep='|')

# Get individual dataframes
airline_df = all_data_df[['AIRLINECODE', 'AIRLINENAME']].drop_duplicates().reset_index(drop=True)
origin_states_df = all_data_df[['ORIGINSTATE', 'ORIGINSTATENAME', 'ORIGAIRPORTNAME']]
dest_states_df = all_data_df[['DESTSTATE', 'DESTSTATENAME', 'DESTAIRPORTNAME']]
origin_airports_df = all_data_df[['ORIGINAIRPORTCODE', 'ORIGAIRPORTNAME', 'ORIGINCITYNAME', 'ORIGINSTATE']]
dest_airports_df = all_data_df[['DESTAIRPORTCODE', 'DESTAIRPORTNAME', 'DESTCITYNAME', 'DESTSTATE']]
departures_df = all_data_df[['TRANSACTIONID', 'ORIGINAIRPORTCODE', 'CRSDEPTIME', 'DEPTIME', 'DEPDELAY', 'TAXIOUT', 'WHEELSOFF']]
arrivals_df = all_data_df[['TRANSACTIONID', 'DESTAIRPORTCODE', 'CRSARRTIME', 'ARRTIME', 'ARRDELAY', 'WHEELSON', 'TAXIIN', 'DEPTIME']]
flights_df = all_data_df[['TRANSACTIONID', 'FLIGHTDATE', 'AIRLINECODE', 'FLIGHTNUM', 'TAILNUM', 'CANCELLED', 'DIVERTED', 'CRSELAPSEDTIME', 'ACTUALELAPSEDTIME', 'DISTANCE']]
del all_data_df

# Alters the airline names to remove notes and code
airline_df['NOTES'] = airline_df['AIRLINENAME'].apply(lambda x: x[x.find('(')+1:x.find(')')] if '(' in x and '(1)' not in x else None)
airline_df['AIRLINENAME'] = airline_df['AIRLINENAME'].apply(lambda x: x.split(':')[0] if x else x)

# Determine Missing States along with Combine and select unique states, dropping NaN's
# Get missing state ids
origin_states_df['STATE'] = origin_states_df.apply(getOriginState, axis=1)
# Get missing state names
origin_states_df['STATENAME'] = origin_states_df.apply(getOriginStateName, axis=1)
# Get missing destination state ids
dest_states_df['STATE'] = dest_states_df.apply(getDepartureState, axis=1)
# Get missing destination state names
dest_states_df['STATENAME'] = dest_states_df.apply(getDepartureStateName, axis=1)
# Drop origin information, making it just STATE and STATENAME for combining with destinations
origin_states_df = origin_states_df.drop(['ORIGINSTATE', 'ORIGINSTATENAME', 'ORIGAIRPORTNAME'], axis=1)
# Drop dest information, making it just STATE and STATENAME for combining with origin
dest_states_df = dest_states_df.drop(['DESTSTATE', 'DESTSTATENAME', 'DESTAIRPORTNAME'], axis=1)
# Mergeds the origin and destination states together and drops duplicates. I know I can just do this on one of the two, because flights are round trip, but I don't trust it...
all_states_df = pd.concat([origin_states_df, dest_states_df], ignore_index=True, sort=False).dropna().drop_duplicates().reset_index(drop=True)

# Combine Airports, select uniques, and drop city/state from name
airport_df_column_names = ['AIRPORTCODE', 'AIRPORTNAME', 'CITYNAME', 'STATE']
# Renames the origin and dest columns to be the same, for merging
origin_airports_df.columns = airport_df_column_names
dest_airports_df.columns = airport_df_column_names
# Merges the origin and dest airports and only keeps unique values, again I know only using one should work, but I still don't trust it...
all_airports_df = pd.concat([dest_airports_df, origin_airports_df], ignore_index=True, sort=False).drop_duplicates().sort_values('AIRPORTCODE').reset_index(drop=True)
# Removes the additional information from AIRPORTNAME (the city and state)
all_airports_df['AIRPORTNAME'] = all_airports_df['AIRPORTNAME'].apply(lambda x: x.split(': ')[1] if ': ' in x else x)

# Determine Departure Delay
departures_df['DEPDELAYGT15'] = departures_df['DEPDELAY'].apply(lambda x: 1 if x > 15 else 0)

# Determine Next Day Arrival
arrivals_df['NEXTDAYARR'] = arrivals_df.apply(determineNextDayArrival, axis=1)

# Determine Distance group and fix data issues
flights_df['DISTANCEGROUP'] = flights_df.apply(determineDistanceGroup, axis=1)
# Removes '-' from in front of some flight numbers and replaces unknowns with null
flights_df['TAILNUM'] = flights_df['TAILNUM'].apply(lambda x: x.split('-')[1] if '-' in str(x) else np.nan if 'NKNO' in str(x) else x)
# Changes all the True or T values to 1 and False or f to 0
flights_df['CANCELLED'] = flights_df['CANCELLED'].apply(lambda x: determineBool(x))
# Changes all the True or T values to 1 and False or f to 0
flights_df['DIVERTED'] = flights_df['DIVERTED'].apply(lambda x: determineBool(x))
# Changes the int storage of dates into a datetime
flights_df['FLIGHTDATE'] = flights_df['FLIGHTDATE'].apply(lambda x: '%s-%s-%s' % (str(x)[0:4], str(x)[4:6], str(x)[6:]))

all_states_dict = all_states_df.to_json(orient='records')
airlines_dict = json.loads(airline_df.to_json(orient='records'))

# Connecting to the database
conn = connect(param_dic)

bar = pb.ProgressBar()
print('Inserting states...')
for airline in bar(airlines_dict):
    query = """
    INSERT INTO candidate1135."DIM_AIRLINES"(AIRLINECODE, AIRLINENAME, AIRLINENOTES) values ('%s', '%s', '%s')
    """ % (airline['AIRLINECODE'], airline['AIRLINENAME'], airline['NOTES'])
    single_insert(conn, query)

conn.close()
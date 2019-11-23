import pandas
import argparse
from pathlib import Path
from time import perf_counter

parser = argparse.ArgumentParser(description='Log parser')
parser.add_argument('method', type=str, help='Parse method (i,d,e,w,s)  like idc')
parser.add_argument('infile', type=Path, help='Log filename')
parser.add_argument('outfile', type=Path, help='Output filename')
argv = parser.parse_args()
start = perf_counter()
if __name__ == '__main__':
    csv = pandas.read_csv(argv.infile, sep='\t', header=None,
                          names=['date', 'server_id', 'dur_ttl', 'dur_outer', 'ip',
                                 'token', 'method', 'url', 'referrer'],
                          usecols=['date', 'dur_ttl', 'dur_outer', 'ip',
                                   'token', 'method', 'url', 'referrer'],
                          dtype={'date': str, 'server_id': str, 'dur_ttl': float, 'dur_outer': float, 'ip': str,
                                 'token': str, 'method': str, 'url': str, 'referrer': str},
                          engine='c', parse_dates=['date'], infer_datetime_format=True, cache_dates=True,
                          encoding='utf-8')
    logged = csv[csv['token'] != '-'].rename(columns={'dur_ttl': 'l_dur_ttl', 'dur_outer': 'l_dur_outer'})

    # Code by date
    by_date = csv.resample('D', on='date').sum().join(logged.resample('D', on='date').sum())
    by_date.fillna(0).to_csv('by_date.csv', sep=';')

    # Code by IP
    logged_ip = logged[['ip', 'l_dur_ttl', 'l_dur_outer']].groupby(['ip']).sum()
    by_ip = csv.groupby(['ip']).sum()
    by_ip_done = pandas.concat([by_ip, logged_ip], axis=1, sort=False)
    by_ip_done.fillna(0).to_csv('by_ip.csv', sep=';')

    # Code by Week
    by_week_df = pandas.DataFrame(columns=['dow'])
    by_week = csv.resample('30T', on='date').agg(['sum', 'median', 'mean','count'])
    by_week['dow'] = by_week.index.weekday
    by_week.index = by_week.index.time
    by_week['time'] = by_week.index
    by_week.sort_values(by=['dow',
                            'time']).drop(columns=['count'], level=1).fillna(0).to_csv('by_week.csv', sep=';')
    print(by_week.info())
    print(csv.info())
    print('Done in :', perf_counter() - start)
    exit(0)

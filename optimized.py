import argparse
import re
from pathlib import Path
from time import perf_counter

import pandas

parser = argparse.ArgumentParser(description='Log parser')
parser.add_argument('infile', type=Path, help='Log filename')
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

    # Code by Date
    by_date = csv.resample('D', on='date').sum().join(logged.resample('D', on='date').sum())
    by_date.fillna(0).to_csv('by_date.csv', sep=';')

    # Code by IP
    logged_ip = logged[['ip', 'l_dur_ttl', 'l_dur_outer']].groupby(['ip']).sum()
    by_ip = csv.groupby(['ip']).sum()
    by_ip_done = pandas.concat([by_ip, logged_ip], axis=1, sort=False)
    by_ip_done.fillna(0).to_csv('by_ip.csv', sep=';')

    # Code by Week
    by_week_df = pandas.DataFrame(columns=['dow'])
    by_week = csv.resample('30T', on='date').agg(['sum', 'median', 'mean', 'count'])
    by_week['dow'] = by_week.index.weekday
    by_week.index = by_week.index.time
    by_week['time'] = by_week.index
    by_week.sort_values(by=['dow',
                            'time']).groupby(['dow', 'time']).sum().fillna(0).to_csv('by_week.csv', sep=';')

    # Code by Session
    csv['date_start'] = csv['date']
    csv['date_end'] = csv['date']
    csv['count'] = csv['method']
    by_session = csv.groupby('token')['date_start', 'date_end',
                                      'method', 'dur_ttl', 'dur_outer'].agg({'date_start': 'min', 'date_end': 'max',
                                                                             'method': 'count', 'dur_ttl': 'sum',
                                                                             'dur_outer': 'sum'}).to_csv('by_session'
                                                                                                         '.csv',
                                                                                                         sep=';')

    # Code by Endpoint
    def regex(string):
        return re.findall(r'^http:\/\/[a-z]+.+\/+[a-z]+\/', string)

    csv['endpoint'] = csv['url'].apply(regex)
    print(csv.groupby('endpoint')['dur_ttl', 'dur_outer'])
    #csv['endpoint'] =
    #csv.groupby('endpoint').to_csv('endpoint.csv', sep=';')
    print('Done in :', perf_counter() - start)
    exit(0)

from __future__ import annotations
import time
import prettytable as pt
from dateutil.parser import parse
from statistics import mean, median
from pathlib import Path
import argparse
import re

loop_start = time.perf_counter()
w_l = list()
collect = {'ip': dict(), 'date': dict(), 'week': list(), 'session': dict(), 'endpoint': dict()}

for i in range(24):
    # Создается массив для значений по неделям (1 столбец - Timedelta, 2-8 - дни недели)
    collect['week'].append(
        ["0" + str(i) + ":00 - 0" + str(i) + ":30" if i < 10 else str(i) + ":00 - " + str(i) + ":30", {}, {}, {}, {},
         {}, {}, {}])
    collect['week'].append(["0" + str(i) + ":30 - " + (
        str(i + 1) if i + 1 == 10 else "0" + str(i + 1)) + ":00" if i < 10 else str(i) + ":30 - " + str(
        i + 1 if i < 23 else "00") + ":00", {}, {}, {}, {}, {}, {}, {}])


# Функция поэлементного сложения двух списков (с выбором начальной позиции)
def vec_add(i, vec1: list, vec2: list):
    vec3 = list()
    for k in range(len(vec1)):
        if not k < i:
            if type(vec1[k]) == list:
                vec3.append(vec1[k] + (vec2[k] if type(vec2[k]) == list else [vec2[k]]))
            elif type(vec1[k]) == str:
                vec3.append(vec2[k])
            else:
                try:
                    vec3.append(vec1[k] + vec2[k])
                except:
                    print(f'Vector 1: {vec1}')
                    print(f'Vector 2: {vec2}')
                    print(f'Position {k}')
                    print(f'Type of error entry: {type(vec1[k])}')
                    print(f'Error entry: {vec1[k]}')
                    print(f'Vector 2 type : {type(vec2[k])}')
                    print(f'Vector 2 entry : {vec2[k]}')
                    exit(0)
        else:
            vec3.append(vec1[k])
    return vec3


'''
Класс оперирующий очередью выполнения (Список в котором хранятся указатели на классы реализующие аггрегацию)
Т.к. файл читается построчно, то классам передаётся уже распарсенная строка. Они записывают результат своей
работы в глобально объявленную область памяти 'collect'
'''
#TODO Улучшить производительность (многопоток?)

class WorkList:
    def create(w_l):
        w_l = list()

    def add(w_l, handler):
        w_l.append(handler)

    # Финализация, в общем нужна только в агрегации по дням недели
    def finalize(w_l):
        for i in w_l:
            i.finalize(collect)

    def work(w_l, data):
        for i in w_l:
            i.parse(data, collect)

    def prepare_table(w_l):
        string_table = ''
        for i in w_l:
            table = i.table_constructor()
            string_table = string_table + table.get_string() + '\n'
        return string_table


class ByIP:
    def parse(self, data, collect):
        # Распаковка данных (Для наглядности)
        date, time, server_id, dur_ttl, dur_out, ip, user_id, session_id, method, url, referrer = data
        try:
            # Проверка есть ли элемент с ключом равным ip-адресу
            type(collect['ip'][ip])
        except:
            # Создаём ключ если таковой не найден
            collect['ip'][ip] = list([0, 0, 0, 0, 0, 0])
        finally:
            # Записываем в него данные
            if session_id == '':
                vector = [1, 0, dur_ttl, 0, dur_out, 0]
            else:
                vector = [1, 1, dur_ttl, dur_ttl, dur_out, dur_out]
            collect['ip'][ip] = vec_add(0, collect['ip'][ip], vector)

    def finalize(self, collect):
        print('By IP: Nothing to finalize')

    def table_constructor(self):
        a = pt.PrettyTable(
            ["IP", "Total queries", "Legal queries", "Query time", "Legal query time", "External total time",
             "External legal time"])
        for i in collect['ip']:
            a.add_row([i] + collect['ip'][i])
        return a


class ByDate:

    def parse(self, data, collect):
        # Распаковка данных (Для наглядности)
        date, time, server_id, dur_ttl, dur_out, ip, user_id, session_id, method, url, referrer = data
        try:
            # Проверка есть ли элемент с ключом равным ip-адресу
            type(collect['date'][date])
        except:
            # Создаём ключ если таковой не найден
            collect['date'][date] = list([0, 0, 0, 0, 0, 0])
        finally:
            # Записываем в него данные
            if session_id == '':
                vector = [1, 0, dur_ttl, 0, dur_out, 0]
            else:
                vector = [1, 1, dur_ttl, dur_ttl, dur_out, dur_out]
            collect['date'][date] = vec_add(0, collect['date'][date], vector)

    def finalize(self, collect):
        print('By Date: Nothing to finalize')

    def table_constructor(self):
        a = pt.PrettyTable(
            ["Date", "Total queries", "Legal queries", "Query time", "Legal query time", "External total time",
             "External legal time"])
        for i in collect['date']:
            a.add_row([i] + collect['date'][i])
        return a


class ByWeek:
    def parse(self, data, collect):
        date, time, server_id, dur_ttl, dur_out, ip, user_id, session_id, method, url, referrer = data
        time = time.split(':')
        timedelta = 2 * int(time[0]) + divmod(int(time[1]), 30)[0]
        weekday = parse(date).isoweekday()
        try:
            type(collect['week'][timedelta][weekday][date])
        except:
            collect['week'][timedelta][weekday][date] = list()
        finally:
            collect['week'][timedelta][weekday][date].append(dur_ttl)

    def finalize(self, collect):
        print('By Week: Finalizing data')
        for timedelta in range(24 * 2):
            for weekday in range(1, 8):
                for values in collect['week'][timedelta][weekday].values():
                    if not len(values) == 0:
                        collect['week'][timedelta][weekday] = [sum(values), mean(values), median(values)]
                    else:
                        collect['week'][timedelta][weekday] = [0, 0, 0]

    def table_constructor(self):
        a = pt.PrettyTable(["Timedelta", "1", "2", "3", "4", "5", "6", "7"])
        for i in collect['week']:
            a.add_row(i)
        return a


class BySession:
    def parse(self, data, collect):
        date, time, server_id, dur_ttl, dur_out, ip, user_id, session_id, method, url, referrer = data
        if not user_id == '':
            vector = [date + ' ' + time, date + ' ' + time, 1, dur_ttl, dur_out]
            try:
                type(collect['session'][user_id])
            except:
                collect['session'][user_id] = {}
            try:
                type(collect['session'][user_id][session_id])
            except:
                collect['session'][user_id][session_id] = vector
            finally:
                collect['session'][user_id][session_id] = vec_add(1, collect['session'][user_id][session_id], vector)

    def finalize(self, collect):
        print('By Session: Nothing to finalize')

    def table_constructor(self):
        a = pt.PrettyTable(
            ["User ID", "Session ID", "Session start", "Session end", "Queries", "Total time", "Outer time"])
        for i in collect['session']:
            for k in collect['session'][i]:
                a.add_row([i] + [k] + collect['session'][i][k])
        return a


class ByEndpoint:
    def parse(self, data, collect):
        date, time, server_id, dur_ttl, dur_out, ip, user_id, session_id, method, url, referrer = data
        regex = re.match(r'^http:\/\/[a-z]+.+\/+[a-z]+\/', url)
        urlregex = ((url[regex.start():regex.end()]) if not url == "-" else "-")
        if session_id == '':
            vector = [[url], 1, 0, dur_ttl, 0, dur_out, 0]
        else:
            vector = [[url], 1, 1, dur_ttl, dur_ttl, dur_out, dur_out]
        try:
            type(collect['endpoint'][urlregex])
        except:
            collect['endpoint'][urlregex] = {}
        try:
            type(collect['endpoint'][urlregex][method])
        except:
            collect['endpoint'][urlregex][method] = [[url], 0, 0, 0, 0, 0, 0]
        finally:
            if url in collect['endpoint'][urlregex][method][0]:
                collect['endpoint'][urlregex][method] = vec_add(1, collect['endpoint'][urlregex][method], vector)
            else:
                collect['endpoint'][urlregex][method] = vec_add(0, collect['endpoint'][urlregex][method], vector)

    def finalize(self, collect):
        print('By Endpoint: Nothing to finalize')

    def table_constructor(self):
        a = pt.PrettyTable(["URL Regex", "Method", "URL's", "Queries", "Legal Queries", "Total time",
                            "Total legal time", "Outer time", "Outer legal time"])
        for urlregex in collect['endpoint']:
            for method in collect['endpoint'][urlregex]:
                urls = collect['endpoint'][urlregex][method][0]
                if len(urls) > 1:
                    vector = collect['endpoint'][urlregex][method].copy()
                    vector.pop(0)
                    a.add_row([urlregex] + [method] + [''] + vector)
                    for i in collect['endpoint'][urlregex][method][0]:
                        a.add_row(['', '', [i], '', '', '', '', '', ''])
                else:
                    a.add_row([urlregex] + [method] + collect['endpoint'][urlregex][method])
        return a


parser = argparse.ArgumentParser(description='Log parser')
parser.add_argument('method', type=str, help='Parse method (i,d,e,w,s)  like idc')
parser.add_argument('infile', type=Path, help='Log filename')
parser.add_argument('outfile', type=Path, help='Output filename')
argv = parser.parse_args()

WorkList().create()
for i in argv.method:
    if i == 'i':
        WorkList.add(w_l, ByIP())
    if i == 'd':
        WorkList.add(w_l, ByDate())
    if i == 'w':
        WorkList.add(w_l, ByWeek())
    if i == 's':
        WorkList.add(w_l, BySession())
    if i == 'e':
        WorkList.add(w_l, ByEndpoint())
if __name__== '__main__':
    print('Opening file')
    with argv.infile.open('r', encoding='utf-8') as log_f:
        for line in log_f:
            data = list()
            datetime, server_id, resp_full, resp_outer, ip, token, method, url, referrer = line.split('\t')
            resp_full = float(resp_full)
            resp_outer = float(resp_outer)
            dt = datetime.split()
            if token == '-':
                user_id = ''
                session_id = ''
            else:
                user_id, session_id = token.split(':')
            data = [dt[0], dt[1], server_id, resp_full, resp_outer, ip, user_id, session_id, method, url, referrer]
            WorkList.work(w_l, data)
    print('Writing results to file')
    with argv.outfile.open('w', encoding='utf-8') as out_f:
        WorkList.finalize(w_l)
        out_f.write(WorkList.prepare_table(w_l))
        WorkList.prepare_table(w_l)
    print('Job done.')
    now = time.perf_counter()
    loop_elapsed = now - loop_start
    print(f'Время выполнения : {loop_elapsed}')
    exit(0)
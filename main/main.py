import mysql.connector as my
import re
from datetime import datetime


def reader(file):
    with open(file, 'r') as f:
        lines = f.readlines()
        return lines


def isdate(value):
    month_map = {'JAN': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'APR': 'Apr', 'MAY': 'May', 'JUN': 'Jun',
                 'JUL': 'Jul', 'AUG': 'Aug', 'SEP': 'Sep', 'OCT': 'Oct', 'NOV': 'Nov', 'DEC': 'Dec'}
    try:
        # Convert month abbreviation to correct format
        for k, v in month_map.items():
            value = re.sub(rf'(\d+)-{k}-(\d+)', rf'\1-{v}-\2', value)
        datetime.strptime(value, '%d-%b-%y')
        return 1
    except ValueError:
        return 0


def chooser(line):
    line = line.strip()
    line = re.split(r'\t|(?<!\S) (?!\S)', line)
    new_line = []
    for i in line:
        i = i.strip()
        if not i or i.isspace():
            continue
        try:
            i = int(i)
        except ValueError:
            if '-' in i:
                if isdate(i):
                    i = date(i)
            pass
        new_line.append(i)
    return new_line


def date(value):
    month_map = {'JAN': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'APR': 'Apr', 'MAY': 'May', 'JUN': 'Jun',
                 'JUL': 'Jul', 'AUG': 'Aug', 'SEP': 'Sep', 'OCT': 'Oct', 'NOV': 'Nov', 'DEC': 'Dec'}
    # Convert month abbreviation to correct format
    for k, v in month_map.items():
        value = re.sub(rf'(\d+)-{k}-(\d+)', rf'\1-{v}-\2', value)
    value = datetime.strptime(value, '%d-%b-%y').strftime('%Y-%m-%d')
    return value


def sql(line, table):
    query = f"insert into {table} values("
    for i, column in enumerate(line):
        if i == len(line) - 1:
            if type(column) == int:
                query += f'{column})'
            else:
                query += f"'{column}')"
        else:
            if type(column) == int:
                query += f'{column}, '
            else:
                query += f"'{column}', "
    return query


def main(table, file, host, user, password, database):
    mycon = my.connect(host=host, user=user, password=password, database=database)
    mycur = mycon.cursor()
    lines = reader(file)
    for line in lines:
        line_temp = chooser(line)
        print(line_temp)
        query = sql(line_temp, table)
        mycur.execute(query)
    mycon.commit()



"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os

import psycopg2

password = os.getenv('PASSWORD')


def get_data_from_csv(filename: str) -> list:
    with open(filename, encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        data = []
        for row in reader:
            data.append(tuple(row))
        return data


conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=password)

try:
    with conn:
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                            get_data_from_csv('north_data/employees_data.csv'))
            cur.executemany('INSERT INTO customers VALUES (%s, %s, %s)',
                            get_data_from_csv('north_data/customers_data.csv'))
            cur.executemany('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                            get_data_from_csv('north_data/orders_data.csv'))
finally:
    conn.close()

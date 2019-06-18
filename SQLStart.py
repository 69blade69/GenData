#!/usr/bin/python3
# https://pypi.org/project/mysqlclient/ pip install mysqlclient
import sqlalchemy
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, ForeignKey

"""CREATE TABLE IN MYSQL"""
metadata = MetaData()
gen_test_table = Table('gen_test_table', metadata,
    Column('inv_nr', Integer, primary_key=True),
    Column('date', Date),
    Column('code', Integer),
    Column('Wert', Integer)
)


print(sqlalchemy.__version__)

engine = create_engine("mysql://admin:r00t!QWE@172.22.100.200/testdb", encoding='utf-8', echo=True)

""" Connect with MySQL testdb"""
with engine.connect() as conn:
    metadata.create_all(engine)   # create table in MySQL
    print(conn)

    """ input and sort inv_nr from testdb.uncoupling"""
    result = conn.execute(
        "SELECT COUNT(ZVCH_INV) AS cnt_inv, ZVCH_INV FROM testdb.uncoupling group by ZVCH_INV ORDER BY cnt_inv desc limit 20;")

    print('Start')
    list_inv_nr = []
    i_inv_nr = 0
    for row in result:
        #pass
        #print(type(row))
        print(i_inv_nr,":", str(row))
        #print(row[0])
        T = (row)  # T = ('cc', 'aa', 'dd', 'bb')
        tmp = list(T)  # Создать список из элементов кортежа
        print(tmp)
        list_inv_nr.append(tmp)
        i_inv_nr += 1
        #print('ROW:', row)
        # print("username:", row['username'])
        # print('ROW:', row[0]) # conn.execute("SHOW DATABASES")
        # print("username:", row['User'])
    print('\n'.join(map(str, list_inv_nr)))  # построчный вывод списка
    # print(list_inv_nr[0][1])
print('Ende')

"""read table from inv_nr with Date uncoupling """
with engine.connect() as conn:
    for count_inv in list_inv_nr:
        # print(count_inv[1])
        result_inv_nr = conn.execute(
            "SELECT ZVCH_INV, `0CALDAY`, ZVCH_BRKT FROM testdb.uncoupling WHERE ZVCH_INV = %s ORDER BY `0CALDAY` ASC" % count_inv[1])
        list_zug = []
        i_zug = 1
        for row_inv_nr in result_inv_nr:
            # print(str(row_inv_nr))
            T_zug = (row_inv_nr)  # T = ('cc', 'aa', 'dd', 'bb')
            tmp_zug = list(T_zug)  # Создать список из элементов кортежа
            list_zug.append(tmp_zug)
            i_inv_nr += 1
        print('\n'.join(map(str, list_zug)))  # построчный вывод списка
        # print(list_zug[0][1], list_zug[1][1])
        i_date = 0
        for count_date in list_zug:
            if len(list_zug) == i_date + 1:
                break
            date_generated = [list_zug[i_date][1] + datetime.timedelta(days=x) for x in range(1, (list_zug[i_date+1][1] - list_zug[i_date][1]).days)]
            i_date += 1
            for date in date_generated:
                print(date.strftime("%Y-%m-%d"))
            print("----------------------------")
#conn.close()g='utf-8', echo=True)
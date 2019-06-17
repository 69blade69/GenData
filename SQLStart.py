#!/usr/bin/python3
# https://pypi.org/project/mysqlclient/ pip install mysqlclient
import sqlalchemy
from sqlalchemy import create_engine

print(sqlalchemy.__version__)

engine = create_engine("mysql://admin:r00t!QWE@172.22.100.200/testdb", encoding='utf-8', echo=True)

""" input and sort inv_nr from testdb.uncoupling"""
with engine.connect() as conn:
    print(conn)

    result = conn.execute(
        #"SELECT ZVCH_INV, ZVCHDATKO, ZVKFGRL FROM testdb.wheels order by ZVCH_INV, ZVCHDATKO, ZVKFGRL limit 100000")  # conn.execute("SELECT User, Host, Password FROM mysql.user") #conn.execute("SHOW DATABASES") # conn.execute("select username from users")
        #"SELECT * FROM testdb.wheels limit 1000")
        #"CREATE TABLE test_table (idtest_table_id INT NOT NULL, test_tablecol VARCHAR(45) NULL, PRIMARY KEY (idtest_table_id))")
        #"INSERT INTO test_table VALUES(001, 'XEP')")
        #"SELECT * FROM testdb.test_table;")
        "SELECT COUNT(ZVCH_INV) AS cnt_inv, ZVCH_INV FROM testdb.uncoupling group by ZVCH_INV ORDER BY cnt_inv desc limit 20;")

    print('Start')
    list_inv_nr = []
    i_inv_nr = 1
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
    print(list_inv_nr[0][1])
print('Ende')

"""read table from inv_nr with Date uncoupling """
with engine.connect() as conn:
    result_inv_nr = conn.execute(
        "SELECT * FROM testdb.uncoupling WHERE ZVCH_INV = %s" % list_inv_nr[0][1])
    list_zug = []
    i_zug = 1
    for row_inv_nr in result_inv_nr:
        # print(str(row_inv_nr))
        T_zug = (row_inv_nr)  # T = ('cc', 'aa', 'dd', 'bb')
        tmp_zug = list(T_zug)  # Создать список из элементов кортежа
        list_zug.append(tmp_zug)
        i_inv_nr += 1
    print('\n'.join(map(str, list_zug)))  # построчный вывод списка
#conn.close()g='utf-8', echo=True)
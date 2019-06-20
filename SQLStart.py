#!/usr/bin/python3
# https://pypi.org/project/mysqlclient/ pip install mysqlclient
import sqlalchemy
import datetime
import random
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, Date, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

"""CREATE TABLE IN MYSQL"""
metadata = MetaData()
gen_test_table = Table('gen_test_table', metadata,
    Column('ID', Integer, primary_key=True),
    Column('inv_nr', Integer),
    Column('date', Date),
    Column('code', Integer),
    Column('Wert', Float)
)

"""Table gen_test_table description"""
Base = declarative_base()
class Zug (Base):
    __tablename__ = 'gen_test_table'
    inv_nr = Column(Integer, primary_key=True)
    date = Column(Date)
    code = Column(Integer)
    Wert = Column(Integer)

    def __init__(self, inv_nr, date, code, Wert):
        self.inv_nr = inv_nr
        self.date = date
        self.code = code
        self.Wert = Wert

    def __repr__(self):
        return "<Zug('%s','%s','%s','%s')>" % (self.inv_nr, self.date, self.code, self.Wert)

print(sqlalchemy.__version__)

engine = create_engine("mysql://admin:r00t!QWE@172.22.100.200/testdb", encoding='utf-8', echo=True)

""" Connect with MySQL testdb"""
with engine.connect() as conn:
    metadata.create_all(engine)   # create table in MySQL
    print(conn)

    """ input and sort inv_nr from testdb.uncoupling"""
    result = conn.execute(
        "SELECT COUNT(ZVCH_INV) AS cnt_inv, ZVCH_INV FROM testdb.uncoupling group by ZVCH_INV ORDER BY cnt_inv desc limit 1000;")

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

    print('\n'.join(map(str, list_inv_nr)))  # построчный вывод списка всех инвентарных номеров вагонов
    # print(list_inv_nr[0][1])
print('Ende')
# counter = 0
"""read table from inv_nr with Date uncoupling """
with engine.connect() as conn:
    Session = sessionmaker(bind=engine)
    session = Session()

    pack_zug = []
    for count_inv in list_inv_nr:
        # print(count_inv[1])
        result_inv_nr = conn.execute(
            "SELECT ZVCH_INV, `0CALDAY`, ZVCH_BRKT FROM testdb.uncoupling WHERE ZVCH_INV = %s ORDER BY `0CALDAY` ASC" % count_inv[1])

        list_zug = []
        for row_inv_nr in result_inv_nr:
            # print(str(row_inv_nr))
            T_zug = (row_inv_nr)  # T = ('cc', 'aa', 'dd', 'bb')
            tmp_zug = list(T_zug)  # Создать список из элементов кортежа
            list_zug.append(tmp_zug)

        # print('\n'.join(map(str, list_zug)))  # построчный вывод списка
        # print(list_zug[0][1], list_zug[1][1])

        """Gen Datetime, import Wert from test_table and push into gen_test_table"""

        i_date = 0
        for count_date in list_zug:
            if len(list_zug) == i_date + 1:
                break
            date_generated = [list_zug[i_date][1] + datetime.timedelta(days=x) for x in range(1, (list_zug[i_date+1][1] - list_zug[i_date][1]).days)]

            result_wert = conn.execute(
                "SELECT value_min, value_max FROM testdb.test_table WHERE code_id = %s" % list_zug[i_date][2]) # TODO: требуется проверка кода
            wert_row_tmp = []
            for wert_row in result_wert:
                wert_row_tmp.append(wert_row)
            # print("Counter: %s" % counter)
            # counter += 1
            test_zug = Zug(list_zug[i_date][0], list_zug[i_date][1], list_zug[i_date][2], wert_row_tmp[0][1])
            pack_zug.append(test_zug)

            if len(date_generated) > 0:
                wert_i = ((wert_row_tmp[0][1]-wert_row_tmp[0][0])/(len(date_generated)))
            else:
                wert_i = 0
            wert = wert_row_tmp[0][1]
            for date in date_generated:
                # test_zug = Zug(list_zug[i_date][0], list_zug[i_date][1], list_zug[i_date][2], wert)
                wert = wert - wert_i
                test_zug_date = Zug(list_zug[i_date][0], date, list_zug[i_date][2], wert)
                pack_zug.append(test_zug_date)

            i_date += 1

        print('\n'.join(map(str, list_zug)))  # построчный вывод списка
        session.add_all(pack_zug)
        session.commit()
        pack_zug.clear()


#conn.close()g='utf-8', echo=True)
import sqlalchemy
import random
from sqlalchemy import create_engine,Column,Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
"""Table description"""
Base = declarative_base()
class Iznos (Base):
    __tablename__ = 'test_table'
    code_id = Column(Integer, primary_key=True)
    value_min = Column(Integer)
    value_max = Column(Integer)

    def __init__(self, code_id, value_min, value_max):
        self.code_id = code_id
        self.value_min = value_min
        self.value_max = value_max

    def __repr__(self):
        return "<Iznos('%s','%s','%s')>" % (self.code_id, self.value_min, self.value_max)

print(sqlalchemy.__version__)

engine = create_engine("mysql://admin:r00t!QWE@172.22.100.200/testdb", encoding='utf-8', echo=True)

with engine.connect() as conn:
    print(conn)
    Session = sessionmaker(bind=engine)
    session = Session()

    result_code = conn.execute("truncate table test_table;")

    """ insert Data from dict_troubles """
    result_code = conn.execute("SELECT code FROM dict_troubles;")
    pack_iznos = []

    # for index in range(1000):
    #     value_mn = random.randrange(1, 3)
    #     value_mx = random.randrange(4,10)
    #     test_iznos = Iznos(index, value_mn, value_mx)
        #pack_iznos.append(test_iznos)
    counter_commit = 0
    for code in result_code:

        print(code)
        print("Count:%s" % result_code.rowcount)

        """ gen Data for test_table"""
        value_mn = random.randrange(1,3)
        value_mx = random.randrange(4,10)
        print("random data: ", value_mn, value_mx)

        """ input Data in to test_table """
        #Session = sessionmaker(bind=engine)
        #session = Session()
        print('Start input data')
        #test_iznos = Iznos(code[0], 1, 5)
        test_iznos = Iznos(code[0], value_mn, value_mx)
        pack_iznos.append(test_iznos)
        if counter_commit % 100 == 0:
            print(counter_commit)
            session.add_all(pack_iznos)
            #pack_iznos.clear()
        # session.add(test_iznos)
            session.commit()
        counter_commit += 1
    if len(pack_iznos) > 0:
        session.add_all(pack_iznos)
        session.commit()

""" Show result from test_table """
    # session.flush()
    # session.commit()
with engine.connect() as conn:
    result = conn.execute("SELECT * FROM testdb.test_table limit 20;")
    print('Show result')
    joinedlist = []
    i = 1
    for row in result:
        # pass
        # print(type(row))
        print(i, ":", str(row))
        # print(row[0])
        T = (row)  # T = ('cc', 'aa', 'dd', 'bb')
        tmp = list(T)  # Создать список из элементов кортежа
        print(tmp)
        joinedlist.append(tmp)
        i += 1
        # print('ROW:', row)
        # print("username:", row['username'])
        # print('ROW:', row[0]) # conn.execute("SHOW DATABASES")
        # print("username:", row['User'])
    print('\n'.join(map(str, joinedlist)))  # построчный вывод списка
    #print(joinedlist[0][1])

print('Ende')
#conn.close()g='utf-8', echo=True)



# import sqlalchemy
# import random
# from sqlalchemy import create_engine,Column,Integer
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
# Base = declarative_base()
# class Iznos (Base):
#     __tablename__ = 'test_table'
#     code_id = Column(Integer, primary_key=True)
#     value_min = Column(Integer)
#     value_max = Column(Integer)
#
#     def __init__(self, code_id, value_min, value_max):
#         self.code_id = code_id
#         self.value_min = value_min
#         self.value_max = value_max
#
#     def __repr__(self):
#         return "<Iznos('%s','%s','%s')>" % (self.code_id, self.value_min, self.value_max)
#
# print(sqlalchemy.__version__)
#
# engine = create_engine("mysql://admin:r00t!QWE@172.22.100.200/testdb", encoding='utf-8', echo=True)
#
#
#
# with engine.connect() as conn:
#     print(conn)
#     Session = sessionmaker(bind=engine)
#     session = Session()
#
#     """ insert Data from dict_troubles """
#     result_code = conn.execute(
#         "SELECT code FROM dict_troubles;")
#     for code in result_code:
#         print(code)
#
#         """ gen Data for test_table"""
#         value_mn = random.randrange(1,3)
#         value_mx = random.randrange(4,10)
#         print("random data: ", value_mn, value_mx)
#
#         """ input Data in to test_table """
#         #Session = sessionmaker(bind=engine)
#         #session = Session()
#         print('Start input data')
#         #test_iznos = Iznos(code[0], 1, 5)
#         test_iznos = Iznos(code[0], value_mn, value_mx)
#         session.add(test_iznos)
#         session.commit()
#
# with engine.connect() as conn:
#     """Show result from test_table"""
#     result = conn.execute(
#         "SELECT * FROM testdb.test_table limit 20;")
#     print('Show result')
#     joinedlist = []
#     i = 1
#     for row in result:
#         # pass
#         # print(type(row))
#         print(i, ":", str(row))
#         # print(row[0])
#         T = (row)  # T = ('cc', 'aa', 'dd', 'bb')
#         tmp = list(T)  # Создать список из элементов кортежа
#         print(tmp)
#         joinedlist.append(tmp)
#         i += 1
#         # print('ROW:', row)
#         # print("username:", row['username'])
#         # print('ROW:', row[0]) # conn.execute("SHOW DATABASES")
#         # print("username:", row['User'])
#     print('\n'.join(map(str, joinedlist)))  # построчный вывод списка
#     #print(joinedlist[0][1])
#
#
# print('Ende')
# #conn.close()g='utf-8', echo=Tru
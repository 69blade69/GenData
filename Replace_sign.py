#!/usr/bin/python3
import os
import psutil   # сторонний модуль pip insall psutil
import sys
import shutil

from datetime import datetime

print("Текущая директория: ", os.getcwd())

answer = ''
while answer.lower() != 'q':
    print("Для выхода нажмите кнопку (q), для начала операции ЗАМЕНА нажмите любую кнопку")
    answer = input()
    if answer.lower() =='q':
        print('Bye!')
        #exit()
    else:
        print("START")
        print(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))

        f_out = open("out.csv", 'w')

        for line in open('test_zvsttlg2019.csv'):  # 'test.csv' 'ZVSTTLG.CSV'
            if '-;' in line:
                #print(line)
                parts = line.split(';')
                #print(parts)
                for step in range(len(parts)):
                    item = parts[step]
                    #print(item)
                    if item.endswith('-'):
                        tmp = item.replace('-', '')
                        parts[step] = "-{}".format(tmp)
                        #parts[step] = "-" + tmp
                        #parts[step] = "-%s" % tmp
                print(parts)
                line = ';'.join(parts)
                # print(parts)
                # print(line)
            f_out.write(line)

        f_out.close()
        print("FINISH")
        print(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


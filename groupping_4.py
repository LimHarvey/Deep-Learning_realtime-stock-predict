# import threading
# import PyQt5
import pymysql
import sys
import os
import ftplib
import pandas as pd
import numpy as np
import time
import tensorflow as tf
import matplotlib.pyplot as plt
from tqdm import tqdm
from queue import Queue
from datetime import datetime
# from threading import Thread
from PyQt5 import QtWidgets, uic, QtGui
from PyQt5.QtCore import QThread
from sklearn.preprocessing import LabelEncoder,StandardScaler

from keras.models import Sequential
from keras.layers import Dense, Flatten,CuDNNLSTM, LSTM,RepeatVector
from keras.layers import TimeDistributed,Dropout, LeakyReLU, ReLU , BatchNormalization
from keras.layers.convolutional import Conv1D, MaxPooling1D
from tensorflow.keras.callbacks import ModelCheckpoint,EarlyStopping
from tensorflow.keras.optimizers import RMSprop, Adam,SGD
from tensorflow.keras.models import load_model

try : 
    import exchange_calendars as marketcals
except : 
    os.system('pip install -U exchange_calendars')
    import exchange_calendars as marketcals

try : 
    import FinanceDataReader as fdr
except : 
    os.system('pip install -U finance-datareader')
    import FinanceDataReader as fdr


ip = '61.77.150.183'
port = 3306
user = 'a1_user'
passwd = '4560'
db = 'kiwoom'


ip_daishin = '221.148.138.91'
port_daishin = 3306
user_daishin = 'remote'
passwd_daishin = '1234'
db_daishin = 'dbstock'

ip_local = 'localhost'
port_local = 3306
user_local = 'root'
passwd_local = '1234'
db_local = 'dbmodel'

conn = pymysql.connect(host = ip, port = port, user = user, passwd = passwd, db = db, charset = 'utf8')
cursor = conn.cursor()

conn_daishin = pymysql.connect(host = ip_daishin, port = port_daishin, user = user_daishin, passwd = passwd_daishin, db = db_daishin, charset = 'utf8')
cursor_daishin = conn_daishin.cursor()

conn_local = pymysql.connect(host = ip_local, port = port_local, user = user_local, passwd = passwd_local, db = db_local, charset = 'utf8')
cursor_local = conn_local.cursor()

conn_local_2 = pymysql.connect(host = ip_local, port = port_local, user = user_local, passwd = passwd_local, db = 'dbgroup', charset = 'utf8')
cursor_local_2 = conn_local_2.cursor()

# display_que = Queue()
display_que_2 = Queue()

df_daishin_ = pd.DataFrame(None, columns=['종목코드','체결시간','매수잔량Null','매도잔량Null','체결강도Null'])

kiwoom_columns_ = ['종목코드','전일종가','900시가','930시가','데이터개수','당일평균','증감율','그룹']
df_kiwoom_ = pd.DataFrame(None, columns = kiwoom_columns_)

merged_columns_ = ['종목코드','매수잔량Null','매도잔량Null','체결강도Null','전일종가','900시가','930시가','데이터개수','당일평균','증감율','그룹']
df_merged_ = pd.DataFrame(None,columns = merged_columns_)

# ftp = ftplib.FTP()
# ftp_path = '/model'
# ftp_ip = 'localhost'
# ftp_port = 21

# class display(QThread) :
#     def __init__(self, parent) :
#         super().__init__(parent)
#         self.parent = parent
#         print('display Thread : Initiate')

#         # self.parent.ui.main_table.setColumnCount(len(self.parent.columns))
#         self.parent.ui.main_table.setColumnCount(4)
#         # self.ui.main_table.setRowCount(len(return_))

#         '종목코드','매수잔량 Null','매도잔량 Null','체결강도 Null'

#         row_num_ = 0
#         self.parent.ui.main_table.setItem(row_num_, 0, QtWidgets.QTableWidgetItem('종목코드'))
#         self.parent.ui.main_table.setItem(row_num_, 1, QtWidgets.QTableWidgetItem(''))
#         self.parent.ui.main_table.setItem(row_num_, 2, QtWidgets.QTableWidgetItem('9:00시가'))
#         self.parent.ui.main_table.setItem(row_num_, 3, QtWidgets.QTableWidgetItem('9:30시가'))
#         self.parent.ui.main_table.setItem(row_num_, 4, QtWidgets.QTableWidgetItem('데이터개수'))
#         self.parent.ui.main_table.setItem(row_num_, 5, QtWidgets.QTableWidgetItem('당일평균'))
#         self.parent.ui.main_table.setItem(row_num_, 6, QtWidgets.QTableWidgetItem('증감율'))
#         self.parent.ui.main_table.setItem(row_num_, 7, QtWidgets.QTableWidgetItem('그룹'))

#     def run(self) :
        
#         while True :
#             # print('display thread : start')
#             if display_que.empty() :
#                 # print('display Thread : empty que')
#                 time.sleep(1)
#                 continue
#             try : 
#                 # print('display thread : 꺼낸다!')
#                 code_, ex_close_price_, price_900_, price_930_, num_data_, avr_price_, differ_rate_, group_ = display_que.get()

#                 # print('display_thread : table의 row 늘리기')
#                 row_num_ = self.parent.ui.main_table.rowCount()
#                 self.parent.ui.main_table.insertRow(row_num_)
#                 # print('display_thread : row_num : ',row_num_)
                
#                 # ['종목코드','전일종가','900시가','930시가','데이터개수','당일평균','그룹']
#                 self.parent.ui.main_table.setItem(row_num_, 0, QtWidgets.QTableWidgetItem(str(code_)))
#                 self.parent.ui.main_table.setItem(row_num_, 1, QtWidgets.QTableWidgetItem(str(ex_close_price_)))
#                 self.parent.ui.main_table.setItem(row_num_, 2, QtWidgets.QTableWidgetItem(str(price_900_)))
#                 self.parent.ui.main_table.setItem(row_num_, 3, QtWidgets.QTableWidgetItem(str(price_930_)))
#                 self.parent.ui.main_table.setItem(row_num_, 4, QtWidgets.QTableWidgetItem(str(num_data_)))
#                 self.parent.ui.main_table.setItem(row_num_, 5, QtWidgets.QTableWidgetItem(str(avr_price_)))
#                 self.parent.ui.main_table.setItem(row_num_, 6, QtWidgets.QTableWidgetItem(str(differ_rate_)))
#                 self.parent.ui.main_table.setItem(row_num_, 7, QtWidgets.QTableWidgetItem(str(group_)))
#                 self.parent.ui.main_table.viewport().update()
#             except Exception as e :
#                 print('display Thread : error : ', e)
#                 continue

class display2(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent
        print('display Thread : Initiate')

        # self.parent.ui.main_table.setColumnCount(len(self.parent.columns))
        self.parent.ui.main_table_2.setColumnCount(9)
        # self.ui.main_table.setRowCount(len(return_))

        row_num_ = 0
        self.parent.ui.main_table_2.setItem(row_num_, 0, QtWidgets.QTableWidgetItem('종목코드'))
        self.parent.ui.main_table_2.setItem(row_num_, 1, QtWidgets.QTableWidgetItem('전일종가'))
        self.parent.ui.main_table_2.setItem(row_num_, 2, QtWidgets.QTableWidgetItem('9:00시가'))
        self.parent.ui.main_table_2.setItem(row_num_, 3, QtWidgets.QTableWidgetItem('9:30시가'))
        self.parent.ui.main_table_2.setItem(row_num_, 4, QtWidgets.QTableWidgetItem('데이터개수'))
        self.parent.ui.main_table_2.setItem(row_num_, 5, QtWidgets.QTableWidgetItem('당일평균'))
        self.parent.ui.main_table_2.setItem(row_num_, 6, QtWidgets.QTableWidgetItem('증감율'))
        self.parent.ui.main_table_2.setItem(row_num_, 7, QtWidgets.QTableWidgetItem('그룹'))

    def run(self) :
        
        while True :
            # print('display thread : start')
            if display_que_2.empty() :
                # print('display Thread : empty que')
                time.sleep(1)
                continue
            try : 
                # print('display thread : 꺼낸다!')
                code_, ex_close_price_, price_900_, price_930_, num_data_, avr_price_, differ_rate_, group_ = display_que_2.get()

                # print('display_thread : table의 row 늘리기')
                row_num_ = self.parent.ui.main_table_2.rowCount()
                self.parent.ui.main_table_2.insertRow(row_num_)
                # print('display_thread : row_num : ',row_num_)
                
                # ['종목코드','전일종가','900시가','930시가','데이터개수','당일평균','그룹']
                self.parent.ui.main_table_2.setItem(row_num_, 0, QtWidgets.QTableWidgetItem(str(code_)))
                self.parent.ui.main_table_2.setItem(row_num_, 1, QtWidgets.QTableWidgetItem(str(ex_close_price_)))
                self.parent.ui.main_table_2.setItem(row_num_, 2, QtWidgets.QTableWidgetItem(str(price_900_)))
                self.parent.ui.main_table_2.setItem(row_num_, 3, QtWidgets.QTableWidgetItem(str(price_930_)))
                self.parent.ui.main_table_2.setItem(row_num_, 4, QtWidgets.QTableWidgetItem(str(num_data_)))
                self.parent.ui.main_table_2.setItem(row_num_, 5, QtWidgets.QTableWidgetItem(str(avr_price_)))
                self.parent.ui.main_table_2.setItem(row_num_, 6, QtWidgets.QTableWidgetItem(str(differ_rate_)))
                self.parent.ui.main_table_2.setItem(row_num_, 7, QtWidgets.QTableWidgetItem(str(group_)))
                self.parent.ui.main_table_2.viewport().update()
            except Exception as e :
                print('display_2 Thread : error : ', e)
                continue
            
class grab_data(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent


    def start(self) : 
        global df_daishin_

        except_table = ['u001', 'z001']

        return_ = []

        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()

        sql = "show tables;"
        cursor_daishin.execute(sql)
        temp = cursor_daishin.fetchall()

        for i in temp :
            if i[0] not in except_table :
                return_.append(i[0])

        today_date_ = year_+'-'+mon_.zfill(2)+'-'+day_.zfill(2)

        

        for cnt, code_ in enumerate(return_) :
            try :
                sql = "select * from {} where 체결시간 between '{}' and '{}';".format(code_,today_date_+' 09:00:00',today_date_+' 15:30:00')
                df_dai = pd.read_sql(sql, conn_daishin)

                # print(df_dai)

                cnt_null_list_ = [code_,]
                
                print('{}번째 종목 {} 진행중'.format(cnt,code_))
                for i in range(4) :
                    cnt_null_ = 0
                    for j in range(len(df_dai)) :
                        if ((df_dai.iloc[j].iloc[i] == 0) | (df_dai.iloc[j].iloc[i] == '')) :
                            cnt_null_ += 1
                            # print('null data : ',df_dai.iloc[j].iloc[i])
                        # else : 
                            # print('not null data : ',df_dai.iloc[j].iloc[i])
                    # print('null counter : ',cnt_null_)
                    cnt_null_list_.append(cnt_null_)
                # print(cnt_null_list_)
                df_daishin_ = df_daishin_.append(pd.Series(cnt_null_list_, index=df_daishin_.columns), ignore_index=True)
                

        #         print('{}번째 {} list개수 : '.format(cnt,code_),len(df_dai))
        #         print(code_, cnt_null_list_)
                # QtWidgets.QApplication.processEvents()
            except Exception as e:
                print('SQL parsing : error : ',e)

        df_daishin_.drop('체결시간', axis=1, inplace=True)

        # print(df_daishin_)

        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()
        date_time_target_ = year_+'-'+mon_+'-'+day_
        
        date_time_realtime_ = datetime.now().strftime('%y-%m-%d-%H-%M-%S')
        df_daishin_.to_csv('daishin_groupping_{}-{}.csv'.format(date_time_target_, date_time_realtime_), encoding='utf8')

        self.parent.ui.main_table.setColumnCount(len(df_daishin_.iloc[0]))
        self.parent.ui.main_table.setRowCount(len(df_daishin_))

        df_display_1 = df_daishin_.copy()
        df_display_1 = df_display_1.astype('str')

        for i in range(len(df_display_1)) :
            for j in range(len(df_display_1.iloc[0])) :
                self.parent.ui.main_table.setItem(i,j,QtWidgets.QTableWidgetItem(df_display_1.iloc[i,j]))


class grab_data_2(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent


    def start(self) : 
        global df_kiwoom_

        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()

        sql = 'show tables;'
        cursor.execute(sql)
        return_ = cursor.fetchall()
        
        table_list_ = []
        ex_close_price_list_ = []
        price_0900_list_ = []
        price_0930_list_ = []
        avr_price_list_ = []
        num_data_list_ = []
        differ_rate_list_ = []
        group_list_ = []

        mcal = marketcals.get_calendar('XKRX')

        today_date_ = year_+'-'+mon_.zfill(2)+'-'+day_.zfill(2)
        ex_open_date_ = mcal.previous_open(today_date_) # 전 개장일 확인

        for cnt,i in enumerate(return_) :
            code_ = i[0]
            

            df = fdr.DataReader(code_[1:],ex_open_date_.date(),today_date_)

            # print(df)
            # 전일 종가 구하기
            ex_close_price_ = df.iloc[0].loc['Close']
            

            try : 
                # 당일 9:00 부터 9:30 까지 현제가 데이터 모두 리스트로 받아오기
                sql = "select 현재가 from {} where 체결시간 between '{}' and '{}';".format(code_,today_date_+' 09:00:00',today_date_+' 09:30:00')
                cursor.execute(sql)
                price_list_temp_ = cursor.fetchall()

                # DB 에서 가져온 데이터의 길이가 0 이면 DataFrame 에서 데이터 밀리는 현상 발생
                if len(price_list_temp_) == 0 :
                    continue

                price_list_ = []

                # 부호 제거
                for i in price_list_temp_ :
                    price_ = i[0]
                    if price_ < 0 :
                        price_ *= -1
                    price_list_.append(price_)

                num_data_ = len(price_list_) # 데이터 개수
                price_list_np_ = np.array(price_list_) # 평균 구하기 위해 np array 로 전환
                avr_price_ = np.mean(price_list_np_) # 평균
                

                thresh1_ = self.parent.ui.thresh1.value()
                thresh2_ = self.parent.ui.thresh2.value()
                thresh3_ = self.parent.ui.thresh3.value()
                thresh4_ = self.parent.ui.thresh4.value()
                thresh5_ = self.parent.ui.thresh5.value()

                differ_rate_ = round(((avr_price_ / ex_close_price_)-1)*100,2) # 증감율
                
                if differ_rate_ > thresh5_ :
                    group_ = 'F'
                elif (differ_rate_> thresh4_) :
                    group_ = 'E'
                elif (differ_rate_> thresh3_) :
                    group_ = 'D'
                elif (differ_rate_> thresh2_) :
                    group_ = 'C'
                elif (differ_rate_> thresh1_) :
                    group_ = 'B'
                else :
                    group_ = 'A'

                table_list_.append(code_) # 종목코드
                ex_close_price_list_.append(ex_close_price_)# 전일 종가
                price_0900_list_.append(price_list_[0]) # 종목별 9:00 시가 
                price_0930_list_.append(price_list_[-1]) # 종목별 9:30 시가
                avr_price_list_.append(avr_price_) # 종목별 9:00~9:30 평균가
                num_data_list_.append(num_data_) # 종목별 데이터 갯수
                differ_rate_list_.append(differ_rate_) # 종목별 증감율
                group_list_.append(group_) # 종목별 그룹

                

                # print('display que 입력')
                # print([code_,ex_close_price_,price_list_[0],price_list_[-1], num_data_, avr_price_])
                display_que_2.put([code_,ex_close_price_,price_list_[0],price_list_[-1], num_data_, avr_price_, differ_rate_, group_])
                # time.sleep(0.1)

            except Exception as e :

                print('grab2 error')
                print('code : ',i)
                print('e : ',e)
                continue

        
        df_kiwoom_ = pd.DataFrame(list(zip(table_list_, ex_close_price_list_, price_0900_list_, price_0930_list_, num_data_list_, avr_price_list_, differ_rate_list_, group_list_)), columns=kiwoom_columns_)
        # print(df_kiwoom_)

        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()
        date_time_target_ = year_+'-'+mon_+'-'+day_
        
        date_time_realtime_ = datetime.now().strftime('%y-%m-%d-%H-%M-%S')
        df_kiwoom_.to_csv('kiwoom_groupping_{}-{}.csv'.format(date_time_target_, date_time_realtime_), encoding='utf8')

        # for i in range(len(df)) :
        #     for j in range(len(df.iloc[0])) :
        #         self.ui.main_table.setItem(i,j,QtWidgets.QTableWidgetItem(df.iloc[i,j]))


class window(QtWidgets.QDialog):
    def __init__(self) :
        self.table_name_ = ''

        super().__init__()
        self.ui = uic.loadUi('./groupping.ui')
        self.ui.show()
        # UI Initialize
        self.init_UI()
        # display_ = display(self)
        # display_.start()
        display_2 = display2(self)
        display_2.start()

    def init_UI(self) :
        # self.ui.pushButton.clicked.connect(self.btn_clicked)
        # self.ui.cmb_year.clicked.connect(self.search_year)
        self.ui.cmb_year.currentTextChanged.connect(self.search_month)
        self.ui.cmb_mon.currentTextChanged.connect(self.search_date)
        self.ui.btn_start.clicked.connect(self.start)
        self.ui.btn_start_modelling.clicked.connect(self.start_modelling)

        self.grab_ = grab_data(self)
        self.grab_2 = grab_data_2(self)
        self.merging = merging(self)
        self.test = test(self)
        self.start_modelling_ = modelling(self)

        self.search_year()

    
    def search_year(self) : 
        # 가장 첫번째 테이블에서 기록된 데이터들의 연도를 추출 함
        print('search Year : entering')
        year_list_ = []

        sql = 'show tables;'
        cursor.execute(sql)
        return_ = cursor.fetchall()
        self.table_name_ = return_[0][0]

        sql = "select year(체결시간) from {}".format(self.table_name_)
        cursor.execute(sql)
        return_ = cursor.fetchall()
        
        year_list_temp_ = []
        for i in return_ :
            year_list_temp_.append(str(i[0]))

        year_list_ = list(set(year_list_temp_))

        self.ui.cmb_year.addItems(year_list_)

    def search_month(self) : 
        # 기록된 데이터들의 연도별로 기록된 월을 선택 함
        month_list_=[]
        year_ = self.ui.cmb_year.currentText()
        
        print('year : ',year_)
        sql = "select month(체결시간) from {} where date_format(체결시간, '%Y')={}".format(self.table_name_,year_)
        cursor.execute(sql)
        return_ = cursor.fetchall()
            
        # print(return_)
        for i in return_ :
            month_list_.append(str(i[0]))
        
        month_list_ = list(set(month_list_))

        self.ui.cmb_mon.addItems(month_list_)
    
    def search_date(self) :
        # 기록된 데이터들의 연도별로 기록된 일을 선택 함

        mon_ = self.ui.cmb_mon.currentText()
        year_ = self.ui.cmb_year.currentText()

        print('mon : ',mon_)
        sql = "select day(체결시간) from {} where date_format(체결시간, '%Y%m')={}".format(self.table_name_,year_+str(mon_).zfill(2))
        print('year_mon_ : ',year_+mon_)
        cursor.execute(sql)
        return_ = cursor.fetchall()
            
        # print(return_)
        date_list_ = []
        for i in return_ :
            date_list_.append(str(i[0]))
        
        date_list_ = list(set(date_list_))

        print('date_list_ : ', date_list_)

        self.ui.cmb_date.addItems(date_list_)
        self.ui.cmb_date.model().sort(0)

    def start(self) :
        self.grab_.start()
        self.grab_2.start()
        self.merging.start()
        self.test.start()

    def start_modelling(self) :
        self.start_modelling_.start()

class merging(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent

    def run(self) :
        global A_list, B_list, C_list, D_list, E_list, F_list 
        A_list, B_list, C_list, D_list, E_list, F_list = [[],[],[],[],[],[]]

        sell_thres = self.parent.ui.sell_thres.value()
        buy_thres = self.parent.ui.buy_thres.value()
        str_thres = self.parent.ui.str_thres.value()

        enabled_group_ = []

        df_daishin_filtered_ = df_daishin_[(df_daishin_.매수잔량Null<=sell_thres)&(df_daishin_.매도잔량Null<=buy_thres)&(df_daishin_.체결강도Null<=str_thres)]
        
        if self.parent.ui.groupA.isChecked() == True :
            enabled_group_.append('A')
        if self.parent.ui.groupB.isChecked() == True :
            enabled_group_.append('B')
        if self.parent.ui.groupC.isChecked() == True :
            enabled_group_.append('C')
        if self.parent.ui.groupD.isChecked() == True :
            enabled_group_.append('D')
        if self.parent.ui.groupE.isChecked() == True :
            enabled_group_.append('E')
        if self.parent.ui.groupF.isChecked() == True :
            enabled_group_.append('F')


        df_kiwoom_filtered_ = df_kiwoom_[df_kiwoom_['그룹'].isin(enabled_group_)]
        # print('merging : df_kiwoom_filtered : ', df_kiwoom_filtered_)
        # print('merging : df_daishin_filtered : ', df_daishin_filtered_)

        df_merged_ = pd.merge(df_daishin_filtered_,df_kiwoom_filtered_, how='inner', on='종목코드')
        # print('merging')
        # print(df_merged_)

        self.parent.ui.main_table_5.setColumnCount(len(df_merged_.iloc[0]))
        self.parent.ui.main_table_5.setRowCount(len(df_merged_))

        df_merged_1 = df_merged_.copy()
        df_merged_1 = df_merged_1.astype('str')

        for i in range(len(df_merged_1)) :
            for j in range(len(df_merged_1.iloc[0])) :
                self.parent.ui.main_table_5.setItem(i,j,QtWidgets.QTableWidgetItem(df_merged_1.iloc[i,j]))

        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()
        date_time_target_ = year_+'-'+mon_+'-'+day_

        date_time_realtime_ = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        df_merged_.to_csv('./group/merged_groupping_{}.csv'.format(date_time_realtime_), encoding='utf8')

        A_list = df_merged_[df_merged_['그룹']=='A']['종목코드']
        B_list = df_merged_[df_merged_['그룹']=='B']['종목코드']
        C_list = df_merged_[df_merged_['그룹']=='C']['종목코드']
        D_list = df_merged_[df_merged_['그룹']=='D']['종목코드']
        E_list = df_merged_[df_merged_['그룹']=='E']['종목코드']
        F_list = df_merged_[df_merged_['그룹']=='F']['종목코드']

        sql = "insert into dbgroup.group (time, position) values ('{}','{}');".format(str(date_time_realtime_),'./group/merged_groupping_{}.csv'.format(date_time_realtime_))
        print('local sql command : ',sql)
        cursor_local_2.execute(sql)
        return_ = cursor_local_2.fetchall()
        conn_local_2.commit()
        print('local sql return : ',return_)

        


class test(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent

    def run(self) :
        print('test : ')
        print(df_merged_)

class modelling(QThread) :
    def __init__ (self, parent) :
        super().__init__(parent)
        self.parent = parent

        self.make_folder()

        timesteps =  self.parent.ui.timesteps.value()
        self.timesteps = int(timesteps)
        offset =  self.parent.ui.offsets.value()
        self.offset = int(offset)

        global A_list, B_list, C_list, D_list, E_list, F_list 

    def make_folder(self):
    
        folders = ['./csv','./csv/raw','./csv/filtered','./numpy','./numpy/A',
                './numpy/B','./numpy/C','./numpy/D',
                './numpy/E','./numpy/F','./numpy/concat','./model','./model/A','./model/B','./model/C','./model/D','./model/E','./model/F']
        
        for names in folders :
            
            if not os.path.exists(names):
                os.mkdir(names)           


    def extract_dates(self, path = 'raw'):
        
    #     path = './csv'
        file_list = os.listdir('./csv/'+ path)
        
        date_list = []
        for i in file_list:
            
            date_list.append(i.split('_')[0])
        
        return date_list

    def daesin_main(self, code, date):    

        sql = 'select * from {};'.format(code)
        df_dae = pd.read_sql(sql, conn_daishin)
        df_dae  = df_dae.set_index('체결시간')
        df_dae= df_dae[['매수잔량','매도잔량','체결강도']]
        # print('daeshin_main : ad_dae.loc[date] : ',df_dae.loc[date])
        df_dae = df_dae.loc[date]
        # print('daeshin_main : ad_dae1 : ',df_dae)
        df_dae.drop(df_dae.between_time('15:20','15:29').index, inplace=True)
        # print('daeshin_main : ad_dae2 : ',df_dae)
        df_dae.drop(df_dae.between_time('15:31','15:32').index, inplace=True)
        # print('daeshin_main : ad_dae3 : ',df_dae)
        df_dae.drop(df_dae.at_time('23:59').index, inplace=True)
        # print('daeshin_main : ad_dae4 : ',df_dae)
        
        
        return  df_dae

    def daesin_kospi(self, date):  

        sql_kospi = 'select * from u001;'
        df_kospi = pd.read_sql(sql_kospi, conn_daishin)
        df_kospi = df_kospi.set_index('체결시간')[['코스피지수']]
        print('daishin_kospi : df_kospi.loc[date] : ',df_kospi.loc[date])
        df_kospi= df_kospi.loc[date]
        df_kospi.drop(df_kospi.between_time('15:20','15:29').index, inplace=True)
        df_kospi.drop(df_kospi.between_time('15:31','15:32').index, inplace=True)
        df_kospi.drop(df_kospi.at_time('23:59').index, inplace=True)    
        globals()['df_kospi_{}'.format(date.replace('-',''))] = df_kospi

        return globals()['df_kospi_{}'.format(date.replace('-',''))]


    def daesin_issued(self, code):
        
        code2 = code.replace(code[0], code[0].upper())
        sql_issued = 'select * from z001;'
        df_issued = pd.read_sql(sql_issued, conn_daishin)
        df_issued=  df_issued[df_issued.종목코드 == code2][['발행주식수']]
        
        globals()['{}_issued'.format(code)] = df_issued.values[0][0]
        
        return globals()['{}_issued'.format(code)]  

    def kiwoom(self, code, date) : 
        
        sql_kiwoom = 'select * from {};'.format(code)
        df_kiwoom = pd.read_sql(sql_kiwoom, conn)
        df_kiwoom  = df_kiwoom.set_index('체결시간')
        df_kiwoom= df_kiwoom.loc[date]
        # print('kiwoom1 : ',df_kiwoom)
        df_kiwoom.rename(columns={'현재가':'종가'},inplace=True)
        # print('kiwoom2 : ',df_kiwoom)
        df_kiwoom.drop('누적거래대금', axis=1, inplace=True)
        # print('kiwoom3 : ',df_kiwoom)

        return df_kiwoom

    def merge_df(self, date, code):
        print('merge_df : enter')
        
        df_dae, df_kiwoom, df_kospi= self.daesin_main(code, date),self.kiwoom(code, date), globals()['df_kospi_{}'.format(date.replace('-',''))]
        print('merge_df2 : enter')
        df_stock_temp = pd.merge(df_dae, df_kiwoom, left_index = True, right_index=True, how='inner')
        df_stock = pd.merge(df_stock_temp,df_kospi,
                            left_index = True, right_index=True, how='inner')
        df_stock = df_stock.apply(lambda x : abs(x))
        merged_df = df_stock.copy()
        
        return merged_df


    def preprocessing(self, code, date):
        # print('preprocessing : enter')
        merged_df = self.merge_df(date, code)
        issued =  globals()['{}_issued'.format(code)]

        # print('preprocessing2 : enter')
        
        merged_df['매수잔량'] = (merged_df['매수잔량']/issued)*100
        merged_df['매도잔량'] = (merged_df['매도잔량']/issued)*100
        merged_df['거래량'] = (merged_df['거래량']/issued)*100
        merged_df.loc[date + ' 15:30:00', '거래량'] = merged_df.at_time('15:30')['거래량'].values[0]/10

        merged_df['코스피지수'] = merged_df['코스피지수'].pct_change()
        merged_df['시가'] = merged_df['시가'].pct_change()
        merged_df['고가'] = merged_df['고가'].pct_change()
        merged_df['저가'] = merged_df['저가'].pct_change()
        
        drop_index = merged_df[(merged_df['체결강도']== 0)|(merged_df['매수잔량']== 0)|(merged_df['체결강도']== 0)].index
        merged_df.drop(drop_index, axis=0,inplace=True)
        
        merged_df.insert(3,'체결강도_pct',merged_df['체결강도'].pct_change())
        merged_df.insert(1,'매수잔량_pct',merged_df['매수잔량'].pct_change())
        merged_df.insert(3,'매도잔량_pct',merged_df['매도잔량'].pct_change())
        merged_df.insert(7,'거래량_pct',merged_df['거래량'].pct_change())
        merged_df.insert(10,'종가_pct', merged_df['종가'].pct_change())

        # print('preprocessing3 : enter')
        merged_df.rename(columns = {'시가' : '시가_pct','고가' : '고가_pct','저가' : '저가_pct','코스피지수' : '코스피_pct'},inplace=True)
        
        merged_df.dropna(axis=0, inplace=True)

        # merged_df.insert(0, 'time_section', merged_df.index)
        # merged_df.loc[merged_df.between_time('09:00','10:30').index, 'time_section'] = '09:00~10:30'
        # merged_df.loc[merged_df.between_time('10:31','13:59').index, 'time_section'] = '10:31~13:59'
        # merged_df.loc[merged_df.between_time('14:00','15:30').index, 'time_section'] = '14:00~15:30'
        
    #     scaler = MinMaxScaler()
    #     merged_df[['매수잔량','매수잔량_pct','매도잔량','매도잔량_pct','체결강도_pct','체결강도','거래량_pct']] = scaler.fit_transform(merged_df[['매수잔량',
    #                                                                                                                    '매수잔량_pct','매도잔량','매도잔량_pct',
    #                                                                                                                    '체결강도_pct','체결강도','거래량_pct']])
    
        
        # scaler = StandardScaler()
        # merged_df[['매수잔량','매수잔량_pct','매도잔량','매도잔량_pct','체결강도_pct','체결강도','거래량_pct']] = scaler.fit_transform(merged_df[['매수잔량',
        #                                                                                                                '매수잔량_pct','매도잔량','매도잔량_pct',
        #      
        # 
        # merged_df.drop(merged_df[merged_df['매수잔량_pct']==np.inf].index, axis=0, inplace=True)
        # merged_df.drop(merged_df[merged_df['매도잔량_pct']==np.inf].index, axis=0, inplace=True)                                                                                                          '체결강도_pct','체결강도','거래량_pct']])
    
        # print('preprocessing4 : enter')
        print(merged_df)
        merged_df.drop(['매수잔량'], axis=1, inplace=True)
        # print('preprocessing5 : enter')
        merged_df.drop(['매도잔량','체결강도'], axis=1, inplace=True)
        merged_df.drop(['시가_pct','고가_pct','저가_pct'], axis=1, inplace=True)
        # print('preprocessing6 : enter')
        merged_df = pd.get_dummies(merged_df)
        print(merged_df)
        # merged_df.drop('time_section',axis=1,inplace=True)
        # merged_df.drop('time_section',axis=1,inplace=True)
        
        # print('preprocessing7 : enter')
        merged_df['target'] = merged_df['종가'].pct_change(periods=int(self.offset))
        # print('preprocessing8 : enter')
        merged_df.drop('종가', axis=1, inplace=True)
        # print('preprocessing9 : enter')
        for i in merged_df.index:
            if merged_df.loc[i,'target'] >=0 :
                merged_df.loc[i,'target'] = 'PLUS'
            else :
                merged_df.loc[i,'target'] = 'MINUS'

        # print('preprocessing10 : complete')

        
        return merged_df

    def split_sequence(self, date):
        print('split_sequence : enter')
        sequence = self.preprocessing(code, date)
        print('split_sequence2 : enter')
        
        x, y = list(), list()
        
        target = 'target'
        
        for i in range(len(sequence)):
            
            end_ix = i + self.timesteps
            
            if end_ix > len(sequence) - self.offset:
                break
                
            seq_x, seq_y = sequence.drop(target,axis=1).iloc[i:end_ix], sequence.iloc[end_ix + self.offset-1][target]
            x.append(seq_x)
            y.append(seq_y)
        print('split_sequence3 : complete')
        return np.array(x), np.array(y)

    def train_test_spilt(self, date):
        print('train_test_spilt : enter')
        x, y = self.split_sequence(date)
        print('train_test_spilt2 : enter')
        e = LabelEncoder()
        e.fit(y)
        y = e.transform(y)
        print('train_test_spilt2 : label encoder complete')
        
        cutoff_rate=0.8
        cutoff_train = round(x.shape[0] * cutoff_rate)

        x_train, y_train = x[:cutoff_train], y[:cutoff_train]
        x_test, y_test = x[cutoff_train:], y[cutoff_train:]
        print('train_test_spilt2 : cutoff apply complete')
        return x_train, y_train, x_test, y_test


    def filter_csv(self, timesteps, offset):
        
        date_list = self.extract_dates(path = 'raw')
        
        for date in date_list :
            
            df_raw = pd.read_csv('./csv/raw/{}_merged_groupping.csv'.format(date), index_col=0)
            raw_code_list = df_raw['종목코드']
            df_raw = df_raw.set_index('종목코드')

            filtered_code_list = []

            for code in raw_code_list :
                df_kiwoom = self.kiwoom(code, date)

                if len(df_kiwoom)-timesteps-offset >= 180:
                    filtered_code_list.append(code)
        #             print(code)

            df_filtered =df_raw.loc[filtered_code_list]
            df_filtered.to_csv('./csv/filtered/{}_filtered_groupping.csv'.format(date)) 
        
        return df_filtered

    def make_group(self, date):
        
        # A_list
        # B_list
        # C_list
        # D_list
        # E_list
        # F_list

        # timesteps = 60
        # offset = 3
        # date_list = self.extract_dates(path = 'filtered')
        
        date_list = [date]

        path = './numpy/'
        group_list = ['A','B','C','D','E','F']

        for date in date_list :
            
            print('********************',date, '시작', '********************')
            
            self.daesin_kospi(date)

            print(date)
            print()
            
            for group_name, code_list in zip(group_list,[A_list, B_list, C_list, D_list, E_list, F_list]):
                
                if len(code_list)==0 :
                    continue

                for code in tqdm(code_list) :
                    
                    self.daesin_issued(code)   
                    
                print('********************',date, group_name, '시작', '********************')
                
                x_train_concat, y_train_concat, x_test_concat, y_test_concat  = self.concat_train_test(code_list, date)

                np.savez_compressed('./numpy/{}/{}_dataset_{}_{}_{}'.format(group_name,group_name,date,self.timesteps,self.offset) , x_train_concat= x_train_concat, y_train_concat=y_train_concat,
                                x_test_concat=x_test_concat,y_test_concat=y_test_concat)
                
                print('********************',date, group_name, '완료', '********************')
                
            print('********************',date, '완료', '********************')

        

        for group in group_list: # B
            
            group_path = path + group
            
            con_x_train,con_x_test,con_y_train,con_y_test = list(),list(),list(),list()
            print('make_group : group : ',group)
            
            for i, filename in enumerate(os.listdir(group_path)): 
                print(filename)
                dataset_load=np.load(group_path+'/'+filename)

                x_train=dataset_load['x_train_concat']
                x_test=dataset_load['x_test_concat']
                y_train=dataset_load['y_train_concat']
                y_test=dataset_load['y_test_concat']

                dataset_load.close()

                con_x_train.append(x_train)
                con_x_test.append(x_test)
                con_y_train.append(y_train)
                con_y_test.append(y_test)
                
                for i in range(len(con_x_train)) :
                    
                    if i == 0 :
                        final_x_train = con_x_train[i]
                        final_x_test = con_x_test[i]
                        final_y_train = con_y_train[i]
                        final_y_test = con_y_test[i]
                        
                    else :
                        final_x_train = tf.concat([final_x_train,con_x_train[i]],axis=0)
                        final_x_test = tf.concat([final_x_test,con_x_test[i]],axis=0)
                        final_y_train = tf.concat([final_y_train,con_y_train[i]],axis=0)
                        final_y_test = tf.concat([final_y_test,con_y_test[i]],axis=0)
                    
                    
                np.savez_compressed('./numpy/concat/final_{}_dataset_{}_{}'.format(group,self.timesteps, self.offset) , x_train= final_x_train, y_train=final_y_train,
                                x_test=final_x_test, y_test=final_y_test)
                
            print('final_{}_dataset save complete'.format(group))
                
                # df_group = pd.read_csv('./csv/filtered/{}_filtered_groupping.csv'.format(date), index_col=0)

                # B_code_list = df_group[df_group.그룹 == 'B'].index.to_list()
                # C_code_list = df_group[df_group.그룹 == 'C'].index.to_list()
                # D_code_list = df_group[df_group.그룹 == 'D'].index.to_list()
                # E_code_list = df_group[df_group.그룹 == 'E'].index.to_list()

        return A_list, B_list, C_list, D_list, E_list, F_list


    def concat_train_test(self, code_list, date):
        global code

        print('concat_train_test : self.timesteps : ',self.timesteps)
        print('concat_train_test : self.offset : ', self.offset)
            
        for i, code in (enumerate(code_list)) :
            
            try :
                print('concat_train_test : i : ', i)
                print('concat_train_test : code : ', code)
            
                if i == 0: 
                    x_train0, y_train0, x_test0, y_test0 = self.train_test_spilt(date)
                    print('concat_train_test : x_train0 : 0 : ', x_train0)
                    print('concat_train_test : y_train0 : 0 : ', y_train0)
                    print('concat_train_test : x_test0 : 0 : ', x_test0)
                    print('concat_train_test : y_test0 : 0 : ', y_test0)

                    x_train_concat = x_train0
                    y_train_concat = y_train0
                    x_test_concat = x_test0
                    y_test_concat = y_test0

                else :
                    x_train, y_train, x_test, y_test = self.train_test_spilt(date)
                    
                    x_train0 = tf.concat([x_train0, x_train], axis=0)
                    y_train0 = tf.concat([y_train0, y_train], axis=0)
                    x_test0 = tf.concat([x_test0, x_test], axis=0)
                    y_test0 = tf.concat([y_test0, y_test], axis=0)

                    print('concat_train_test : x_train0 : !0 : ', x_train0)
                    print('concat_train_test : y_train0 : !0 : ', y_train0)
                    print('concat_train_test : x_test0 : !0 : ', x_test0)
                    print('concat_train_test : y_test0 : !0 : ', y_test0)
                    
                    x_train_concat = x_train0
                    y_train_concat = y_train0
                    x_test_concat = x_test0
                    y_test_concat = y_test0
                    
                    print('정상', i,'/',len(code_list)-1, code)
                    
            except Exception as ex:
                print(ex, code)
                continue
                
            
        return x_train_concat, y_train_concat, x_test_concat, y_test_concat 


    def load_final_data(self, path = './numpy/concat/',group = None ,offset=None, timesteps=None):

        dataset_load=np.load(path+'final_{}_dataset_{}_{}.npz'.format(group,self.timesteps,self.offset))

        x_train=dataset_load['x_train']
        x_test=dataset_load['x_test']
        y_train=dataset_load['y_train']
        y_test=dataset_load['y_test']

        dataset_load.close()
        
        return x_train,x_test,y_train,y_test

    def Standardscale(self, x_train, x_test):
        x_train = x_train.reshape(x_train.shape[0],-1)
        x_test = x_test.reshape(x_test.shape[0],-1)
        scaler = StandardScaler()
        scaler.fit(x_train)
        x_train_scaled = scaler.transform(x_train)
        x_test_scaled = scaler.transform(x_test)
        
        return x_train_scaled,x_test_scaled

    def make_model(self, group_) :

        print('make_model : group : ', group_)
        # 원하는 데이터 불러오기
        x_train,x_test,y_train,y_test=self.load_final_data(path = './numpy/concat/',group = group_, offset=self.offset, timesteps=self.timesteps) 
        x_train,x_test = self.Standardscale(x_train,x_test)

        epochs = 50
        batch_size =128
        verbose=0
        opti_adam = Adam(learning_rate = 0.001)
        opti_rms = RMSprop(learning_rate = 0.001)
        opti_sgd = SGD(lr=0.01)

        tf.keras.backend.clear_session()   

        model = Sequential()

        model.add(Dense(32, activation='relu',input_dim=x_train.shape[1],kernel_initializer='he_normal'))
        model.add(BatchNormalization())
        model.add(Dropout(0.5))
        model.add(Dense(10, activation='relu',kernel_initializer='he_normal'))
        model.add(BatchNormalization())
        model.add(Dropout(0.5))
        model.add(Dense(1, activation='sigmoid',kernel_initializer='he_normal'))



        model.compile(optimizer=opti_adam, loss='binary_crossentropy', metrics=['accuracy'])

        filepath = "./model/weights-improvement-{epoch:02d}-{loss:.4f}-bigger.hdf5"
        checkpoint = ModelCheckpoint(
            filepath, monitor='loss',
            verbose=1,        
            save_best_only=True,        
            mode='min'
        )

        earlystopping = EarlyStopping(monitor='val_loss', patience =10)

        callbacks_list = [checkpoint,earlystopping ]     

        history  = model.fit(x_train, y_train, epochs=epochs,
                                batch_size=batch_size, validation_split=0.3, callbacks=callbacks_list)

        
        now_time_ = datetime.now().strftime('%y-%m-%d-%H-%M-%S')
        model.save('./model/{}/{}_{}.hdf5'.format(group_, group_, now_time_))
        # ftp.connect(ftp_ip,ftp_port)
        # ftp.login('a1_model','a1234b')
        # ftp.cwd(ftp_path)
        # file = open('./model/{}/{}_{}.hdf5'.format(group_, group_, now_time_), 'rb')
        # ftp.storbinary('STOR '+'{}_{}.hdf5'.format(group_, now_time_), file)
        # file.close
        # ftp.close
        sql = "insert into {} (time, position) values ('{}','{}');".format(group_, now_time_,'./model/{}/{}_{}.hdf5'.format(group_, group_, now_time_))
        print('local sql command : ',sql)
        cursor_local.execute(sql)
        return_ = cursor_local.fetchall()
        conn_local.commit()
        print('local sql return : ',return_)

        

        model.summary()

        model.evaluate(x_test, y_test)

        # history_dict = history.history
        # loss_values = history_dict['loss']
        # val_loss_values = history_dict['val_loss']
        # # loss_values50 = loss_values[0:150]
        # # val_loss_values50 = val_loss_values[0:150]
        # epochs = range(1, len(loss_values) + 1)
        # plt.plot(epochs, loss_values, 'b',color = 'blue', label='Training loss')
        # plt.plot(epochs, val_loss_values, 'b',color='red', label='Validation loss')
        # plt.rc('font', size = 18)
        # plt.title('Training and validation loss')
        # plt.xlabel('Epochs')
        # plt.ylabel('Loss')
        # plt.legend()
        # plt.xticks(epochs)
        # fig = plt.gcf()
        # fig.set_size_inches(15,7)
        # plt.show()
        return
    
    def run(self) :
        print('modelling : run')

        # print('A_list : ', A_list)
        # print('B_list : ', B_list)
        # print('C_list : ', C_list)
        # print('D_list : ', D_list)
        # print('E_list : ', E_list)
        # print('F_list : ', F_list)

        list_group_ = []
        if len(A_list)>0 :
            list_group_.append('A')
        if len(B_list)>0 :
            list_group_.append('B')
        if len(C_list)>0 :
            list_group_.append('C')
        if len(D_list)>0 :
            list_group_.append('D')
        if len(E_list)>0 :
            list_group_.append('E')
        if len(F_list)>0 :
            list_group_.append('F')


        year_ = self.parent.ui.cmb_year.currentText()
        mon_ = self.parent.ui.cmb_mon.currentText()
        day_ = self.parent.ui.cmb_date.currentText()
        date_time_target_ = year_+'-'+mon_+'-'+day_

        self.make_group(date_time_target_)

        print('make model : ',list_group_)
        for i in list_group_ :
            self.make_model(i)


        



if __name__ == '__main__' :
    app = QtWidgets.QApplication(sys.argv)
    # t_sql = threading.Thread(target=sql_start, args=(sql_que,), daemon=True)
    # t_sql.start()
    w = window()
    app.exec()

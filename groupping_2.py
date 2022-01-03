# import threading
# import PyQt5
import pymysql
import sys
import os
import pandas as pd
import numpy as np
import time
from queue import Queue
from datetime import datetime
# from threading import Thread
from PyQt5 import QtWidgets, uic, QtGui
from PyQt5.QtCore import QThread

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

conn = pymysql.connect(host = ip, port = port, user = user, passwd = passwd, db = db, charset = 'utf8')
cursor = conn.cursor()

conn_daishin = pymysql.connect(host = ip_daishin, port = port_daishin, user = user_daishin, passwd = passwd_daishin, db = db_daishin, charset = 'utf8')
cursor_daishin = conn_daishin.cursor()

# display_que = Queue()
display_que_2 = Queue()

df_daishin_ = pd.DataFrame(None, columns=['종목코드','체결시간','매수잔량Null','매도잔량Null','체결강도Null'])

kiwoom_columns_ = ['종목코드','전일종가','900시가','930시가','데이터개수','당일평균','증감율','그룹']
df_kiwoom_ = pd.DataFrame(None, columns = kiwoom_columns_)

merged_columns_ = ['종목코드','매수잔량Null','매도잔량Null','체결강도Null','전일종가','900시가','930시가','데이터개수','당일평균','증감율','그룹']
df_merged_ = pd.DataFrame(None,columns = merged_columns_)

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
        self.ui = uic.loadUi('./groupping/groupping.ui')
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

        self.grab_ = grab_data(self)
        self.grab_2 = grab_data_2(self)
        self.merging = merging(self)
        self.test = test(self)

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

class merging(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent

    def run(self) :
        global df_merged_
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

        df_merged_ = pd.merge(df_daishin_filtered_,df_kiwoom_filtered_, how='inner', on='종목코드')
        print('merging')
        print(df_merged_)

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

        date_time_realtime_ = datetime.now().strftime('%y-%m-%d-%H-%M-%S')
        df_merged_.to_csv('merged_groupping_{}-{}.csv'.format(date_time_target_, date_time_realtime_), encoding='utf8')


class test(QThread) :
    def __init__(self, parent) :
        super().__init__(parent)
        self.parent = parent

    def run(self) :
        print('test : ')
        print(df_merged_)


if __name__ == '__main__' :
    app = QtWidgets.QApplication(sys.argv)
    # t_sql = threading.Thread(target=sql_start, args=(sql_que,), daemon=True)
    # t_sql.start()
    w = window()
    app.exec()

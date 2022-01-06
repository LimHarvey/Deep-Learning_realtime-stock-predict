from logging import exception
import sys
import pymysql
import time
import datetime
import ftplib

import pandas as pd
import numpy as np

from queue import Queue
from sklearn.preprocessing import StandardScaler, LabelEncoder
from PyQt5 import QtWidgets, uic
from PyQt5.QtCore import QThread
from tensorflow.keras.models import load_model


main_display_Que = Queue()
write_result_Que = Queue()

model_list_=[]
df_group = np.nan

class main_display_Thread(QThread) :
    def __init__(self, parent) :
        super().__init__(parent) 
        self.parent = parent

    def run(self) :
        while True :
            if main_display_Que.empty() :
                time.sleep(1)
                continue
            row_, col_, data_  = main_display_Que.get()
            self.parent.ui.main_table.setItem(row_, col_, QtWidgets.QTableWidgetItem(str(data_)))
            self.parent.ui.main_table.viewport().update()


class write_result_Thread(QThread) :
    def __init__(self, parent) :
        super().__init__(parent) 
        self.parent = parent

        self.sql_complete_num = 0

        ip_predict = 'localhost'
        # ip_predict_inner = '192.168.0.14'
        port_predict = 3306
        user_predict = 'root'
        passwd_predict = '1234'
        db_predict = 'dbpredict'

        self.conn_predict = pymysql.connect(host = ip_predict, port = port_predict, user = user_predict, passwd = passwd_predict, db = db_predict, charset = 'utf8')
        self.cursor_predict = self.conn_predict.cursor()
        print('write result thread start...')

    def run(self) :
        while True :
            if write_result_Que.empty() :
                time.sleep(1)
                # print('write_result_Que : empty')
                continue
            
            code_, time_, data_  = write_result_Que.get()
            # print('isinstance float? : ',(isinstance(data_, np.float32)))
            # print('isinstance nan? : ',data_ is np.nan)
            # 예측결과 nan 값 처리
            if data_ is np.nan :
                continue

            # 종목명의 테이블이 있는지 확인
            sql = "show tables like '{}'".format(code_)
            self.cursor_predict.execute(sql)
            return_ = self.cursor_predict.fetchall()
            # return_ = return_[0]
            print('sql que : check table : ', return_)
            # 없으면 테이블 생성
            if len(return_)==0 :
                try :
                    sql = '''
                    CREATE TABLE {} (
                    `예측시간` DATETIME NOT NULL,
                    `예측결과` DOUBLE NULL,
                    PRIMARY KEY (`예측시간`),
                    UNIQUE INDEX `time_UNIQUE` (`예측시간` ASC));
                    '''.format(code_)
                    self.cursor_predict.execute(sql)
                    return_ = self.cursor_predict.fetchall()
                    self.conn_predict.commit()
                    print("CREATE TABLE {} ".format(code_))
                    print('SQL thread : DB 생성 완료')
                except Exception as e :
                    print('SQL thread : DB 생성 오류, ',e)
                    continue

            try : 
                print('데이터 2개 신규구나...')
                sql = "INSERT INTO {} (예측시간, 예측결과) VALUES ('{}','{}');".format(str(code_),str(time_),data_)            
                self.cursor_predict.execute(sql)
                # print('DB  정상입력')
                return_ = self.cursor_predict.fetchall()
                # print('sql write : new : ',return_)
                self.conn_predict.commit()
                self.sql_complete_num+=1
                print('sql complete : ',self.sql_complete_num)
            except Exception as e :
                print('write result error : ',e)
                # print(code_, time_, data_)
                # sql_ = "UPDATE {} SET 예측결과='{}' WHERE 예측시간='{}';".format(str(code_),data_,str(time_))
                # self.cursor_predict.execute(sql)
                # # print('DB  정상입력')
                # return_ = self.cursor_predict.fetchall()
                # print('sql write exception : ',return_)
                # self.conn_predict.commit()
                # self.sql_complete_num+=1
                # print('sql complete : ',self.sql_complete_num)
                continue
            



class sql :
    def __init__(self) :
        self.max_data_buffer_ = 360

        ip_kiwoom = '61.77.150.183'
        port_kiwoom = 3306
        user_kiwoom = 'a1_user'
        passwd_kiwoom = '4560'
        db_kiwoom = 'kiwoom'

        self.daishin_except_tables = ['u001','z001']

        ip_daishin = '221.148.138.91'
        ip_daishin_inner = '192.168.0.14'
        port_daishin = 3306
        user_daishin = 'remote'
        passwd_daishin = '1234'
        db_daishin = 'dbstock'

        self.conn_kiwoom = pymysql.connect(host = ip_kiwoom, port = port_kiwoom, user = user_kiwoom, passwd = passwd_kiwoom, db = db_kiwoom, charset = 'utf8')
        self.cursor_kiwoom = self.conn_kiwoom.cursor()

        self.conn_daishin = pymysql.connect(host = ip_daishin_inner, port = port_daishin, user = user_daishin, passwd = passwd_daishin, db = db_daishin, charset = 'utf8')
        self.cursor_daishin = self.conn_daishin.cursor()


    def get_daishin_codes(self) : 
        temp_list_ = []
        sql = 'show tables;'
        self.cursor_daishin.execute(sql)
        temp_ = self.cursor_daishin.fetchall()

        for i in temp_ :
            if i[0] not in self.daishin_except_tables :
                temp_list_.append(i[0])

        return temp_list_

    def daesin_main(self, code):    
        sql = 'select * from {};'.format(code)
        # self.cursor_daishin.execute(sql)
        df_dae = pd.read_sql(sql, self.conn_daishin)
        df_dae  = df_dae.set_index('체결시간')
        # df_dae = df_dae.loc[date]
        df_dae = df_dae.iloc[-self.max_data_buffer_:]
        df_dae.drop(df_dae.between_time('15:20','15:29').index, inplace=True)
        df_dae.drop(df_dae.between_time('15:31','15:32').index, inplace=True)
        df_dae.drop(df_dae.at_time('23:59').index, inplace=True)
        df_dae= df_dae[['매수잔량','매도잔량','체결강도']]

        return df_dae

    def daesin_kospi(self):
        sql2 = 'select * from u001;'
        df_kospi = pd.read_sql(sql2, self.conn_daishin)
        df_kospi = df_kospi.set_index('체결시간')[['코스피지수']]
        # df_kospi= df_kospi.loc[date]
        df_kospi = df_kospi.iloc[-self.max_data_buffer_:]
        df_kospi.drop(df_kospi.between_time('15:20','15:29').index, inplace=True)
        df_kospi.drop(df_kospi.between_time('15:31','15:32').index, inplace=True)
        df_kospi.drop(df_kospi.at_time('23:59').index, inplace=True)    
        return df_kospi

    def daesin_issued(self, code):
        code2 = code.replace(code[0], code[0].upper())
        sql4 = 'select * from z001;'
        df_issued = pd.read_sql(sql4, self.conn_daishin)
        globals()['df_issued_{}'.format(code)] = df_issued[df_issued.종목코드 == code2][['3일평균가','발행주식수']]

        return globals()['df_issued_{}'.format(code)]  

    def kiwoom(self, code) : 
        sql3 = 'select * from {};'.format(code)
        df_kiwoom = pd.read_sql(sql3, self.conn_kiwoom)
        df_kiwoom  = df_kiwoom.set_index('체결시간')
        # df_kiwoom= df_kiwoom.loc[date]
        df_kiwoom = df_kiwoom.iloc[-self.max_data_buffer_:]
        df_kiwoom.rename(columns={'현재가':'종가'},inplace=True)
        df_kiwoom.drop('누적거래대금', axis=1, inplace=True)
        
        globals()['df_ki_{}'.format(code)] = df_kiwoom
        
    #     return globals()['df_ki_{}'.format(code)], df_kiwoom
        return df_kiwoom

class get_new_model_Thread(QThread) :
    
    def __init__(self, parent) :
        super().__init__(parent) 
        self.parent = parent

    def run(self) :
        
        global model_list_

        ip_model = '221.148.138.91'
        ip_model_inner = '192.168.0.92'
        port_model = 3306
        user_model = 'remote'
        passwd_model = 'a12345678b!'
        db_model = 'dbmodel'

        conn_model = pymysql.connect(host = ip_model_inner, port = port_model, user = user_model, passwd = passwd_model, db = db_model, charset = 'utf8')
        cursor_model = conn_model.cursor()

        conn_model_2 = pymysql.connect(host = ip_model_inner, port = port_model, user = user_model, passwd = passwd_model, db = 'dbgroup', charset = 'utf8')
        cursor_model_2 = conn_model.cursor()

        ftp = ftplib.FTP()

        ftp_path = '/model'
        pc_path = './model'
        ftp_ip = '221.148.138.91'
        ftp_port = 2101
        ftp_id = 'a1_model'
        ftp_pass = 'a1234b'

        ftp.connect(ftp_ip, ftp_port)
        ftp.login(ftp_id, ftp_pass)
        ftp.cwd(ftp_path)

        sql = 'show tables;'
        cursor_model.execute(sql)
        return_ = cursor_model.fetchall()
        table_list_ = []
        for i in return_ :
            table_list_.append(i[0])

        last_time_ = []

        for i in table_list_ :
            sql = "select max(time) from {};".format(i)
            cursor_model.execute(sql)
            return_ = cursor_model.fetchall()
            last_time_.append(return_[0][0])
        

        for group_, time_ in zip(table_list_,last_time_) :
            print(group_.upper(), time_)

            if time_ is None :
                model_list_.append(None)
            else :
                file_name = group_+'_'+str(time_.strftime('%y-%m-%d-%H-%M-%S'))+'.hdf5'
                pc_file_ = pc_path+'/'+group_+'/'+file_name
                print(pc_file_)
                file = open(pc_file_,'wb')
                ftp.cwd(group_.upper())
                ftp.retrbinary('RETR '+file_name, file.write)
                file.close()
                # ftp.retrlines('RETR '+file_name, file.write)
                # time.sleep(1.5)
                ftp.cwd(ftp_path)

                model = load_model(pc_file_)
                self.parent.ui.now_model_name.setText(pc_file_)

                model_list_.append(model)
                
        # print(model_list_)


        ftp_path = '/group'
        pc_path = './group'


        sql = "select max(time) from dbgroup.group;"
        cursor_model_2.execute(sql)
        return_ = cursor_model_2.fetchall()
        last_time_ = return_[0][0]
        print('group data : ',last_time_)

        file_name = group_+'_'+str(last_time_.strftime('%Y-%m-%d-%H-%M-%S'))+'.hdf5'
        file_name = 'merged_groupping_{}.csv'.format(str(last_time_.strftime('%Y-%m-%d-%H-%M-%S')))

        print('file_name : ',file_name)

        pc_file_ = pc_path+'/'+file_name
        print('pc_file : ',pc_file_)
        file = open(pc_file_,'wb')
        ftp.cwd(ftp_path)
        ftp.retrbinary('RETR '+file_name, file.write)
        file.close()

        df_group = pd.read_csv(pc_file_)
        print(df_group)
        
        print('row num_ : ',self.parent.ui.main_table.rowCount())
        for i in range(self.parent.ui.main_table.rowCount()) :
            code_ = self.parent.ui.main_table.item(i,0)
            code_ = code_.text()
            print(df_group[df_group['종목코드']==code_]['그룹'])
            group_name_ = df_group[df_group['종목코드']==code_]['그룹']
            try :
                group_name = group_name_.iloc[0]

                main_display_Que.put([i, 1, group_name])
            except Exception as e :
                print('group_display : ',i, code_, e)
                pass

            

        
        

            




        


class main(QtWidgets.QDialog) :
    def __init__(self) :
        super().__init__()
        self.ui = uic.loadUi('predict_main.ui')
        self.ui.show()

        self.timesteps = 60
        self.offset = 5
        # self.date = '2021-12-17'

        self.sql = sql()

        self.ui.model_update_btn.clicked.connect(self.model_update)
        self.ui.run_predict_btn.clicked.connect(self.predict_run)

        self.initUi()

    
    def model_update(self) :
        # path_ = './model/'
        # file_ = 'D_60_5.hdf5'

        # print('model update : ', path_ + file_)

        # self.model = load_model(path_ + file_)
        # self.ui.now_model_name.setText(path_ + file_)
        self.get_new_model_thread = get_new_model_Thread(self)
        self.get_new_model_thread.start()
        

    def make_group_B(self, date):
        df_group = pd.read_csv('./csv/{}_merged_groupping.csv'.format(date), index_col=0)
        df_group = df_group.set_index('종목코드') 

        B_code_list = df_group[df_group.그룹 == 'B'].index.to_list()
        C_code_list = df_group[df_group.그룹 == 'C'].index.to_list()
        D_code_list = df_group[df_group.그룹 == 'D'].index.to_list()
        E_code_list = df_group[df_group.그룹 == 'E'].index.to_list()
                
        return B_code_list, C_code_list,D_code_list,E_code_list

    def pre_pro(self):
    
        x, y = self.split_sequence(self.timesteps, self.offset)
        
        e = LabelEncoder()
        e.fit(y)
        y = e.transform(y)

        return x, y

    def merge_df(self, code):
    
        df_dae, df_kiwoom, df_kospi= self.sql.daesin_main(code),self.sql.kiwoom(code), self.sql.daesin_kospi()
        
        df_stock_temp = pd.merge(df_dae, df_kiwoom, left_index = True, right_index=True, how='inner')
        df_stock = pd.merge(df_stock_temp, df_kospi, left_index = True, right_index=True, how='inner')
        df_stock = df_stock.apply(lambda x : abs(x))
        merged_df = df_stock.copy()

        # print('merge_df : ', merged_df)
        
        return merged_df

    def preprocessing(self, code):
    
        merged_df = self.merge_df(code)
        # print('merged_df : ', merged_df)
        df_issued = self.sql.daesin_issued(code)
        # print('df_issued : ', df_issued)
        
        merged_df['매수잔량'] = (merged_df['매수잔량']/df_issued['발행주식수'].values[0])*100
        # print('merged_df 1 : ', merged_df)
        merged_df['매도잔량'] = (merged_df['매도잔량']/df_issued['발행주식수'].values[0])*100
        # print('merged_df 2 : ', merged_df)
        merged_df['거래량'] = (merged_df['거래량']/df_issued['발행주식수'].values[0])*100
        # print('merged_df 3 : ', merged_df)
        # print('merged_df 4 : ', self.date + ' 15:30:00', )
        # merged_df.loc[self.date + ' 15:30:00', '거래량'] = merged_df.at_time('15:30')['거래량'].values[0]/10
        # print('merged_df 4 : ', merged_df)
        merged_df['코스피지수'] = merged_df['코스피지수'].pct_change()
        merged_df['시가'] = merged_df['시가'].pct_change()
        merged_df['고가'] = merged_df['고가'].pct_change()
        merged_df['저가'] = merged_df['저가'].pct_change()
        # print('merged_df 5: ', merged_df)
        drop_index = merged_df[(merged_df['체결강도']== 0)|(merged_df['매수잔량']== 0)|(merged_df['체결강도']== 0)].index
        merged_df.drop(drop_index, axis=0,inplace=True)
        # print('merged_df 6: ', merged_df)
        merged_df.insert(3,'체결강도_pct',merged_df['체결강도'].pct_change())
        merged_df.insert(1,'매수잔량_pct',merged_df['매수잔량'].pct_change())
        merged_df.insert(3,'매도잔량_pct',merged_df['매도잔량'].pct_change())
        merged_df.insert(7,'거래량_pct',merged_df['거래량'].pct_change())
        merged_df.insert(10,'종가_pct', merged_df['종가'].pct_change())
        # print('merged_df 7: ', merged_df)
        
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
    
        merged_df.drop(['매수잔량','매도잔량','체결강도'], axis=1, inplace=True)
        merged_df.drop(['시가_pct','고가_pct','저가_pct'], axis=1, inplace=True)
        
        merged_df = pd.get_dummies(merged_df)
        merged_df.drop('종가', axis=1, inplace=True)

        return merged_df

    def predict_run(self) :
        global model_list_
        cnt = 0
        while True :
            print(cnt)
            try : 
                code_ = self.ui.main_table.item(cnt,0)
                code_ = code_.text()

                group_ = self.ui.main_table.item(cnt,1)
                group_ = group_.text()
            except Exception as e :
                cnt+=1
                print('code_ : ',code_)
                QtWidgets.QApplication.processEvents()
                if cnt >= self.ui.main_table.rowCount() :
                    cnt=0
                continue

            print('predict_run : ', code_, group_)

            if group_ == 'A' :
                model = model_list_[0]
            elif group_ == 'B' :
                model = model_list_[1]
            elif group_ == 'C' :
                model = model_list_[2]
            elif group_ == 'D' :
                model = model_list_[3]
            elif group_ == 'E' :
                model = model_list_[4]
            elif group_ == 'F' :
                model = model_list_[5]
            else :
                cnt+=1
                print('code_ : ',code_)
                QtWidgets.QApplication.processEvents()
                if cnt >= self.ui.main_table.rowCount() :
                    cnt=0
                continue

            try : 
                # x_test_B = self.split_sequence(code_)
                # print(code_, 'preprocessing ???')
                sequence = self.preprocessing(code_)
                # print(code_, 'preprocessing ??? 2')
                x = sequence[-60:]
                # print(code_, 'preprocessing ??? 3 ')
                print('split_sequence : x : ', x)
                x_test = np.array(x).reshape(1,-1)
                # x_test_B = self.Standardscale(x_test_B)
                # print('predict run : x_test_B : ',x_test_B)
                y_test_ = model.predict(x_test)
                y_test_ = y_test_[0][0]
                time_date_ = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            except Exception as e: 
                print('predict error : ',e)
                cnt += 1
                if cnt >= self.ui.main_table.rowCount() :
                    cnt=0
                continue

            num_ = self.code_dict[code_]

            # print('predict run : x : ', x)
            x_time_range = list(x.index)

            

            main_display_Que.put([num_, 2, y_test_])
            main_display_Que.put([num_, 3, min(x_time_range)])
            main_display_Que.put([num_, 4, max(x_time_range)])
            write_result_Que.put([code_, time_date_, y_test_])

            # print('write result que input : ',code_, time_date_, y_test_)

            QtWidgets.QApplication.processEvents()

            print('{} predicted {}'.format(code_,y_test_))

            cnt += 1
            if cnt >= self.ui.main_table.rowCount() :
                cnt=0


    def initUi(self) :
        sql_ = sql()
        codes = sql_.get_daishin_codes()
        print(codes)

        self.ui.main_table.setColumnCount(9)
        self.ui.main_table.setRowCount(len(codes))

        self.main_display_thread = main_display_Thread(self)
        self.main_display_thread.start()

        self.write_result_thread = write_result_Thread(self)
        self.write_result_thread.start()

        self.code_dict = {}

        for cnt,i in enumerate(codes) :
            self.code_dict[i] = cnt
            self.ui.main_table.setItem(cnt, 0, QtWidgets.QTableWidgetItem(str(i)))
        

if __name__ == '__main__' :
    app = QtWidgets.QApplication(sys.argv)
    main_window = main()
    app.exec_()

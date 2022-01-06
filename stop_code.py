# import win32com.client
# import time
import sys
# import pythoncom
import pandas as pd
import glob

from PyQt5.QtWidgets import *
from PyQt5 import uic
from PyQt5.QtGui import QStandardItemModel
from PyQt5.QtGui import QStandardItem
from PyQt5.QtCore import *

stop_code = uic.loadUiType("stop_code.ui")[0]



class Thread1(QThread):
    def __init__(self, parent) :
        print('thread : initiate')
        super().__init__(parent)
        self.parent = parent

    def run(self) :
        # pythoncom.CoInitialize()
        saved_data = glob('stop_code.bin')

        if saved_data != False :
            saved_code_list = pd.read_csv('./stop_code.bin')
            for i in range(len(saved_code_list)) :
                saved_code_list[i]
                self.code_table.setItem(i,0,QTableWidgetItem(saved_code_list[i][0]))
                self.code_table.setItem(i,1,QTableWidgetItem(saved_code_list[i][1]))
                


class main_window(QMainWindow, stop_code) :
    list_ = QStandardItemModel()

    def __init__(self):
        super().__init__()
        print('stop_code_window : initiate window')



    def InitUI(self) :
        self.setupUi(self)
        print('main_window : initiate UI')

        self.read_btn.clicked.connect(self.read_stop_code)

        exitAction = QAction('Exit', self)
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit application')
        exitAction.triggered.connect(self.app_exit)

        menubar = self.menuBar()
        menubar.setNativeMenuBar(False)
        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(exitAction)

        
    def read_stop_code (self) :
        df_stop_code = pd.read_csv('./stop_code.csv')
            
        for i in range(len(df_stop_code)):
            stop_name = df_stop_code[i][1]
        print(stop_name)

        # self.txt_sleep.setText(str(0.5))
        # self.display_code()

    # def save_btn_clk(self) :
    #     col_num = self.code_table.columnCount()
    #     row_num = self.code_table.rowCount()

    #     temp[row_num,]

    #     for i in range(row_num) :
    #         for j in range(col_num) :


    #     pd.DataFrame()

    def display_data(self, r, c, txt) :
        for cnt,code in enumerate(self.codeList) :
            name = self.code_list.CodeToName(code)
            self.tableWidget.setItem(r, c, QTableWidgetItem(txt))

    def app_exit(self):
        print('Quit the App')
        app.quit()
        sys.exit()

    def run_real_data(self) :
        self.thread1.start()


if __name__ == '__main__' :
    app = QApplication(sys.argv)
    window = stop_code()
    print('stop_code_window : Initiate')
    window.show()
    print('show window')
    app.exec_()
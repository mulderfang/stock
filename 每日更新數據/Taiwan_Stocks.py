import Stocks_Crawl as SC
import requests
from io import StringIO
from datetime import date
from dateutil.rrule import rrule, DAILY
import pandas as pd


class Taiwan_Stocks(SC.Stocks_Crawl):

    def __init__(self, **kwargs):

        
        # Get all the settings done 

        # print(self.table_name)
        # print(self.Flag_tpe_stocks)
        # print(self.Flag_tsw_stocks) 
        
        # self.stock_name = ""
        # self.stock_num = ""

        self.table_name = "daily_price" #每日價格
        self.table_name2 = "daily_insti_inv"  #每日三大法人
        self.table_name3 = "daily_statistics"  #每日本益比
        self.table_name4 = "sub_category"  #每日本益比
        self.table_name5 = "daily_twse"  #每日本益比
        self.table_name6 = "daily_updown"  #每日漲跌停家數
        self.table_name7 = "daily_tx"  #每日漲跌停家數
        self.table_name8 = "daily_tif_investors"  #每日漲跌停家數


        self.dates = []
        self.Stocks_settings()

        # Check whether it is a tpe or tsw stock


        #self.Flag_tpe_stocks = True

        # 輪流開看看
        #self.Flag_tsw_stocks = True


        #self.Control_Check_stocks()
        
        super().__init__(**kwargs)

    def time_calculate(self, start_time, end_time):
        
        start_year = int( start_time[:4] )
        end_year = int( end_time[:4] )
        start_month = int( start_time[4:6] )
        end_month = int( end_time[4:6] )
        start_day = int( start_time[6:] )
        end_day = int( end_time[6:] )
        
        #時間抓取設定
        start_date = date(start_year, start_month, start_day)
        end_date = date(end_year, end_month, end_day)

        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            self.dates.append(dt.strftime("%Y%m%d"))

    
    def Stocks_settings(self):

        # 股票類型設定


        # 時間抓取設定
        
        # print("""請輸入想要抓取的時間區間，輸入格式為\n20210102 -> 起始時間\n20210228 -> 結束時間""")
        # start_time = input("請輸入起始時間:")
        # end_time = input("請輸入結束時間:")
        # print("\n----Please enter the date interval----\nThe format is...\nstart time -> 20210102\nend time -> 20210228")


        print("\n  {}".format("(2) Please enter the date interval"))
        print("----------------------------------------")
        print("\n{:^39}".format("The Date Format"))
        print("########################################")
        print("#{:^38}#".format("start time -> 20210101"))
        print("#{:^38}#".format("End time   -> 20210228"))
        print("########################################")
        start_time = input("\nEnter the start time: ")
        end_time = input("Enter the end time:   ")

        # Get the date, format -> 20210104
        self.time_calculate(start_time, end_time)


    ##############################################
    
    def Check_stocks(self, df, check_name, check_num):

    
        if df[df[check_name]==self.stock_name].empty and df[df[check_num]==self.stock_num].empty:

            return False

        else:

            if self.stock_name != "" and self.stock_num != '':
                # assert df[df[check_name] == self.stock_name][check_num].values[0] == self.stock_num, "股票名稱與股票代號不符!! 請重新輸入!!"
                assert df[df[check_name] == self.stock_name][check_num].values[0] == self.stock_num, "The stock name is inconsistent with the stock number!! Please enter again!!"
                
            if not self.stock_name:
                self.stock_name = df[df[check_num] == self.stock_num][check_name].values[0]
            if not self.stock_num:
                self.stock_num = df[df[check_name] == self.stock_name][check_num].values[0]
            
            print("Pass checking... Starts analyzing stocks..")

            return True


    def Control_Check_stocks(self):


        print("\n  {}".format("(3) Starts checking"))
        print("----------------------------------------")
        print("\nChecking the stock name and number...")


        ##### 上市公司

        datestr = '20210104'
        r = requests.post('https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')
        # 整理資料，變成表格
        df = pd.read_csv(StringIO(r.text.replace("=", "")), header=["證券代號" in l for l in r.text.split("\n")].index(True)-1)

        self.Flag_tsw_stocks = self.Check_stocks(df, check_name="證券名稱", check_num="證券代號")

        ##### 上櫃公司

        if not self.Flag_tsw_stocks:

            datestr = '110/01/04'
            r = requests.post('http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d=' + datestr + '&s=0,asc,0')
            # 整理資料，變成表格
            df = pd.read_csv(StringIO(r.text), header=2).dropna(how='all', axis=1).dropna(how='any')
            self.Flag_tpe_stocks = self.Check_stocks(df, check_name="名稱", check_num="代號")
        
        # Set the table_name
        self.table_name = self.stock_name 
        
        # assert Flag_tpe_stocks or Flag_tsw_stocks, "非上市上櫃公司!"
        assert self.Flag_tpe_stocks or self.Flag_tsw_stocks, "Not Listed company!"
        


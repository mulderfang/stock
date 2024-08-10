import MySQL_Database as MD
import requests
from io import StringIO
import pandas as pd
import time
import json
from json import load
from bs4 import BeautifulSoup
import re

class Stocks_Crawl(MD.MySQL_Database):
    
    def __init__(self, timesleep=5, Crawl_flag = True, MySQL_flag = True, 
                 Fetch_stock_statistics_flag = True, 
                 Flag_sub_category = True, 
                 Flag_twse = True, 
                 Flag_tpe_stocks = True,
                 Flag_tsw_stocks = True,
                 Flag_updown = True,
                 **kwargs):
        
        super().__init__(**kwargs)      

        self.Crawl_flag = Crawl_flag
        self.MySQL_flag = MySQL_flag
        self.Fetch_stock_statistics_flag = Fetch_stock_statistics_flag
        #不用每天跑
        self.Flag_sub_category = Flag_sub_category
        #大盤
        self.Flag_twse= Flag_twse
        #上櫃
        self.Flag_tpe_stocks = Flag_tpe_stocks
        #上市
        self.Flag_tsw_stocks = Flag_tsw_stocks
        #漲跌幅
        self.Flag_updown = Flag_updown
        


        ################# 上櫃公司價格資料
        self.url_tpex_stock = "http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d="
        # self.tpex_df_stocks = pd.DataFrame( data = [], 
        #                                     columns = ['Date', '證券代號', '證券名稱', 
        #                                                '成交股數', '成交筆數', 
        #                                                '成交金額', '開盤價', 
        #                                                '最高價', '最低價', 
        #                                                '收盤價', '漲跌(+/-)', 
        #                                                '漲跌價差' ])

        ################# 上櫃公司法人買賣資料
        self.url_tpex_df_institutional_investors = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d="
        # self.tpex_df_institutional_investors = pd.DataFrame( data = [], 
        #                                                      columns = ['證券代號', '證券名稱', 
        #                                                                 '外陸資買進股數(不含外資自營商)', 
        #                                                                 '外陸資賣出股數(不含外資自營商)',
        #                                                                 '外陸資買賣超股數(不含外資自營商)', '外資自營商買進股數', 
        #                                                                 '外資自營商賣出股數', '外資自營商買賣超股數', 
        #                                                                 '投信買進股數','投信賣出股數', 
        #                                                                 '投信買賣超股數', '自營商買賣超股數', 
        #                                                                 '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)',
        #                                                                 '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)',
        #                                                                 '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
        #                                                                 '三大法人買賣超股數' ])

        ################# 上市公司價格資料
        
        self.url_stock = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='
        self.df_stocks = pd.DataFrame(data = [],
                                      columns = ['Date', '證券代號', '證券名稱', 
                                                 '成交股數', '成交筆數', 
                                                 '成交金額', '開盤價', 
                                                 '最高價', '最低價', 
                                                 '收盤價', '漲跌(+/-)', 
                                                 '漲跌價差' ])
        
        ################# 上市公司法人買賣資料
        
        self.url_institutional_investors = 'https://www.twse.com.tw/rwd/zh/fund/T86?response=csv&date='
        self.df_institutional_investors = pd.DataFrame( data = [], 
                                                        columns = ['證券代號', '證券名稱', 
                                                                    '外陸資買進股數(不含外資自營商)', 
                                                                    '外陸資賣出股數(不含外資自營商)',
                                                                    '外陸資買賣超股數(不含外資自營商)', '外資自營商買進股數', 
                                                                    '外資自營商賣出股數', '外資自營商買賣超股數', 
                                                                    '投信買進股數','投信賣出股數', 
                                                                    '投信買賣超股數', '自營商買賣超股數', 
                                                                    '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)',
                                                                    '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)',
                                                                    '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
                                                                    '三大法人買賣超股數'])

        ################# 上市櫃公司股票本益比, 股價淨值比, 殖利率, 股利年度

        self.df_statistics = pd.DataFrame( data = [], 
                                           columns = ["證券代號", "證券名稱", "本益比", "股價淨值比", "殖利率", "股利年度"])
        
        self.url_df_category = 'https://ic.tpex.org.tw/company_chain.php?stk_code='        
        self.df_category = pd.DataFrame( data = [], 
                                    columns = ["stock_id", "stock_name", "main_category", "sub_category"]) 
        
        self.url_twse = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?reponse=csv&date='        
        self.df_twse = pd.DataFrame( data = [], 
                                    columns = ['指數名稱','價格指數值','報酬指數值','漲跌點數','漲跌百分比']) 
        # 漲跌幅
        self.df_updown_combined = pd.DataFrame( data = [], 
                                    columns = ['上市上漲家數','上市漲停家數','上市下跌家數','上市跌停家數','上市持平家數','上市上漲下跌比',
                                               '上櫃上漲家數','上櫃漲停家數', '上櫃下跌家數', '上櫃跌停家數','上櫃持平家數','上櫃上漲下跌比',
                                               '總上漲家數','總漲停家數', '總下跌家數', '總跌停家數','總持平家數','總上漲下跌比']) 

        
        self.timesleep = timesleep
        
        if self.Crawl_flag:
            self.Crawl()
        elif self.Fetch_stock_statistics_flag:
            self.Fetch_stock_statistics()
        else:
            print("The program is useless...END")

        # 爬蟲完要不要存進MySQL資料庫
        if self.MySQL_flag:

            # 存進去Database
            self.SaveIntoDatabase()

            # 爬蟲完，也如果有將資料存進MySQL，將資料庫關起來
            self.Close()
            
    

    # Change the date
    #############################################

    def date_changer(self, date):

        year = date[:4]
        year = str(int(year)-1911)
        month = date[4:6]
        day = date[6:]

        return year+"/"+month+"/"+day
    
        # 定义函数来提取数值
    def extract_values(self, row):
        indicator = row.iloc[0]
        values = []
        
        if indicator == '上漲(漲停)':
            values.append(('上漲家數', int(row.iloc[2].split('(')[0].replace(',', ''))))
            values.append(('漲停家數', int(row.iloc[2].split('(')[1].strip(')'))))
        elif indicator == '下跌(跌停)':
            values.append(('下跌家數', int(row.iloc[2].split('(')[0].replace(',', ''))))
            values.append(('跌停家數', int(row.iloc[2].split('(')[1].strip(')'))))
        elif indicator == '持平':
            values.append(('持平家數', int(row.iloc[2].replace(',', ''))))
        elif indicator == '未成交':
            values.append(('未成交家數', int(row.iloc[2].replace(',', ''))))
        elif indicator == '無比價':
            values.append(('無比價家數', int(row.iloc[2].replace(',', ''))))

        return values

    # CRAWLING
    #############################################
        
    def Crawl(self):
        
        # Start crawling data
        for date in self.dates:

            print(date + " starts crawling")

            try:

                ################ 爬上櫃公司 ################
                
                if self.Flag_tpe_stocks:
                    
                    ROC_era_date = self.date_changer(date)

                    # 股價資訊
                    self.Crawl_method(url = self.url_tpex_stock, 
                                            date = ROC_era_date, 
                                            Date = date, 
                                            url_suffix='&s=0,asc,0', 
                                            Flag_tpex_stocks=True,
                                            Flag_tpex_insti_inv=False,
                                            Flag_stocks=False, 
                                            Flag_insti_inv=False)
                                            
                    # 三大法人資訊
                    self.Crawl_method(url = self.url_tpex_df_institutional_investors, 
                                            date = ROC_era_date, 
                                            Date = date, 
                                            url_suffix='&s=0,asc', 
                                            Flag_tpex_stocks=False,
                                            Flag_tpex_insti_inv=True,
                                            Flag_stocks=False, 
                                            Flag_insti_inv=False)

                    # 本益比, 股價淨值比, 殖利率(%), 股利年度

                    # self.Crawl_PB_and_PE(ROC_era_date,date)  #算一次就好

                ################ 爬上市公司 ################
                
                if self.Flag_tsw_stocks:

                    # 股價資訊
                    self.Crawl_method(url = self.url_stock, 
                                            date = date, 
                                            Date = date, 
                                            url_suffix='&type=ALLBUT0999', 
                                            Flag_tpex_stocks=False,
                                            Flag_tpex_insti_inv=False,
                                            Flag_stocks=True, 
                                            Flag_insti_inv=False)
                                            
                    #爬上市公司三大法人資訊
                    self.Crawl_method(url = self.url_institutional_investors, 
                                            date = date, 
                                            Date = date, 
                                            url_suffix='&selectType=ALLBUT0999', 
                                            Flag_tpex_stocks=False,
                                            Flag_tpex_insti_inv=False,
                                            Flag_stocks=False, 
                                            Flag_insti_inv=True)

                    # 本益比, 股價淨值比, 殖利率(%), 股利年度

                    self.Crawl_PB_and_PE(ROC_era_date,date)

                if self.Flag_sub_category:
                    self.sub_category_list(url = self.url_df_category)

                if self.Flag_twse:
                    self.Crawl_twse(Date = date, url = self.url_twse, url_suffix='&type=ALLBUT0999')

                if self.Flag_updown:
                    self.Crawl_updown( Date = date,  url = self.url_twse, url_suffix='&type=ALLBUT0999')                

            except Exception as err:
                
                if type(err) == ValueError:
                    # print(err)
                    print(date +" is holiday")

                elif type(err) == KeyError:
                    # print(err)
                    print(date +" is holiday")
                else:
                    
                    print("Error happens!! -> " + str(err))
                    break

                                        
            time.sleep(self.timesleep)

        # 把所有資料concatenate起來

        self.ConcatData()
        
    
    # 重新命名col name, 確保一致
    #############################################

    def Rename_df_columns(self, df, Flag_tpex_stocks = False, Flag_tpex_insti_inv = False):

        tpex_stocks_rename_columns = {  "代號":"證券代號",
                                        "名稱":"證券名稱",
                                        "收盤 ":"收盤價",
                                        "漲跌":"漲跌價差",
                                        "開盤 ":"開盤價", 
                                        "最高 ":"最高價",
                                        "最低":"最低價",
                                        "成交股數  ":"成交股數",
                                        "成交金額(元)":"成交金額",
                                        "成交筆數 ":"成交筆數"}

        tpex_insti_inv_rename_columns = {   "代號":"證券代號", 
                                            "名稱":"證券名稱", 
                                            "外資及陸資(不含外資自營商)-買進股數":"外陸資買進股數(不含外資自營商)", 
                                            "外資及陸資(不含外資自營商)-賣出股數":"外陸資賣出股數(不含外資自營商)", 
                                            "外資及陸資(不含外資自營商)-買賣超股數":"外陸資買賣超股數(不含外資自營商)", 
                                            "外資自營商-買進股數":"外資自營商買進股數", 
                                            "外資自營商-賣出股數":"外資自營商賣出股數", 
                                            "外資自營商-買賣超股數":"外資自營商買賣超股數",
                                            "投信-買進股數":"投信買進股數",
                                            "投信-賣出股數":"投信賣出股數",
                                            "投信-買賣超股數":"投信買賣超股數",
                                            "自營商(自行買賣)-買進股數":"自營商買進股數(自行買賣)",
                                            "自營商(自行買賣)-賣出股數":"自營商賣出股數(自行買賣)",
                                            "自營商(自行買賣)-買賣超股數":"自營商買賣超股數(自行買賣)",
                                            "自營商(避險)-買進股數":"自營商買進股數(避險)",
                                            "自營商(避險)-賣出股數":"自營商賣出股數(避險)",
                                            "自營商(避險)-買賣超股數":"自營商買賣超股數(避險)",
                                            "自營商-買賣超股數":"自營商買賣超股數",
                                            "三大法人買賣超股數合計":"三大法人買賣超股數" }

        if Flag_tpex_stocks:  
            df.rename(columns=tpex_stocks_rename_columns, inplace = True)
        elif Flag_tpex_insti_inv:
            df.rename(columns=tpex_insti_inv_rename_columns, inplace = True)
        else:
            print("Error!!")

        return df

    # 開始爬蟲
    #############################################

    def Crawl_method(self, url, date, Date, url_suffix='', Flag_tpex_stocks=False, Flag_tpex_insti_inv=False,
                     Flag_stocks=False, Flag_insti_inv=False, Flag_twse=False, Flag_updown=False):
        
        # 下載股價
        r = requests.get( url + date + url_suffix)

        # 整理資料，變成表格
        
        if not Flag_tpex_stocks and not Flag_tpex_insti_inv and not Flag_stocks and not Flag_insti_inv and not Flag_twse and not Flag_updown:
            
            print("Error...Crawling nothing, please set the flags right")
            return 0


        ######### 爬上櫃公司 #########

        if Flag_tpex_stocks:
            
            df = pd.read_csv(StringIO(r.text), header=2).dropna(how='all', axis=1).dropna(how='any')

            df = df.iloc[:, :11]

            df = self.Rename_df_columns(df, Flag_tpex_stocks = True, Flag_tpex_insti_inv = False)

            #df = self.Get_specific_stock(df)

            df.insert(0, "Date", Date)
            
            df = df[df['證券代號'].apply(lambda x: len(x) == 4)]

            df.drop("均價 ", axis = "columns", inplace = True)
            
            df["漲跌(+/-)"] = df["漲跌價差"].values[0][0] if df["漲跌價差"].values[0][0] != "0" else "X"

            df= df.apply(lambda s:s.astype(str).str.replace(',',''))

            cols_to_numeric = ['開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
            df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
            df[cols_to_numeric] = df[cols_to_numeric].fillna(0)

            # self.df_stocks = self.df_stocks.append(df, ignore_index=True)
            if not self.df_stocks.empty and not df.empty:
                self.df_stocks = pd.concat([self.df_stocks, df], ignore_index=True)
            else : 
                self.df_stocks = df.copy()
        
        if Flag_tpex_insti_inv:

            df = pd.read_csv(StringIO(r.text.replace("=", "")), header = 1 ).dropna(how='all', axis=1).dropna(how='any')

            df.insert(0, "Date", Date)
            
            df.drop(columns=[ "自營商-買進股數", 
                              "自營商-賣出股數",
                              "外資及陸資-買進股數",
                              "外資及陸資-賣出股數",
                              "外資及陸資-買賣超股數"], inplace = True)

            df = self.Rename_df_columns(df, Flag_tpex_stocks = False, Flag_tpex_insti_inv = True)

            df = df[df['證券代號'].apply(lambda x: len(x) == 4)]

            df= df.apply(lambda s:s.astype(str).str.replace(',',''))
            #df = self.Get_specific_stock(df)
            
            # self.df_institutional_investors = self.df_institutional_investors.append(df, ignore_index = True)
            if not self.df_institutional_investors.empty and not df.empty:
                self.df_institutional_investors = pd.concat([self.df_institutional_investors, df], ignore_index=True)
            else : 
                self.df_institutional_investors = df.copy()   

        ######### 爬上市公司 #########

        if Flag_stocks:

            df = pd.read_csv(StringIO(r.text.replace("=", "")), 
                             header = ["證券代號" in l for l in r.text.split("\n")].index(True)-1 )
          
            df.insert(0, "Date", date)

            df = df.iloc[:, :12]

            df = df[df['證券代號'].apply(lambda x: len(x) == 4)]

            df= df.apply(lambda s:s.astype(str).str.replace(',',''))

            cols_to_numeric = ['開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
            df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
            df[cols_to_numeric] = df[cols_to_numeric].fillna(0)

            if not self.df_stocks.empty and not df.empty:
                self.df_stocks = pd.concat([self.df_stocks, df], ignore_index=True)
            else : 
                self.df_stocks = df.copy()      
        
        if Flag_insti_inv:

            df = pd.read_csv(StringIO(r.text.replace("=", "")),
                             header = 1 ).dropna(how='all', axis=1).dropna(how='any')

            df.insert(0, "Date", date)

            df = df[df['證券代號'].apply(lambda x: len(x) == 4)]

            df= df.apply(lambda s:s.astype(str).str.replace(',',''))

            #df = self.Get_specific_stock(df)
            # self.df_institutional_investors = pd.concat([self.df_institutional_investors, df], ignore_index=True)
            if not self.df_institutional_investors.empty and not df.empty:
                self.df_institutional_investors = pd.concat([self.df_institutional_investors, df], ignore_index=True)
            else : 
                self.df_institutional_investors = df.copy()   

    # 爬大盤指數
    def Crawl_twse(self, Date, url, url_suffix):
        data = requests.get(url+Date+url_suffix, timeout=10)
        if data.text =='':
            return pd.DataFrame()
        jsondata = json.loads(data.text)
        if jsondata['stat'] == 'OK':

            # 存加權指數table
            df_twse = pd.DataFrame(jsondata['data1']).drop(5, axis=1) 
            df_twse.columns = ['指數名稱','價格指數值','報酬指數值','漲跌點數','漲跌百分比']
            df_twse= df_twse.dropna(axis=1,how='all').dropna(axis=0,how='all')
            df_twse['Date']=Date
            df_twse= df_twse.apply(lambda s:s.astype(str).str.replace("<p style ='color:red'>",''))
            df_twse= df_twse.apply(lambda s:s.astype(str).str.replace("<p style ='color:green'>",''))
            df_twse= df_twse.apply(lambda s:s.astype(str).str.replace("</p>",''))
            df_twse= df_twse.apply(lambda s:s.astype(str).str.replace(',',''))

            cols_to_numeric = ['價格指數值', '漲跌點數', '漲跌百分比']
            df_twse[cols_to_numeric] = df_twse[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
            df_twse[cols_to_numeric] = df_twse[cols_to_numeric].fillna(0)

            if not self.df_twse.empty and not df_twse.empty:
                self.df_twse = pd.concat([self.df_twse, df_twse], ignore_index=True)
            else : 
                self.df_twse = df_twse.copy()   
        else:
            print(Date + 'df_twse data not found' )

    # 爬漲跌停家數
    def Crawl_updown(self,Date, url, url_suffix):
        ROC_Date = self.date_changer(Date)
        # 先爬上市
        data = requests.get(url+Date+url_suffix, timeout=10)
        if data.text =='':
            return pd.DataFrame()
        jsondata = json.loads(data.text)
        if jsondata['stat'] == 'OK':

            df_twse_ud = pd.DataFrame(jsondata['data8'])

            # 提取所有数值
            extracted_data = []
            for idx, row in df_twse_ud.iterrows():
                
                extracted_data.extend(self.extract_values(row))

            df_twse_updown = pd.DataFrame(extracted_data, columns=['指標', '數值'])

            # 轉置 
            df_twse_updown = df_twse_updown.T
            df_twse_updown.columns = df_twse_updown.iloc[0]
            df_twse_updown = df_twse_updown[1:]
            df_twse_updown.drop(['未成交家數', '無比價家數'], axis=1, inplace=True)
            df_twse_updown.rename(columns={'上漲家數': '上市上漲家數','漲停家數': '上市漲停家數','下跌家數': '上市下跌家數','跌停家數': '上市跌停家數','持平家數': '上市持平家數'}, inplace=True)
            df_twse_updown['Date'] = Date
            df_twse_updown

            # 再爬上櫃 網址不同寫死
            url = "https://www.tpex.org.tw/web/stock/aftertrading/market_highlight/highlight_result.php?l=zh-tw&o=csv&d="

            # 下載股價
            r = requests.get(url + ROC_Date) #要用ROC的處理

            data = []
            for line in StringIO(r.text):
                try:
                    data.append(line.split(','))
                except Exception as e:
                    print(f"Error processing line: {line}, error: {e}")

            df = pd.DataFrame(data)
            extracted_data = []

            for i in range(len(df)):
                df_col = df.iloc[i]
                # 清理数据，移除 \r\n 和多余的空格
                cleaned_data = df_col.dropna().str.replace('\r\n', '').str.strip()

                # 提取数值并创建 DataFrame

                for item in cleaned_data:
                    match1 = re.match(r'"(上漲家數|漲停家數|下跌家數|跌停家數|平盤家數):\s*(\d+)"', item)
                    if match1:
                        indicator = match1.group(1)
                        value = int(match1.group(2))
                        extracted_data.append((indicator, value))

                df_tpex_updown = pd.DataFrame(extracted_data, columns=['指標', '數值'])
            df_tpex_updown

            # 轉置 
            df_tpex_updown = df_tpex_updown.T
            df_tpex_updown.columns = df_tpex_updown.iloc[0]
            df_tpex_updown = df_tpex_updown[1:]
            df_tpex_updown.rename(columns={'上漲家數': '上櫃上漲家數','漲停家數': '上櫃漲停家數','下跌家數': '上櫃下跌家數','跌停家數': '上櫃跌停家數','平盤家數': '上櫃持平家數'}, inplace=True)
            df_tpex_updown['Date'] = Date

            # 保留索引水平合并
            df_updown_combined = pd.merge(df_tpex_updown, df_twse_updown, on='Date')

            df_updown_combined['總上漲家數'] = df_tpex_updown['上櫃上漲家數'].iloc[0] + df_twse_updown['上市上漲家數'].iloc[0] 
            df_updown_combined['總下跌家數'] = df_tpex_updown['上櫃下跌家數'].iloc[0] + df_twse_updown['上市下跌家數'].iloc[0] 
            df_updown_combined['總漲停家數'] = df_tpex_updown['上櫃漲停家數'].iloc[0] + df_twse_updown['上市漲停家數'].iloc[0] 
            df_updown_combined['總跌停家數'] = df_tpex_updown['上櫃跌停家數'].iloc[0] + df_twse_updown['上市跌停家數'].iloc[0] 
            df_updown_combined['總持平家數'] = df_tpex_updown['上櫃持平家數'].iloc[0] + df_twse_updown['上市持平家數'].iloc[0] 

            # 做多 1.35 ~ 0.6 做空
            df_updown_combined['上櫃上漲下跌比']  = round(df_tpex_updown['上櫃上漲家數'].iloc[0] / df_tpex_updown['上櫃下跌家數'].iloc[0], 2 )
            df_updown_combined['上市上漲下跌比']  = round(df_twse_updown['上市上漲家數'].iloc[0] / df_twse_updown['上市下跌家數'].iloc[0], 2 )
            df_updown_combined['總上漲下跌比']    = round(df_updown_combined['總上漲家數'].iloc[0]  / df_updown_combined['總下跌家數'].iloc[0], 2 )

            if not self.df_updown_combined.empty and not df_updown_combined.empty:
                self.df_updown_combined = pd.concat([self.df_updown_combined, df_updown_combined], ignore_index=True)
            else : 
                self.df_updown_combined = df_updown_combined.copy()   
        else:
            print(Date + 'df_updown_combined data not found' )


    def sub_category_list(self, url):

        #  爬蟲爬細產業 只有第一次需要或有新的股票
        info_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMi0yMyAyMjoxNzoyNiIsInVzZXJfaWQiOiJtdWxkZXIiLCJpcCI6IjIwMy4yMDQuMTkzLjEwNCJ9.K95hVEFR_KVdOG2zdeFMC2DCydLAhEP4MjS97Fvt7UQ"

        # 取得 股票名稱
        parameter = {
            "dataset": "TaiwanStockInfo",
            "token": token, # 參考登入，獲取金鑰
        }
        resp = requests.get(info_url, params=parameter)
        info = resp.json()
        info = pd.DataFrame(info["data"]) #個股基本資料


        categories_to_exclude = ['ETF', 'Index', '大盤', '上櫃指數股票型基金(ETF)']
        filtered_info = info[~info['industry_category'].isin(categories_to_exclude)]
        filtered_info = filtered_info[filtered_info['stock_id'].str.len() == 4]


        df_category_list = pd.DataFrame()
        temp_id = ''
        for i in range(len(filtered_info)):
            stock_id = filtered_info.iloc[i].stock_id
            stock_name = filtered_info.iloc[i].stock_name
            if (stock_id == temp_id):
                continue
            else:
                temp_id = stock_id
                stock_url = url + filtered_info.iloc[i].stock_id
                result = requests.get(stock_url) #將此頁面的HTML GET下來
                result.encoding = 'utf-8'
                soup = BeautifulSoup(result.text,"html.parser") #將網頁資料以html.parser
                stock_subcat = soup.find_all("h4")
                if(len(stock_subcat)==0):
                    continue
                else:
                    for cat in stock_subcat:
                        if str(cat.text).find(">") == -1:
                            continue
                        else:
                            main = str(cat.text).split('>')[0].replace("\xa0","")
                            sub = str(cat.text).split('>')[1].replace("\xa0","")
                            category_list = {
                                "stock_id": [stock_id],
                                "stock_name": [stock_name],
                                "main_category": [main],
                                "sub_category": [sub]}
                            category_list_temp = pd.DataFrame(category_list)
                            df_category_list = pd.concat([df_category_list,category_list_temp])
        self.df_category = df_category_list


        # 合併Date
    #############################################

    def ConcatData(self):

        # 將index reset 以免concat出現NaN值
        self.df_stocks.reset_index(drop=True, inplace=True)

        self.df_institutional_investors.reset_index(drop=True, inplace=True)

        self.df_statistics.reset_index(drop=True, inplace=True)

        self.df_twse.reset_index(drop=True, inplace=True)


    # 將Date存進資料庫
    #############################################

    def SaveIntoDatabase(self):


        # creating column list for insertion
        cols = "`,`".join([str(i) for i in self.df_stocks.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i, row in self.df_stocks.iterrows():

            try:
                sql = "INSERT INTO `{}` (`".format(self.table_name) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                
                self.cursor.execute(sql, tuple(row))

                # the connection is not autocommitted by default, so we must commit to save our changes
                self.db.commit()
                
            except Exception as err:
                
                print(err)
                print("個股資料")
                print(row)
                print("This : " + self.table_name + "data already exists")
                continue

        # ============================================================================
        cols = "`,`".join([str(i) for i in self.df_institutional_investors.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i, row in self.df_institutional_investors.iterrows():

            try:
                sql = "INSERT INTO `{}` (`".format(self.table_name2) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                
                self.cursor.execute(sql, tuple(row))

                # the connection is not autocommitted by default, so we must commit to save our changes
                self.db.commit()
                
            except Exception as err:
                
                print(err)
                print("This" + self.table_name2 + "data already exists" )
                continue
        # ============================================================================
        cols = "`,`".join([str(i) for i in self.df_statistics.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i, row in self.df_statistics.iterrows():

            try:
                sql = "INSERT INTO `{}` (`".format(self.table_name3) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                
                self.cursor.execute(sql, tuple(row))

                # the connection is not autocommitted by default, so we must commit to save our changes
                self.db.commit()
                
            except Exception as err:
                
                print(err)
                print("This" + self.table_name3 + "data already exists" )
                continue
        # ============================================================================

        if self.Flag_sub_category:
            # 塞次產業 不用每天跑
            cols = "`,`".join([str(i) for i in self.df_category.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.df_category.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name4) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name4 + "data already exists" )
                    continue
        
        if self.Flag_twse:
            # 跑加權指數
            cols = "`,`".join([str(i) for i in self.df_twse.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.df_twse.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name5) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name5 + "data already exists" )
                    continue

        if self.Flag_updown:
            # 跑漲跌停家數
            cols = "`,`".join([str(i) for i in self.df_updown_combined.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.df_updown_combined.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name6) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name6 + "data already exists" )
                    continue                


    # 抓取PB, PE
    #############################################

    def Crawl_PB_and_PE(self, ROC_era_date,date):

        """
        This function is for crwaling the PB, PE and Dividend yield statistics.
        """


        # 上櫃公司

        if self.Flag_tpe_stocks:
            
            url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_download.php?l=zh-tw&d="+ROC_era_date+"&s=0,asc,0"

            r = requests.get(url)

            r = r.text.split("\n")

            df = pd.read_csv(StringIO("\n".join(r[3:-1]))).fillna(0)

            df.insert(0, "Date", date)

            columns_title = ["Date","股票代號", "名稱", "本益比", "股價淨值比", "殖利率(%)", "股利年度" ]

            df = df[columns_title]

            df.rename(columns = {"殖利率(%)":"殖利率", "股票代號":"證券代號", "名稱":"證券名稱"}, inplace = True)

            #df = self.Get_specific_stock(df)

            # self.df_statistics = self.df_statistics.append(df, ignore_index=True)
            # self.df_statistics = pd.concat([self.df_statistics, df], ignore_index=True)
            if not self.df_statistics.empty and not df.empty:
                self.df_statistics = pd.concat([self.df_statistics, df], ignore_index=True)
            else : 
                self.df_statistics = df.copy()   


        # 上市公司

        if self.Flag_tsw_stocks:

            url = "https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date="+date+"&selectType=ALL"

            r = requests.get(url)

            r = r.text.split("\r\n")[:-13]

            df = pd.read_csv(StringIO("\n".join(r)), header=1).dropna(how="all", axis=1).apply(lambda x:x.replace("-", 0))
            
            df.insert(0, "Date", date)
            
            columns_title = ["Date","證券代號", "證券名稱", "本益比", "股價淨值比", "殖利率(%)", "股利年度" ]

            df = df[columns_title]

            df.rename(columns = {"殖利率(%)":"殖利率"}, inplace = True)

            #df = self.Get_specific_stock(df)

            if not self.df_statistics.empty and not df.empty:
                self.df_statistics = pd.concat([self.df_statistics, df], ignore_index=True)
            else : 
                self.df_statistics = df.copy()   
            

        
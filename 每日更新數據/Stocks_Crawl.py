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
    
    def __init__(self, timesleep=20, Crawl_flag = True, MySQL_flag = True, 
                 Fetch_stock_statistics_flag = True, 
                 Flag_sub_category = True, 
                 Flag_twse = True, 
                 Flag_tpe_stocks = True,
                 Flag_tsw_stocks = True,
                 Flag_updown = True,
                 Flag_tx = True,
                 Flag_tif = True,
                 Flag_pc_ratio = True,
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
        #期貨點數
        self.Flag_tx= Flag_tx
        #法人留倉口點數
        self.Flag_tif= Flag_tif       
        #Put call ratio
        self.Flag_pc_ratio= Flag_pc_ratio       

        ################# 上櫃公司價格資料
        self.url_tpex_stock = "http://wwwov.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d="
        # self.tpex_df_stocks = pd.DataFrame( data = [], 
        #                                     columns = ['Date', '證券代號', '證券名稱', 
        #                                                '成交股數', '成交筆數', 
        #                                                '成交金額', '開盤價', 
        #                                                '最高價', '最低價', 
        #                                                '收盤價', '漲跌(+/-)', 
        #                                                '漲跌價差' ])

        ################# 上櫃公司法人買賣資料
        self.url_tpex_df_institutional_investors = "https://wwwov.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d="
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

        self.url_tx = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'  

        self.daily_tx_data = pd.DataFrame( data = [], 
                                        columns = ['契約','到期月份','開盤價','最高價','最低價','最後成交價','一般交易時段成交量'])         
        # 跑法人留倉口數    
        self.url_tif_top5 = 'https://www.taifex.com.tw/cht/3/largeTraderFutQry'  
        self.url_tif_inv = 'https://www.taifex.com.tw/cht/3/futContractsDate'  

        self.daily_tif_investors_data = pd.DataFrame( data = [], 
                                        columns = ['自營未平倉餘額口數','自營未平倉餘額金額','投信未平倉餘額口數','投信未平倉餘額金額','外資未平倉餘額口數','外資未平倉餘額金額','前五大交易人合計'])      

       # 跑Put call ratio    
        self.url_pc_ratio = 'https://www.taifex.com.tw/cht/3/pcRatio'  

        self.daily_pc_ratio_data = pd.DataFrame( data = [], 
                                        columns = ['賣權成交量','買權成交量','買賣權成交量比率','賣權未平倉量','買權未平倉量','買賣權未平倉量比率'])      

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
    
    # 單純西元轉斜線
    def date_changer_ad(self, date):

        year = date[:4]
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
                # 跑期貨點數
                if self.Flag_tx:
                    AD_era_date = self.date_changer_ad(date)
                    self.Crawl_tx( Date = AD_era_date,  url = self.url_tx)   
                # 跑法人留倉口數
                if self.Flag_tif:
                    AD_era_date = self.date_changer_ad(date)
                    self.Crawl_tif(Date = AD_era_date, url_top5 = self.url_tif_top5 ,url_inv = self.url_tif_inv)        

                # 跑put call ratio
                if self.Flag_pc_ratio:
                    AD_era_date = self.date_changer_ad(date)
                    self.Crawl_pc_ratio(Date = AD_era_date, url = self.url_pc_ratio)    

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
                     Flag_stocks=False, Flag_insti_inv=False, Flag_twse=False, Flag_updown=False, Flag_tx = False, Flag_tif = False, Flag_pc_ratio = False):
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        # 下載股價
        r = requests.get( url + date + url_suffix, headers=headers)

        # 整理資料，變成表格
        
        if not Flag_tpex_stocks and not Flag_tpex_insti_inv and not Flag_stocks and not Flag_insti_inv and not Flag_twse and not Flag_updown and not Flag_tx and not Flag_tif and not Flag_pc_ratio :
            
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
                
        time.sleep(2)

    # 爬大盤指數
    def Crawl_twse(self, Date, url, url_suffix):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        data = requests.get(url+Date+url_suffix, headers=headers, timeout=10)
        if data.text =='':
            return pd.DataFrame()
        jsondata = json.loads(data.text)
        if jsondata['stat'] == 'OK':

            # 存加權指數table
            df_twse = pd.DataFrame(jsondata['tables'][0]['data']).drop(5, axis=1) 
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
            # 只讀 發行量加權股價指數
            df_twse = df_twse[df_twse['指數名稱'] == '發行量加權股價指數'].copy()

            if not self.df_twse.empty and not df_twse.empty:
                self.df_twse = pd.concat([self.df_twse, df_twse], ignore_index=True)
            else : 
                self.df_twse = df_twse.copy()   
        else:
            print(Date + ' df_twse data not found' )
        
        time.sleep(2)

    # 爬期貨指數
    def Crawl_tx(self, Date, url):

        # POST 表單數據
        payload_tx = {
            'queryType': '1',            # 每日報告
            'marketCode': '0',           # 市場代碼，0 表示全部
            'commodity_id': 'TX',        # 商品代碼，TX 代表臺股期貨
            'queryDate': Date,     # 查詢日期，格式為 YYYYMMDD
            'MarketCode': '0'
        }
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        # 發送 POST 請求
        response = requests.post(url, data=payload_tx, headers=headers , timeout=10)

        # 檢查請求是否成功
        if response.status_code == 200:
            # 解析 HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # 找到數據表格
            table = soup.find('table', class_='table_f')

            # 提取表格中的數據
            rows = []
            if table:
                for row in table.find_all('tr')[1:]:  # 跳過表頭行
                    cols = row.find_all('td')
                    cols = [col.text.strip() for col in cols]
                    rows.append(cols)


                if (rows[0][0] == 'TX'):
                    daily_tx_data = pd.DataFrame({
                        'Date': [Date], 
                        '契約': [rows[0][0]],
                        '到期月份': [rows[0][1]],
                        '開盤價': [rows[0][2]],
                        '最高價': [rows[0][3]],
                        '最低價': [rows[0][4]],
                        '最後成交價': [rows[0][5]],
                        '一般交易時段成交量': [rows[0][9]]
                    })

                if not self.daily_tx_data.empty and not daily_tx_data.empty:
                    self.daily_tx_data = pd.concat([self.daily_tx_data, daily_tx_data], ignore_index=True)
                else : 
                    self.daily_tx_data = daily_tx_data.copy()   
            else:
                print(Date + ' daily_tx_data data not found' )
        else:
                print(f"payload_tx請求失敗，狀態碼：{response.status_code}")
                
        time.sleep(2)

    # 跑法人留倉口數
    def Crawl_tif(self, Date, url_top5 ,url_inv):

        # 表單數據
        payload_top5 = {
            'queryDate': Date,               
            'contractId': 'TX'  # 臺股期貨+小臺指期貨/4+臺指摩根期貨/20
        }
        
        tx_top5_buy = 0
        tx_top5_sell = 0
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        # 發送 POST 請求
        response = requests.post(url_top5, headers= headers, data=payload_top5)

        # 確認請求是否成功
        if response.status_code == 200:
            # 解析 HTML 結果
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取目標表格
            table = soup.find('table', class_='table_f')

            if table:
                # 解析表格內容
                rows = []
                for row in table.find_all('tr')[1:]:  # 跳過標題行
                    cols = row.find_all('td')
                    cols = [col.text.strip() for col in cols]
                    rows.append(cols)

                if((rows[2][0] == '臺股期貨(TX+MTX/4+TMF/20)') & (rows[4][0] == '所有契約')):
                    text1 = rows[4][1] # 買方
                    match1 = re.search(r'\(([\d,]+)\)', text1)
                    # 如果找到匹配，去除逗號並取出數字
                    if match1:
                        tx_top5_buy = match1.group(1).replace(',', '')
                    
                    text2 = rows[4][5] # 賣方
                    match2 = re.search(r'\(([\d,]+)\)', text2)
                    # 如果找到匹配，去除逗號並取出數字
                    if match2:
                        tx_top5_sell = match2.group(1).replace(',', '')
            else:
                print(Date + ' top5_data data not found' )

        else:
            print(f"top5請求失敗，狀態碼：{response.status_code}")

        # 你可以更改日期來獲取不同天數的數據
        payload_tif = {
            'queryDate': Date,  # 若要查詢特定日期，填入日期
            'commodity_id': 'TXF'  # TX 表示臺股期貨
        }
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        # 發送 POST 請求
        response = requests.post(url_inv, headers= headers , data=payload_tif)

        # 檢查請求是否成功
        if response.status_code == 200:
            # 解析 HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            trust_data = []    
            # 尋找數據表格

            # 尋找數據表格
            table = soup.find('table', class_='table_f')
            if table:
                rows = table.find_all("tr")[1:]  # 忽略表头
                for row in rows:
                    columns = row.find_all("td")
                    row_data = [col.text.strip() for col in columns]
                    if(len(row_data)!=0):
                        trust_data.append(row_data)

                if ( (trust_data[0][1] == '臺股期貨') &(trust_data[0][2] == '自營商') & (trust_data[1][0] == '投信') & (trust_data[2][0] == '外資')):

                    daily_tif_investors_data = pd.DataFrame({
                        'Date': [Date], 
                        '自營未平倉餘額口數': [trust_data[0][13].replace(',', '')],
                        '自營未平倉餘額金額': [trust_data[0][14].replace(',', '')],
                        '投信未平倉餘額口數': [trust_data[1][11].replace(',', '')],
                        '投信未平倉餘額金額': [trust_data[1][12].replace(',', '')],
                        '外資未平倉餘額口數': [trust_data[2][11].replace(',', '')],
                        '外資未平倉餘額金額': [trust_data[2][12].replace(',', '')],
                        '買方前五大交易人合計': [tx_top5_buy], 
                        '賣方前五大交易人合計': [tx_top5_sell] 
                        })
                    
                if not self.daily_tif_investors_data.empty and not daily_tif_investors_data.empty:
                    self.daily_tif_investors_data = pd.concat([self.daily_tif_investors_data, daily_tif_investors_data], ignore_index=True)
                else : 
                    self.daily_tif_investors_data = daily_tif_investors_data.copy()   
            else:
                print(Date + ' daily_tif_investors data not found' )
        else:
            print(f"TXF請求失敗，狀態碼：{response.status_code}")
        
        time.sleep(2)

    # 跑 選擇權 PC RATIO
    def Crawl_pc_ratio(self, Date, url):

        # POST 表單數據
        payload_pc_ratio = {
            'queryStartDate': Date,     # 查詢日期，格式為 YYYYMMDD
            'queryEndDate': Date       # 查詢日期，格式為 YYYYMMDD
        }
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        # 發送 POST 請求
        response = requests.post(url, data=payload_pc_ratio, headers = headers ,timeout=10)
        
        # 確認請求是否成功
        if response.status_code == 200:
            # 解析 HTML 結果
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取目標表格
            table = soup.find('table', class_='table_f')

            if table:
                # 解析表格內容
                rows = []
                for row in table.find_all('tr')[1:]:  # 跳過標題行
                    cols = row.find_all('td')
                    cols = [col.text.strip() for col in cols]
                    rows.append(cols)
                if(len(rows) > 0):
                    daily_pc_ratio_data = pd.DataFrame({
                        'Date': [Date], 
                        '賣權成交量': [rows[0][1].replace(',', '')],
                        '買權成交量': [rows[0][2].replace(',', '')],
                        '買賣權成交量比率': [rows[0][3]],
                        '賣權未平倉量': [rows[0][4].replace(',', '')],
                        '買權未平倉量': [rows[0][5].replace(',', '')],
                        '買賣權未平倉量比率': [rows[0][6]]
                    })
                    
                    cols_to_numeric = ['賣權成交量', '買權成交量', '買賣權成交量比率', '賣權未平倉量','買權未平倉量','買賣權未平倉量比率']
                    daily_pc_ratio_data[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
                    daily_pc_ratio_data[cols_to_numeric] = daily_pc_ratio_data[cols_to_numeric].fillna(0)

                    if not self.daily_pc_ratio_data.empty and not daily_pc_ratio_data.empty:
                        self.daily_pc_ratio_data = pd.concat([self.daily_pc_ratio_data, daily_pc_ratio_data], ignore_index=True)
                    else : 
                        self.daily_pc_ratio_data = daily_pc_ratio_data.copy()  
                else:
                    print(Date + ' put call ratio data not found' ) 

            else:
                print(Date + ' put call ratio data not found' )

        else:
            print(f"put call ratio請求失敗，狀態碼：{response.status_code}")

        time.sleep(2)

    # 爬漲跌停家數
    def Crawl_updown(self,Date, url, url_suffix):
        ROC_Date = self.date_changer(Date)
        # 先爬上市
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        data = requests.get(url+Date+url_suffix,headers = headers, timeout=10)
        if data.text =='':
            return pd.DataFrame()
        jsondata = json.loads(data.text)
        if jsondata['stat'] == 'OK':

            df_twse_ud = pd.DataFrame(jsondata['tables'][7]['data'])

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
            url = "https://wwwov.tpex.org.tw/web/stock/aftertrading/market_highlight/highlight_result.php?l=zh-tw&o=csv&d="

            # 下載股價
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url + ROC_Date, headers=headers) #要用ROC的處理

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
            print(Date + ' df_updown_combined data not found' )

        time.sleep(2)
        
    def sub_category_list(self, url):

        #  爬蟲爬細產業 只有第一次需要或有新的股票
        info_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMi0yMyAyMjoxNzoyNiIsInVzZXJfaWQiOiJtdWxkZXIiLCJpcCI6IjIwMy4yMDQuMTkzLjEwNCJ9.K95hVEFR_KVdOG2zdeFMC2DCydLAhEP4MjS97Fvt7UQ"

        # 取得 股票名稱
        parameter = {
            "dataset": "TaiwanStockInfo",
            "token": token, # 參考登入，獲取金鑰
        }
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        resp = requests.get(info_url, params=parameter , headers=headers)
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
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
                result = requests.get(stock_url, headers=headers) #將此頁面的HTML GET下來
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
                print("This : " + self.table_name + " data already exists")
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
                print("This" + self.table_name2 + " data already exists" )
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
                print("This" + self.table_name3 + " data already exists" )
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
                    print("This " + self.table_name4 + " data already exists" )
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
                    print("This " + self.table_name5 + " data already exists" )
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
                    print("This " + self.table_name6 + " data already exists" )
                    continue     

        
        if self.Flag_tx:
            # 爬蟲期貨資料
            cols = "`,`".join([str(i) for i in self.daily_tx_data.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.daily_tx_data.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name7) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name7 + " data already exists" )
                    continue   

        
        if self.Flag_tif:
            # 爬蟲外資自營投信留倉
            cols = "`,`".join([str(i) for i in self.daily_tif_investors_data.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.daily_tif_investors_data.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name8) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name8 + " data already exists" )
                    continue    

        if self.Flag_pc_ratio:
            # 爬蟲選擇權 put call ratio
            cols = "`,`".join([str(i) for i in self.daily_pc_ratio_data.columns.tolist()])

            # Insert DataFrame recrds one by one.
            for i, row in self.daily_pc_ratio_data.iterrows():

                try:
                    sql = "INSERT INTO `{}` (`".format(self.table_name9) +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    
                    self.cursor.execute(sql, tuple(row))

                    # the connection is not autocommitted by default, so we must commit to save our changes
                    self.db.commit()
                    
                except Exception as err:
                    
                    print(err)
                    print("This " + self.table_name9 + " data already exists" )
                    continue    

    # 抓取PB, PE
    #############################################

    def Crawl_PB_and_PE(self, ROC_era_date,date):

        """
        This function is for crwaling the PB, PE and Dividend yield statistics.
        """


        # 上櫃公司

        if self.Flag_tpe_stocks:
            
            url = "https://wwwov.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_download.php?l=zh-tw&d="+ROC_era_date+"&s=0,asc,0"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url, headers = headers)

            r = r.text.split("\n")

            df = pd.read_csv(StringIO("\n".join(r[3:-1]))).fillna(0)

            df.insert(0, "Date", date)

            columns_title = ["Date","股票代號", "名稱", "本益比", "股價淨值比", "殖利率(%)", "股利年度" ]

            df = df[columns_title]

            df.rename(columns = {"殖利率(%)":"殖利率", "股票代號":"證券代號", "名稱":"證券名稱"}, inplace = True)
            
            columns_to_clean = ["本益比", "股價淨值比", "殖利率"]
            for col in columns_to_clean:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)
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
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url, headers = headers)

            r = r.text.split("\r\n")[:-13]

            df = pd.read_csv(StringIO("\n".join(r)), header=1).dropna(how="all", axis=1).apply(lambda x:x.replace("-", 0))
            
            df.insert(0, "Date", date)
            
            columns_title = ["Date","證券代號", "證券名稱", "本益比", "股價淨值比", "殖利率(%)", "股利年度" ]

            df = df[columns_title]

            df.rename(columns = {"殖利率(%)":"殖利率"}, inplace = True)

            columns_to_clean = ["本益比", "股價淨值比", "殖利率"]
            for col in columns_to_clean:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)
            #df = self.Get_specific_stock(df)

            if not self.df_statistics.empty and not df.empty:
                self.df_statistics = pd.concat([self.df_statistics, df], ignore_index=True)
            else : 
                self.df_statistics = df.copy()   
            

        